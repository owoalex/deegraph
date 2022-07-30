package com.indentationerror.dds.database;

import com.nimbusds.jose.JWSObject;
import org.json.JSONArray;
import org.json.JSONObject;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.UUID;

public class TrustBlock {
    byte[] hash;
    JWSObject signature;
    UUID guarantorId;
    String guarantorFqdn;
    ArrayList<TrustBlock> trustedBy;


    public TrustBlock(UUID guarantorId, String guarantorFqdn, JWSObject signature) {
        this.signature = signature;
        this.guarantorId = guarantorId;
        this.guarantorFqdn = guarantorFqdn;
        this.trustedBy = new ArrayList<>();
    }

    public byte[] getHash() {
        if (this.hash != null) {
            return this.hash;
        }
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        byte[] signatureBytes = this.signature.serialize().getBytes(StandardCharsets.UTF_8);
        byte[] guarantorFqdnBytes = this.guarantorFqdn.getBytes(StandardCharsets.UTF_8);
        ByteBuffer bb = ByteBuffer.wrap(new byte[16 + signatureBytes.length + guarantorFqdnBytes.length]);
        bb.put(signatureBytes);
        bb.putLong(this.guarantorId.getMostSignificantBits());
        bb.putLong(this.guarantorId.getLeastSignificantBits());
        bb.put(guarantorFqdnBytes);
        this.hash = digest.digest(bb.array());
        return this.hash;
    }

    public static TrustBlock createRoot(Node node, GraphDatabase gdb) {
        JWSObject signature = gdb.signPayloadRaw(node.getHash());
        TrustBlock newBlock = new TrustBlock(gdb.getInstanceId(), gdb.getInstanceFqdn(), signature);
        return newBlock;
    }

    public boolean offerPeerTrust(TrustBlock trustBlock) {
        if (trustBlock.getSignature().getPayload().toBytes().equals(getHash())) {
            this.trustedBy.add(trustBlock);
            return true;
        } else {
            return false;
        }
    }

    public TrustBlock trust(GraphDatabase gdb) {
        JWSObject signature = gdb.signPayloadRaw(this.getHash());
        TrustBlock newBlock = new TrustBlock(gdb.getInstanceId(), gdb.getInstanceFqdn(), signature);
        this.trustedBy.add(newBlock);
        return newBlock;
    }

    public JWSObject getSignature() {
        return this.signature;
    }

    public static TrustBlock fromJson(JSONObject trustChain) throws ParseException {
        JWSObject signature = JWSObject.parse(trustChain.getString("signature"));
        UUID guarantorId = UUID.fromString(trustChain.getString("guarantor_id"));
        String guarantorFqdn = trustChain.getString("guarantor_fqdn");
        TrustBlock trustBlock = new TrustBlock(guarantorId, guarantorFqdn, signature);
        for (Object tcDefn: trustChain.getJSONArray("trusted_by")) {
            JSONObject trustChild = (JSONObject) tcDefn;
            trustBlock.offerPeerTrust(TrustBlock.fromJson(trustChild));
        }
        return trustBlock;
    }

    public JSONObject toJson() {
        JSONArray trusts = new JSONArray();
        JSONObject out = new JSONObject();
        for (TrustBlock tb: trustedBy) {
            trusts.put(tb.toJson());
        }
        out.put("trusted_by", trusts);
        out.put("signature", this.signature.serialize());
        out.put("guarantor_id", this.guarantorId);
        out.put("guarantor_fqdn", this.guarantorFqdn);
        return out;
    }
}
