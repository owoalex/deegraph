package org.deegraph.database;

import com.nimbusds.jose.jwk.OctetKeyPair;
import org.json.JSONObject;

import java.text.ParseException;
import java.util.UUID;

public class TrustKeyJournalEntry extends JournalEntry {
    private OctetKeyPair publicJWK;
    private UUID actor;
    private String instanceFQDN;

    public TrustKeyJournalEntry(OctetKeyPair publicJWK, String instanceFQDN, UUID actor) {
        this.publicJWK = publicJWK;
        this.actor = actor;
        this.instanceFQDN = instanceFQDN;
    }

    public UUID getActor() {
        return actor;
    }

    public static JournalEntry fromJson(JSONObject input) throws ParseException {
        UUID actor = UUID.fromString(input.getString("actor_id"));
        OctetKeyPair jwk = OctetKeyPair.parse(input.getString("public_jwk"));
        String fqdn = input.getString("instance_fqdn");
        return new TrustKeyJournalEntry(jwk, fqdn, actor);
    }

    @Override
    public JSONObject asJson() {
        JSONObject out = new JSONObject();
        out.put("type", "TRUST_KEY");
        out.put("actor_id", this.actor);
        out.put("instance_fqdn", this.instanceFQDN);
        out.put("public_jwk", this.publicJWK.toJSONString());
        return out;
    }
    @Override
    public boolean replayOn(GraphDatabase graphDatabase, Node source) throws ParseException {

        graphDatabase.installPeerNodeKey(this.instanceFQDN, this.publicJWK, graphDatabase.getNodeUnsafe(actor));
        return false;
    }
}
