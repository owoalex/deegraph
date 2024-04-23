package org.deegraph.server;

import com.google.crypto.tink.subtle.Base64;

import java.text.ParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SharedSecretAuthentication extends AuthenticationMethod {
    private byte[] secret;
    public SharedSecretAuthentication(byte[] secret) {
        this.secret = secret;
    }

    public SharedSecretAuthentication(String b64) throws ParseException {
        Pattern pattern = Pattern.compile("^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$");
        if (pattern.matcher(b64).find()) {
            this.secret = Base64.decode(b64);
        } else {
            throw new ParseException("Invalid Base64 string for shared secret", 0);
        }
    }

    @Override
    public boolean validate(Object obj) {
        if (obj instanceof byte[]) {
            return secret.equals(obj);
        } else if (obj instanceof String) {
            try {
                return Base64.encode(secret).equals(obj);
            } catch (Exception e) {
                return false;
            }
        }
        return false;
    }
}
