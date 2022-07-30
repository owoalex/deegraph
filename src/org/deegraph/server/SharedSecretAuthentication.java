package org.deegraph.server;

import com.google.crypto.tink.subtle.Base64;

public class SharedSecretAuthentication extends AuthenticationMethod {
    private byte[] secret;
    public SharedSecretAuthentication(byte[] secret) {
        this.secret = secret;
    }

    public SharedSecretAuthentication(String b64) {
        this.secret = Base64.decode(b64);
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
