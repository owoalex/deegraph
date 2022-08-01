package org.deegraph.formats;

import com.google.crypto.tink.subtle.Base64;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;

public class DataUrl {
    byte[] binaryData;
    String stringData;
    String mimeType;

    public DataUrl(String url) throws ParseException {
        if (url == null) {

        } else if (url.startsWith("data:")) {
            String header = url.substring(5, url.indexOf(','));
            String dta = url.substring(url.indexOf(',') + 1);
            //System.out.println("HDR: " + header);
            if (header.indexOf(";base64") == -1) {
                this.stringData = URLDecoder.decode(dta, StandardCharsets.UTF_8);
            } else {
                this.binaryData = Base64.decode(dta);
            }
        } else {
            throw new ParseException("Data URLs must start with data:", 0);
        }
    }

    public String getMimeType() {
        return mimeType;
    }

    public byte[] getRawData() {
        if (this.binaryData != null) {
            return this.binaryData;
        } else {
            if (this.stringData == null) {
                return null;
            } else {
                if (this.stringData == null) {
                    return null;
                }
                return this.stringData.getBytes(StandardCharsets.UTF_8);
            }
        }
    }

    public String getStringData() {
        if (this.binaryData != null) {
            return new String(this.binaryData, StandardCharsets.UTF_8);
        } else {
            if (this.stringData == null) {
                return null;
            } else {
                return this.stringData;
            }
        }
    }

    @Override
    public String toString() {
        if (this.binaryData != null) {
            return Base64.encode(this.binaryData);
        } else {
            if (this.stringData == null) {
                return null;
            } else {
                return URLEncoder.encode(this.stringData, StandardCharsets.UTF_8);
            }
        }
    }
}

