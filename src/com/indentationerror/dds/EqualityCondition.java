package com.indentationerror.dds;

import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.Arrays;

public class EqualityCondition extends Condition {
    private String e1;
    private String e2;

    public EqualityCondition(DatabaseInstance databaseInstance, String e1, String e2) {
        super(databaseInstance);
        this.e1 = e1;
        this.e2 = e2;
    }

    @Override
    public boolean eval(NodePathContext context) {
        try {
            byte[] rawValue1 = null;
            byte[] rawValue2 = null;

            if (this.e1.startsWith("\"")) { // Decide whether to treat as literal or not
                rawValue1 = this.e1.substring(1, this.e1.length() - 1).getBytes(StandardCharsets.UTF_8);
            } else {
                Node e1Node = new RelativeNodePath(this.e1, context).toAbsolute().getNodeFrom(this.databaseInstance);
                if (e1Node != null && e1Node.getData() != null) {
                    rawValue1 = new DataUrl(e1Node.getData()).getRawData();
                }
            }


            if (this.e2.startsWith("\"")) { // Decide whether to treat as literal or not
                rawValue2 = this.e2.substring(1, this.e2.length() - 1).getBytes(StandardCharsets.UTF_8);
            } else {
                Node e2Node = new RelativeNodePath(this.e2, context).toAbsolute().getNodeFrom(this.databaseInstance);
                if (e2Node != null && e2Node.getData() != null) {
                    rawValue2 = new DataUrl(e2Node.getData()).getRawData();
                }
            }


            if (rawValue1 == null || rawValue2 == null) {
                return (rawValue1 == null && rawValue2 == null);
            }
            return Arrays.equals(rawValue1, rawValue2);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
