package com.indentationerror.dds.conditions;

import com.indentationerror.dds.database.DatabaseInstance;
import com.indentationerror.dds.database.Node;
import com.indentationerror.dds.database.NodePathContext;
import com.indentationerror.dds.database.RelativeNodePath;
import com.indentationerror.dds.formats.DataUrl;

import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.Arrays;

public class EqualityCondition extends Condition {
    private Condition c1;
    private Condition c2;

    public EqualityCondition(DatabaseInstance databaseInstance, Condition c1, Condition c2) {
        super(databaseInstance);
        this.c1 = c1;
        this.c2 = c2;
    }

    @Override
    public boolean eval(NodePathContext context) {
        try {
            String e1 = this.c1.asLiteral(context);
            String e2 = this.c2.asLiteral(context);
            
            byte[] rawValue1 = null;
            byte[] rawValue2 = null;

            if (e1.startsWith("\"")) { // Decide whether to treat as literal or not
                rawValue1 = e1.substring(1, e1.length() - 1).getBytes(StandardCharsets.UTF_8);
            } else {
                Node e1Node = new RelativeNodePath(e1).toAbsolute(context).getNodeFrom(this.databaseInstance);
                if (e1Node != null && e1Node.getData() != null) {
                    rawValue1 = new DataUrl(e1Node.getData()).getRawData();
                }
            }


            if (e2.startsWith("\"")) { // Decide whether to treat as literal or not
                rawValue2 = e2.substring(1, e2.length() - 1).getBytes(StandardCharsets.UTF_8);
            } else {
                Node e2Node = new RelativeNodePath(e2).toAbsolute(context).getNodeFrom(this.databaseInstance);
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
