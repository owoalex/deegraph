package com.indentationerror.dds;

import java.util.UUID;

public class WUUID {
    public final UUID instance;
    public final UUID node;

    public WUUID(UUID instance, UUID node) {
        this.instance = instance;
        this.node = node;
    }

    public UUID getInstanceId() {
        return instance;
    }

    public UUID getOriginalNodeId() {
        return node;
    }
    // Overriding equals() to compare two Complex objects
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof WUUID)) {
            return false;
        }
        WUUID c = (WUUID) o;
        return this.node.equals(c.node) && this.instance.equals(c.instance);
    }

    @Override
    public String toString() {
        return this.instance.toString() + "-" + this.node.toString();
    }


    public static WUUID fromString(String input) {
        if (input.length() != 73) {
            return null;
        }
        return new WUUID(UUID.fromString(input.substring(0, 36)), UUID.fromString(input.substring(37, 73)));
    }
}
