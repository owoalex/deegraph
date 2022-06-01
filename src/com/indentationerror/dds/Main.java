package com.indentationerror.dds;

import com.indentationerror.dds.database.DatabaseInstance;
import com.indentationerror.dds.server.APIServer;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws Exception {
        System.out.println("Starting server");

        DatabaseInstance databaseInstance = new DatabaseInstance("./config.json");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                databaseInstance.shutdown();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, "shutdown-thread"));

        APIServer server = new APIServer(databaseInstance);
        server.start(databaseInstance.getConfig().getInt("port"));
    }
}
