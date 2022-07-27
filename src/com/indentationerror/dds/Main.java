package com.indentationerror.dds;

import com.indentationerror.dds.database.GraphDatabase;
import com.indentationerror.dds.server.APIServer;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws Exception {
        System.out.println("Starting server");

        GraphDatabase graphDatabase = new GraphDatabase("./config.json");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                graphDatabase.shutdown();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, "shutdown-thread"));

        APIServer server = new APIServer(graphDatabase);
        server.start(graphDatabase.getConfig().getInt("port"));
    }
}
