package org.deegraph.tests;

import org.deegraph.database.GraphDatabase;
import org.deegraph.exceptions.UnvalidatedJournalSegment;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

public class TestUtilities {
    protected static final String TEST_CONFIG_FILE = "./test.json";
    public static GraphDatabase initTestDb() throws UnvalidatedJournalSegment, IOException {
        File configFile = new File(TEST_CONFIG_FILE);
        StringBuilder jsonBuilder = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new FileReader(configFile))) {
            String currentLine;
            while ((currentLine = br.readLine()) != null) {
                jsonBuilder.append(currentLine).append("\n");
            }
        }
        JSONObject config = new JSONObject(jsonBuilder.toString());
        File dbDirectory = new File(config.getString("data_directory"));
        if (dbDirectory.exists()) {
            Files.walk(Paths.get(dbDirectory.getAbsolutePath()))
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
        dbDirectory.mkdirs();

        return new GraphDatabase(TEST_CONFIG_FILE, true);
    }

    public static GraphDatabase reloadTestDb() throws UnvalidatedJournalSegment, IOException {
        File configFile = new File(TEST_CONFIG_FILE);
        StringBuilder jsonBuilder = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new FileReader(configFile))) {
            String currentLine;
            while ((currentLine = br.readLine()) != null) {
                jsonBuilder.append(currentLine).append("\n");
            }
        }
        JSONObject config = new JSONObject(jsonBuilder.toString());
        return new GraphDatabase(TEST_CONFIG_FILE, true);
    }
}
