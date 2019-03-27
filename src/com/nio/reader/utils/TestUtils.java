package com.nio.reader.utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

public class TestUtils {

    public static void prepareTestData(Path directoryPath, int testFiles, int entriesPerFile, int repeatIdEach) {
        System.out.println("Clearing test directory...");
        Arrays.stream(directoryPath.toFile().listFiles()).forEach(file -> {
            if (!file.isDirectory()) {
                file.delete();
            }
        });
        System.out.println("Generating test data...");
        IntStream.range(0, testFiles).forEach(x -> {
            try {
                writeTestData(directoryPath.toFile().getAbsolutePath() + "/" + x + ".csv", entriesPerFile, repeatIdEach);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        System.out.println("Test data filled with " + testFiles + " files contains " + entriesPerFile + " entries");
    }


    public static void writeTestData(String fileName, int entries, int repeatIdEach) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
        Random random = new Random();
        IntStream.range(0, entries).forEach(x -> {
            try {
                writer.write(x % repeatIdEach + ",testName" + x + ",testCondition" + x + ",testState" + "," + ThreadLocalRandom.current().nextDouble(1, 2) + ";\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        writer.close();
    }


}
