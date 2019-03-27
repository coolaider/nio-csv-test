package com.nio.reader;


import com.nio.reader.model.Product;
import com.nio.reader.service.NioCSVReader;
import com.nio.reader.utils.TestUtils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class Main {


    public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
        Path directoryPath = Paths.get(System.getProperty("user.dir") + "/csv-test-data/");
        TestUtils.prepareTestData(directoryPath, 10, 100000, 200);
        processFiles(directoryPath, 50, 1000000);
    }


    public static void processFiles(Path directoryPath, int concurrentConsumers, int bufferSize) throws InterruptedException, IOException {
        List<Product> products = new NioCSVReader().nonBlockingRead(directoryPath, concurrentConsumers, bufferSize);
        System.out.println(products.size());

        Files.write(Paths.get(System.getProperty("user.dir") + "/csv-output-data/result.csv"), products.stream()
                .map(p->p.getId()+","+p.getName()+","+p.getCondition()+","+p.getState()+","+p.getPrice()).collect(Collectors.toList()));
    }


}





