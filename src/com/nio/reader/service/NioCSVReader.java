package com.nio.reader.service;

import com.nio.reader.model.Product;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class NioCSVReader {

    public NioCSVReader() {

    }


    public List<Product> nonBlockingRead(Path directoryPath, int concurrentConsumers, int bufferSize) throws IOException, InterruptedException {
        ConcurrentHashMap<Integer, SortedSet<Product>> aggregationBuffer = new ConcurrentHashMap<>();

        try (Stream<Path> paths = Files.walk(directoryPath)) {
            //create worker for each file
            ConcurrentLinkedDeque<FileWorker> workers = paths.filter(path -> Files.isRegularFile(path) && path.toFile().getName().endsWith(".csv"))
                    .map(p -> new FileWorker(p, bufferSize)).collect(Collectors.toCollection(ConcurrentLinkedDeque::new));
            //run workers
            //fixme: more flexible and better way to do this is use zip() operator, but seems like no way to hug request() backpressure with zipped observables
            if (!workers.isEmpty()) {
                List<FileWorker> runnedWorkersPool = new ArrayList<>();
                IntStream.range(0, workers.size() >= concurrentConsumers ? workers.size() : concurrentConsumers).forEach(x -> Optional.ofNullable(workers.poll()).ifPresent(worker -> {
                    worker.subscribe(new AggregationSubscriber(aggregationBuffer, workers, worker.getFile().toString()));
                    runnedWorkersPool.add(worker);
                }));

                //wait until all workers will be completed
                while (!runnedWorkersPool.stream().allMatch(FileWorker::isCompleted)) {
                    Thread.sleep(1000);
                }
            }
        }

        return new ArrayList<>(aggregationBuffer.entrySet().stream().map(Map.Entry::getValue).flatMap(Collection::stream)
                .collect(Collectors.toCollection(() -> new TreeSet<>(Comparator.comparingDouble(Product::getPrice)))));
    }

}
