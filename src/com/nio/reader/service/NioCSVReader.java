package com.nio.reader.service;

import com.nio.reader.model.Product;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class NioCSVReader {

    ConcurrentHashMap<Integer, SortedSet<Product>> aggregationBuffer;

    public NioCSVReader() {
        this.aggregationBuffer = new ConcurrentHashMap<>();
    }


    public List<Product> nonBlockingRead(Path directoryPath, int concurrentConsumers, int bufferSize) throws IOException, InterruptedException {

        try (Stream<Path> paths = Files.walk(directoryPath)) {
            //create worker for each file and run
            runWorkers(paths.filter(path -> Files.isRegularFile(path) && path.toFile().getName().endsWith(".csv"))
                    .map(p -> new FileWorker(p, bufferSize)).collect(Collectors.toCollection(ConcurrentLinkedDeque::new)), concurrentConsumers);
        }

        return new ArrayList<>(aggregationBuffer.entrySet().stream().map(Map.Entry::getValue).flatMap(Collection::stream)
                .collect(Collectors.toCollection(() -> new TreeSet<>(Comparator.comparingDouble(Product::getPrice)))));
    }




    private void runWorkers(ConcurrentLinkedDeque<FileWorker> workers, int concurrentConsumers) throws InterruptedException {
        //fixme: more flexible and better way to do this is use zip() operator, but seems like no way to hug request() backpressure with zipped observables. here we have hard components coupling
        if (!workers.isEmpty()) {
            List<FileWorker> runnedWorkersPool = new ArrayList<>();
            IntStream.range(0, workers.size() >= concurrentConsumers ? workers.size() : concurrentConsumers).forEach(x -> Optional.ofNullable(workers.poll()).ifPresent(worker -> {
                worker.subscribe(new AggregationSubscriber(strings -> {
                    strings.stream().filter(s -> !s.isEmpty()).map(string -> {
                        String[] s = string.split(",");
                        return new Product(Integer.valueOf(s[0]), s[1], s[2], s[3], Double.valueOf(s[4]));
                    }).collect(AggregationCollector.toCSVFilesCollector(aggregationBuffer, 20, 1000));

                }, workers, worker.getFile().toString()));
                runnedWorkersPool.add(worker);
            }));

            //wait until all workers will be completed
            while (!runnedWorkersPool.stream().allMatch(FileWorker::isCompleted)) {
                Thread.sleep(1000);
            }
        }
    }

}
