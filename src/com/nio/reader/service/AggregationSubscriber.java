package com.nio.reader.service;

import com.nio.reader.model.Product;
import io.reactivex.subscribers.DisposableSubscriber;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;


public class AggregationSubscriber extends DisposableSubscriber<List<String>> {

    ConcurrentLinkedDeque<FileWorker> workersPool;
    String filename;
    ConcurrentHashMap<Integer, SortedSet<Product>> aggregationBuffer;


    public AggregationSubscriber(ConcurrentHashMap<Integer, SortedSet<Product>> aggregationBuffer, ConcurrentLinkedDeque<FileWorker> workersPool, String filename) {
        this.workersPool = workersPool;
        this.filename = filename;
        this.aggregationBuffer = aggregationBuffer;
    }


    @Override
    public void onNext(List<String> strings) {
        strings.stream().filter(s -> !s.isEmpty()).map(string -> {
            String[] s = string.split(",");
            return new Product(Integer.valueOf(s[0]), s[1], s[2], s[3], Double.valueOf(s[4]));
        }).collect(AggregationCollector.toCSVFilesCollector(aggregationBuffer, 20));
        request(1);
    }

    @Override
    public void onError(Throwable throwable) {
        System.out.println(throwable.getMessage());
    }

    @Override
    public void onComplete() {
        Optional.ofNullable(workersPool.poll()).ifPresent(worker -> worker.subscribe(new AggregationSubscriber(aggregationBuffer, workersPool, worker.getFile().toString())));
        System.out.println(filename + " succesfully collected");
    }


}