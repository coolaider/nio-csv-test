package com.nio.reader.service;

import com.nio.reader.model.Product;
import io.reactivex.subscribers.DisposableSubscriber;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Consumer;
import java.util.function.Supplier;


public class AggregationSubscriber extends DisposableSubscriber<List<String>> {

    ConcurrentLinkedDeque<FileWorker> workersPool;
    String filename;
    Consumer<List<String>> onNexConsumer;


    public AggregationSubscriber(Consumer<List<String>> onNexConsumer, ConcurrentLinkedDeque<FileWorker> workersPool, String filename) {
        this.workersPool = workersPool;
        this.filename = filename;
        this.onNexConsumer = onNexConsumer;
    }


    @Override
    public void onNext(List<String> strings) {
        onNexConsumer.accept(strings);
        request(1);
    }

    @Override
    public void onError(Throwable throwable) {
        System.out.println(throwable.getMessage());
    }

    @Override
    public void onComplete() {
        Optional.ofNullable(workersPool.poll()).ifPresent(worker -> worker.subscribe(new AggregationSubscriber(onNexConsumer, workersPool, worker.getFile().toString())));
        System.out.println(filename + " succesfully collected");
    }


}