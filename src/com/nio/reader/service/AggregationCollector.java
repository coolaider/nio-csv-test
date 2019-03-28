package com.nio.reader.service;

import com.nio.reader.model.Product;

import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class AggregationCollector {


    //fixme: in context of console application is enough static correlation buffers here , but as for component of real application there is should be another approach (pass as parameters to collector for example)
    private static ConcurrentHashMap<Integer, ReentrantLock> locks = new ConcurrentHashMap<>();

    private static AtomicInteger correlationCounter = new AtomicInteger(0);


    public static Collector<Product, ?, ConcurrentHashMap<Integer, SortedSet<Product>>> toCSVFilesCollector(ConcurrentHashMap<Integer, SortedSet<Product>> aggregationBuffer, int maxSameID, int maxResultEntries) {
        return Collectors.toConcurrentMap(Product::getId, (Product v) -> {
            SortedSet<Product> set = aggregationBuffer.getOrDefault(v.getId(), new TreeSet<>(Comparator.comparingDouble(Product::getPrice)));
            locks.putIfAbsent(v.getId(), new ReentrantLock());
            locks.get(v.getId()).lock();
            int size = set.size();
            set.add(v);
            Product last = set.last();
            if (set.size() > maxSameID) {
                set.remove(last);
            }
            if (set.size() <= maxSameID && correlationCounter.get() == maxResultEntries) {
                set.remove(last);
            }
            if (correlationCounter.get() < maxResultEntries && size < set.size()) {
                correlationCounter.incrementAndGet();
            }
            locks.get(v.getId()).unlock();
            return set;
        }, (k, v) -> v, () -> aggregationBuffer);
    }


}
