package com.nio.reader.service;

import io.reactivex.*;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;


public class FileWorker extends Flowable<List<String>> {

    private Path file;
    private int bufferSize;
    private String delimeter = ";";
    private boolean completed = false;

    static int MIN_BUFFER_SIZE = 1024;

    static int MAX_BUFFER_SIZE = 1000000;


    private Subscriber<? super List<String>> subscriber;

    String correlationBuffer;


    public FileWorker(Path file) {
        this(file, MIN_BUFFER_SIZE);
    }


    public FileWorker(Path file, int bufferSize) {
        this.file = file;
        this.bufferSize = bufferSize < MIN_BUFFER_SIZE ? (bufferSize > MAX_BUFFER_SIZE ? MAX_BUFFER_SIZE : bufferSize) : bufferSize;
    }


    @Override
    protected void subscribeActual(Subscriber<? super List<String>> subscriber) {
        this.subscriber = subscriber;
        long producers = (this.file.toFile().length() / bufferSize);
        try {
            AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(file, StandardOpenOption.READ);
            subscriber.onSubscribe(new Subscription() {
                int segmentCounter = 0;
                @Override
                public void request(long l) {
                    if (segmentCounter <= producers)
                        readData(fileChannel, segmentCounter++ * bufferSize, bufferSize);
                    else {
                        completed = true;
                        subscriber.onComplete();
                    }

                }
                @Override
                public void cancel() {

                }
            });
        } catch (IOException e) {
            System.out.println("Cannot read file " + this.file.toFile().getName());
        }
    }


    private void readData(AsynchronousFileChannel fileChannel, long position, int bufferSize) {
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        fileChannel.read(buffer, position, buffer, new ReadHandler());
    }


    class ReadHandler implements CompletionHandler<Integer, ByteBuffer> {

        @Override
        public void completed(Integer result, ByteBuffer attachment) {
            attachment.flip();
            byte[] data = new byte[attachment.limit()];
            attachment.get(data);
            List<String> res = new LinkedList<>(Arrays.asList(new String(data).split("\n")));
            if (!res.isEmpty()) {
                if (correlationBuffer != null) {
                    res.set(0, correlationBuffer.concat(res.get(0)));
                    correlationBuffer = null;
                }
                if (!res.get(res.size() - 1).endsWith(delimeter)) {
                    correlationBuffer = res.get(res.size() - 1);
                    res.remove(res.get(res.size() - 1));
                }
                attachment.clear();
                subscriber.onNext(res.stream().map(s -> s.replace(delimeter, "")).collect(Collectors.toList()));
            }
            attachment.clear();

        }

        @Override
        public void failed(Throwable exc, ByteBuffer attachment) {
            attachment.clear();
            subscriber.onError(exc);
        }
    }

    public boolean isCompleted() {
        return completed;
    }

    public Path getFile() {
        return file;
    }
}
