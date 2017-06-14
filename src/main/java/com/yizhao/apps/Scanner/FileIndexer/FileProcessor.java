package com.yizhao.apps.Scanner.FileIndexer;

import com.yizhao.apps.utils.FileUtils.general.FileMoveUtil;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

/**
 * Created by yzhao on 6/13/17.
 */

public class FileProcessor implements Runnable {
    /*
    * BlockingQueue is a unique collection type which not only store elements but also supports flow control by introducing blocking if either BlockingQueue is full or empty.
    * take() method of BlockingQueue will block if Queue is empty and put() method of BlockingQueue will block if Queue is full.
    * This property makes BlockingQueue an ideal choice for implementing Producer consumer design pattern where one thread insert element into BlockingQueue and other thread consumes it.
    *
    * 1) BlockingQueue in Java doesn't allow null elements, various implementation of BlockingQueue like ArrayBlockingQueue, LinkedBlockingQueue throws NullPointerException when you try to add null on queue.
    * 2) BlockingQueue can be bounded or unbounded. A bounded BlockingQueue is one which is initialized with initial capacity and call to put() will be blocked if BlockingQueue is full and size is equal to capacity. This bounding nature makes it ideal to use a shared queue between multiple threads like in most common Producer consumer solutions in Java. An unbounded Queue is one which is initialized without capacity, actually by default it initialized with Integer.MAX_VALUE.
    * 3)BlockingQueue implementations like ArrayBlockingQueue, LinkedBlockingQueue and PriorityBlockingQueue are thread-safe. All queuing method uses concurrency control and internal locks to perform operation atomically. Since BlockingQueue also extend Collection, bulk Collection operations like addAll(), containsAll() are not performed atomically until any BlockingQueue implementation specifically supports it. So call to addAll() may fail after inserting couple of elements.
    * 4) Common methods of BlockingQueue is are put() and take() which are blocking methods in Java and used to insert and retrive elements from BlockingQueue in Java. put() will block if BlockingQueue is full and take() will block if BlockingQueue is empty, call to take() removes element from head of Queue
    * */
    private final BlockingQueue<File> queue;
    private final File outputDir;

    public FileProcessor(BlockingQueue<File> queue, File outputDir) {
        this.queue = queue;
        this.outputDir = outputDir;
    }

    public void run() {
        try {
            while (true) {
                process(queue.take());
            }
        } catch (InterruptedException e) {
            System.out.println("Indexer Interrupted");
            Thread.currentThread().interrupt();
        }
    }

    public void process(File file) {
        // do something with the file...
        try {
            FileMoveUtil.moveFile(file, outputDir);
            System.out.println("File: " + file.getName() + "  has moved to " + outputDir.getName());
        }catch(IOException e){
            System.out.println();
            e.printStackTrace();
        }
    }
}