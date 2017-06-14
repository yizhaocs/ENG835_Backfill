package com.yizhao.apps.Scanner.FileCrawler;

import java.io.File;
import java.io.FileFilter;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by yzhao on 6/13/17.
 */
public class FileCrawler implements Runnable {
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
    private final BlockingQueue fileQueue;
    private final FileFilter fileFilter;
    private final File root;
    private final ScheduledThreadPoolExecutor thread = new ScheduledThreadPoolExecutor(1);
    private final int workerWaitMilliseconds = 5000;
    private ConcurrentSkipListSet indexedFiles = new ConcurrentSkipListSet();

    public FileCrawler(BlockingQueue fileQueue, final FileFilter fileFilter, File inputDir) {

        this.fileQueue = fileQueue;
        this.root = inputDir;
        this.fileFilter = new FileFilter() {
            public boolean accept(File f) {
                return f.isDirectory() || fileFilter.accept(f);
            }
        };
    }

    public void run() {
        submitCrawlTask(root);
    }

    private void submitCrawlTask(File f) {
        final CrawlTask crawlTask = new CrawlTask(f);
        thread.scheduleAtFixedRate(crawlTask, 0L, workerWaitMilliseconds, TimeUnit.MILLISECONDS);
        //exec.execute(crawlTask);
    }

    private class CrawlTask implements Runnable {
        private final File file;

        CrawlTask(File file) {
            this.file = file;
        }

        public void run() {
            if (Thread.currentThread().isInterrupted()) {
                return;
            }

            File[] entries = file.listFiles(fileFilter);

            if (entries != null) {
                for (File entry : entries)
                    if (entry.isDirectory()) {
                        // recursively
                        submitCrawlTask(entry);
                    } else if (entry != null && !indexedFiles.contains(entry)) {
                        indexedFiles.add(entry);
                        try {
                            fileQueue.put(entry);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
            }
        }
    }
}