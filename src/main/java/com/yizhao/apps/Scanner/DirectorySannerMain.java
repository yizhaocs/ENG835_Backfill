package com.yizhao.apps.Scanner;



import com.yizhao.apps.Scanner.FileCrawler.FileCrawler;
import com.yizhao.apps.Scanner.FileFilter.fastrackFileFilter;
import com.yizhao.apps.Scanner.FileProcessor.FileProcessor;

import java.io.File;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Reference:
 * https://stackoverflow.com/questions/12870606/directory-scanner-in-java
 * <p>
 * Purpose:
 * Scan a set of directories continuously for a set of file name filters. For each file name filter arrived, process the file to move the file from inputDir to outputDir and repeat the steps for all
 *
 * Dir -> "/Users/yzhao/Desktop/input":
 * 1.jpg
 * 2.jpg
 *
 * after a while
 * 3.jpg
 *
 * output:
 * 1.jpg
 * 2.jpg
 *
 * after a while
 * 3.jpg
 */
public class DirectorySannerMain {
    public static void main(String[] args) {
        File inputDir = new File("/Users/yzhao/Desktop/input");
        File outputDir = new File("/Users/yzhao/Desktop/output");
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
        BlockingQueue blockingQueue = new ArrayBlockingQueue(5);
        FileCrawler fileCrawler = new FileCrawler(blockingQueue, new fastrackFileFilter(), inputDir);
        new Thread(fileCrawler).start();

        FileProcessor processor = new FileProcessor(blockingQueue, outputDir);
        new Thread(processor).start();
    }
}
