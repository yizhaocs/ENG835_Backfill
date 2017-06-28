package com.opinmind.Crawler.FileCrawler;

import org.apache.log4j.Logger;

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
    private static final Logger log = Logger.getLogger(FileCrawler.class);
    private final BlockingQueue fileQueue;
    private final FileFilter fileFilter;
    private final File root;
    private final ScheduledThreadPoolExecutor thread = new ScheduledThreadPoolExecutor(1);
    private final int workerWaitMilliseconds = 5000;
    // private ConcurrentSkipListSet indexedFiles = new ConcurrentSkipListSet();

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
                log.info("[FileCrawler.run] is interrupted, thread name is:" + Thread.currentThread().getName());
                return;
            }
            log.info("[FileCrawler.run] is running, thread name is:" + Thread.currentThread().getName());

            File[] entries = file.listFiles(fileFilter);

            if (entries != null) {
                for (File entry : entries)
                    if (entry.isDirectory()) {
                        // recursively
                        submitCrawlTask(entry);
                    //} else if (entry != null && !indexedFiles.contains(entry)) {
                    } else if (entry != null) {
                        //indexedFiles.add(entry);
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