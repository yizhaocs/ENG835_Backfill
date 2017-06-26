package com.yizhao.apps.Util.FileUtils.concurrent.DirectoryScanner;


import com.yizhao.apps.Util.FileUtils.concurrent.DirectoryScanner.FileCrawler.FileCrawler;
import com.yizhao.apps.Util.FileUtils.concurrent.DirectoryScanner.FileFilter.fastrackFileFilter;
import com.yizhao.apps.Util.FileUtils.concurrent.DirectoryScanner.FileProcessor.FileProcessor;

import java.io.File;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class DirectorySannerMain {
    public static void main(String[] args) {
        File inputDir = new File("/Users/yzhao/Desktop/input");
        File outputDir = new File("/Users/yzhao/Desktop/output");
        BlockingQueue blockingQueue = new ArrayBlockingQueue(5);
        FileCrawler fileCrawler = new FileCrawler(blockingQueue, new fastrackFileFilter(), inputDir);
        new Thread(fileCrawler).start();

        FileProcessor processor = new FileProcessor(blockingQueue, outputDir, "delete");
        new Thread(processor).start();
    }
}
