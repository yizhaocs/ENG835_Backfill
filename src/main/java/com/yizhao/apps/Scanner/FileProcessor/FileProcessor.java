package com.yizhao.apps.Scanner.FileProcessor;


import com.yizhao.apps.Util.FileDeleteUtil;
import com.yizhao.apps.Util.FileMoveUtil;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

/**
 * Created by yzhao on 6/13/17.
 */

public class FileProcessor implements Runnable {
    private final BlockingQueue<File> queue;
    private final File outputDir;

    public FileProcessor(BlockingQueue<File> queue, File outputDir) {
        this.queue = queue;
        this.outputDir = outputDir;
    }

    public void run() {
        try {
            while (true) {
                deleteFile(queue.take());
            }
        } catch (InterruptedException e) {
            System.out.println("Indexer Interrupted");
            Thread.currentThread().interrupt();
        }
    }

    public void deleteFile(File file) {
        FileDeleteUtil.deleteFile(file);
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