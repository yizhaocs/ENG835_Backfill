package com.yizhao.apps.Scanner.FileProcessor;


import com.yizhao.apps.BackfillController;
import com.yizhao.apps.Util.FileUtils.FileDeleteUtil;
import com.yizhao.apps.Util.FileUtils.FileMoveUtil;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

/**
 * Created by yzhao on 6/13/17.
 */

public class FileProcessor implements Runnable {
    private static final Logger log = Logger.getLogger(FileProcessor.class);
    private final BlockingQueue<File> queue;
    private final File outputDir;
    private final String command;

    public FileProcessor(BlockingQueue<File> queue, File outputDir, String command) {
        this.queue = queue;
        this.outputDir = outputDir;
        this.command = command;
    }

    public void run() {
        try {
            while (true) {
                if(command.equals("delete")){
                    deleteFile(queue.take());
                }else if(command.equals("foundNewFileInDir")){
                    if(queue.size() != 0){
                        BackfillController.getmMyWaitNotify().doNotify();
                        FileDeleteUtil.deleteFile(queue.take());
                    }
                }

            }
        } catch (InterruptedException e) {
            System.out.println("Indexer Interrupted");
            Thread.currentThread().interrupt();
        }
    }

    public void deleteFile(File file) {
        FileDeleteUtil.deleteFilesUnderDir(file.getAbsolutePath(), ".csv");
    }

    public void foundNewFileInDir(File file) {


    }

    public void process(File file) {
        // do something with the file...
        try {
            FileMoveUtil.moveFile(file, outputDir);
            log.info("File: " + file.getName() + "  has moved to " + outputDir.getName());
        }catch(IOException e){
            System.out.println();
            e.printStackTrace();
        }
    }
}