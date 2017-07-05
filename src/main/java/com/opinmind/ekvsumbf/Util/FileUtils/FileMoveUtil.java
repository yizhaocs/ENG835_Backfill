package com.opinmind.ekvsumbf.Util.FileUtils;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;

/**
 * Created by yzhao on 4/15/17.
 */
public class FileMoveUtil {
    private static final Logger logger = Logger.getLogger(FileMoveUtil.class);

    public static void main(String[] args) throws Exception {
        File from = new File("/Users/yzhao/Desktop/test.txt");
        File to = new File("/Users/yzhao/Desktop/test2.txt");
        doMoveFile(from, to, false);

        moveFilesUnderDir("/Users/yzhao/Desktop/input", ".jpg", new File("/Users/yzhao/Desktop/output"));
    }


    /**
     * @param rootPath
     * @param fileEndWith
     * @param toDirectory
     */
    public static void moveFilesUnderDir(String rootPath, final String fileEndWith, File toDirectory) throws IOException {
        if (DirGetAllFiles.getAllFilesInDir(rootPath, fileEndWith).length == 0) {
            logger.info("[FileMoveUtil.moveFilesUnderDir] rootPath:" + rootPath + " with fileEndWith:" + fileEndWith + " is empty");
            return;
        }
        File[] files = DirGetAllFiles.getAllFilesInDir(rootPath, fileEndWith);
        logger.info("[FileMoveUtil.moveFilesUnderDir] is moving total:" + files.length + " files from" + rootPath + " to " + toDirectory.getAbsolutePath());
        for (File f : files) {
            moveFile(f, toDirectory);
        }
    }

    /**
     * Move file to a directory, overwriting it if necessary.
     *
     * @param file
     * @param toDirectory
     * @throws IOException
     */
    public static void moveFile(File file, File toDirectory) throws IOException {
        moveFile(file, toDirectory, true);
    }

    /**
     * Move file into the given directory.
     *
     * @param file
     * @param toDirectory
     * @param overwrite
     * @throws IOException
     */
    public static void moveFile(File file, File toDirectory, boolean overwrite) throws IOException {
        if (!file.exists())
            return;
        File toFile = new File(toDirectory, file.getName());
        doMoveFile(file, toFile, overwrite);
    }

    public static void doMoveFile(File fromFile, File toFile, boolean overwrite) throws IOException {
        if (logger.isDebugEnabled()) {
            logger.debug("START moving file " + fromFile + " to " + toFile);
        }

        if (toFile.equals(fromFile)) {
            if (logger.isDebugEnabled()) {
                logger.debug(String
                        .format("Warning - tried to move a file %s to itself %s - no action taken...",
                                fromFile, toFile));
            }
            return;
        }

        if (toFile.exists() && overwrite && !toFile.delete())
            throw new IOException("Could not delete existing file " + toFile + " to overwrite");

        if (!fromFile.renameTo(toFile)) {
            throw new IOException("Could not move file " + fromFile + " to " + toFile);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("FINISH moving file " + fromFile + " to " + toFile);
        }
    }
}
