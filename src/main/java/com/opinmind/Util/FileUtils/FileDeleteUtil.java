package com.opinmind.Util.FileUtils;

import java.io.File;

/**
 * Created by yzhao on 4/15/17.
 */
public class FileDeleteUtil {
    public static void main(String[] args) {
        File a = new File("/Users/yzhao/Desktop/pde_map_block_toCountry.csv");
        System.out.println(deleteFile(a));

        System.out.println(deleteFilesUnderDir("/Users/yzhao/Desktop/input", ".jpg"));
    }


    /**
     * @param rootPath
     * @param fileEndWith
     * @return
     */
    public static int deleteFilesUnderDir(String rootPath, final String fileEndWith) {
        File[] files = DirGetAllFiles.getAllFilesInDir(rootPath, fileEndWith);
        if(files == null){
            return -1;
        }
        int count = deleteFiles(files); // success deleted file
        return count;
    }


    /**
     * Delete given array of files
     *
     * @param filesToDelete
     * @return
     */
    public static int deleteFiles(File[] filesToDelete) {
        int deletedCount = 0;
        for (int i = 0; i < filesToDelete.length; i++) {
            deletedCount += deleteFile(filesToDelete[i]);
        }
        return deletedCount;
    }

    /**
     * @param file
     * @return
     */
    public static int deleteFile(File file) {
        int deletedCount = 0;
        if (file != null && file.delete())
            deletedCount++;
        return deletedCount;
    }
}
