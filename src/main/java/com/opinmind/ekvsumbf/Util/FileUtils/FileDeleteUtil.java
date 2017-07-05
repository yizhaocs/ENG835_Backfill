package com.opinmind.ekvsumbf.Util.FileUtils;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;

/**
 * Created by yzhao on 4/15/17.
 */
public class FileDeleteUtil {
    private static final Logger logger = Logger.getLogger(FileDeleteUtil.class);
    public static void main(String[] args) {
        File a = new File("/Users/yzhao/Desktop/pde_map_block_toCountry.csv");
        System.out.println(deleteFile(a));

        System.out.println(deleteFilesUnderDir("/Users/yzhao/Desktop/input", ".jpg"));
    }

    /**
     * Delete the directory named “C:\\mkyong-new“, and all it’s sub-directories and files as well.
     * @param file
     * @throws IOException
     */
    public static void deleteDirAndItsSubDirs(File file)
            throws IOException {

        if(file.isDirectory()){

            //directory is empty, then delete it
            if(file.list().length==0){

                file.delete();
                logger.info("[FileDeleteUtil.deleteDirAndItsSubDirs] Directory is deleted : " + file.getAbsolutePath());

            }else{

                //list all the directory contents
                String files[] = file.list();

                for (String temp : files) {
                    //construct the file structure
                    File fileDelete = new File(file, temp);

                    //recursive delete
                    deleteDirAndItsSubDirs(fileDelete);
                }

                //check the directory again, if empty then delete it
                if(file.list().length==0){
                    file.delete();
                    logger.info("[FileDeleteUtil.deleteDirAndItsSubDirs] Directory is deleted : " + file.getAbsolutePath());
                }
            }

        }else{
            //if file, then delete it
            file.delete();
            logger.info("[FileDeleteUtil.deleteDirAndItsSubDirs] File is deleted : " + file.getAbsolutePath());
        }
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
