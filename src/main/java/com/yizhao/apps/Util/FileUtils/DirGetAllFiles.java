package com.yizhao.apps.Util.FileUtils;

import java.io.File;
import java.io.FileFilter;

/**
 * Created by yzhao on 6/22/17.
 */
public class DirGetAllFiles {
    public static void main(String[] args){

        System.out.println("fileEndWith is .jpg");
        for(File f: getAllFilesInDir("/Users/yzhao/Desktop/input", ".jpg")){
            System.out.println(f.getName());
        }

        //////////////////////////////
        System.out.println();
        System.out.println("fileEndWith is null");
        for(File f: getAllFilesInDir("/Users/yzhao/Desktop/input", null)){
            System.out.println(f.getName());
        }
    }


    /**
     * fileEndWith is ".xml or .csv"
     * @param rootPath
     * @param fileEndWith
     * @return
     */
    public static File[] getAllFilesInDir(String rootPath, final String fileEndWith){
        File testDirectory = new File(rootPath);

        if(fileEndWith == null){
            return testDirectory.listFiles();
        }
        File[] files = testDirectory.listFiles(new FileFilter() {
            public boolean accept(File pathname) {
                String name = pathname.getName().toLowerCase();
                return name.endsWith(fileEndWith) && pathname.isFile();
            }
        });

        return files;
    }
}
