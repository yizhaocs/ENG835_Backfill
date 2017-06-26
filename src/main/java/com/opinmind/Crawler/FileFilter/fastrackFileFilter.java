package com.opinmind.Crawler.FileFilter;

import java.io.File;
import java.io.FileFilter;

/**
 * Created by yzhao on 6/13/17.
 */
public class fastrackFileFilter implements FileFilter {
    private final String[] okFileExtensions = new String[]{".csv", ".csv.force"};

    public boolean accept(File file) {
        for (String extension : okFileExtensions) {
            if (file.getName().toLowerCase().endsWith(extension)) {
                return true;
            }
        }
        return false;
    }
}