package com.opinmind.ekvsumbf.Crawler.FileFilter;

import java.io.File;
import java.io.FileFilter;

/**
 * @author YI ZHAO
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