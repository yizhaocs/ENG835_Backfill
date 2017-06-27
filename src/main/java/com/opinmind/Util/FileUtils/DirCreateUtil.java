package com.opinmind.Util.FileUtils;


import com.opinmind.Util.ArgumentUtils.ArgumentsUtil;

import java.io.File;
import java.io.IOException;

/**
 * Created by yzhao on 4/15/17.
 */
public class DirCreateUtil {
    public static void main(String[] args) throws Exception {
        createDirectory(new File("/Users/yzhao/Desktop/input/xyz/abc/xxx"));
    }

    /**
     * Create directory with all parent directories, if parent doesn't exist, then parent dir will be also created.
     *
     * @param directory the directory to create
     */
    public static void createDirectory(File directory) throws IOException {
        ArgumentsUtil.validateMandatory(directory);
        if (!directory.exists()) {
            if (!directory.mkdirs())
                throw new IOException("Failed to create directory " + directory);
        }
    }

    /**
     * Create directories. Ignores, if the directory already exists.
     *
     * @param directories
     * @throws IOException
     */
    public static void createDirectories(File[] directories) throws IOException {
        ArgumentsUtil.validateMandatory((Object[]) directories);
        for (File file : directories) {
            if (file != null) {
                createDirectory(file);
            }
        }
    }
}
