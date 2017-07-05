package com.opinmind.ekvsumbf.Util;

/**
 * Created by yzhao on 6/29/17.
 */
public class Constants {
    public final class Mode{
        public static final String TESTING_BACKFILL = "testing_backfill";
        public static final String BACKFILL = "backfill";
        public static final String DUMP_EKVRAW = "dump_ekvraw";
        public static final String EKVRAW_TO_FASTRACK = "ekvrawToFastrack";
        public static final String CONVERT = "convert";
    }
    public final class Type {
        public static final String HOTEL = "hotel";
        public static final String FLIGHT = "flight";
    }
    public final class Files{
        public static final String DEFAULT_FILE_PATH = "/tmp/ekvsummarybackfill/";
        public static final String EKVRAW_FILE_PATH = DEFAULT_FILE_PATH + "ekvrawFile/ekvraw.csv";
        public static final String fastrackFileOutputPath = DEFAULT_FILE_PATH + "fastrackFile/";
        public static final String googleCloudFiles = DEFAULT_FILE_PATH + "processedFiles/googleCloud/";
        public static final String netezzaCloudFiles = DEFAULT_FILE_PATH + "processedFiles/netezza/";
    }
}
