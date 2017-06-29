package com.opinmind;


import com.opinmind.Util.Constants;
import com.opinmind.Util.DateAndTimeUtils.DateCalendar;
import com.opinmind.Util.DateUtil;
import com.opinmind.Connector.NetezzaConnector;
import com.opinmind.Converter.GoogleCloudFileToNetezzaFileConvertor;
import com.opinmind.Converter.EkvrawToFastrackFileConvertor;
import com.opinmind.Crawler.FileCrawler.FileCrawler;
import com.opinmind.Crawler.FileFilter.fastrackFileFilter;
import com.opinmind.Crawler.FileProcessor.FileProcessor;
import com.opinmind.Util.FileUtils.DirCreateUtil;
import com.opinmind.Util.FileUtils.DirGetAllFiles;
import com.opinmind.Util.FileUtils.FileDeleteUtil;
import com.opinmind.Util.FileUtils.FileMoveUtil;
import com.opinmind.Util.MathUtil;
import com.opinmind.Util.ThreadUtils.general.ThreadUtil;
import com.opinmind.Util.ThreadUtils.threadsignaling.MyWaitNotify;
import org.apache.log4j.Logger;

import java.io.File;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * @author YI ZHAO
 *         <p>
 *         build it:
 *         mvn clean package
 *         scp /Users/yzhao/IdeaProjects/ENG835_Backfill/target/Backfill-jar-with-dependencies.jar manager:/home/yzhao/
 *         <p>
 *         Run it:
 *         apac
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar convert /workplace/yzhao/googleFiles/apac/flightFiles/122016/ /workplace/yzhao/netezzaFiles/apac/flightFiles/122016 122016 flight
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar convert /workplace/yzhao/googleFiles/apac/flightFiles/012017/ /workplace/yzhao/netezzaFiles/apac/flightFiles/012017 012017 flight
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar convert /workplace/yzhao/googleFiles/apac/flightFiles/022017/ /workplace/yzhao/netezzaFiles/apac/flightFiles/022017 022017 flight
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar convert /workplace/yzhao/googleFiles/apac/flightFiles/032017/ /workplace/yzhao/netezzaFiles/apac/flightFiles/032017 032017 flight
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar convert /workplace/yzhao/googleFiles/apac/hotelFiles/122016/ /workplace/yzhao/netezzaFiles/apac/hotelFiles/122016 122016 hotel
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar convert /workplace/yzhao/googleFiles/apac/hotelFiles/012017/ /workplace/yzhao/netezzaFiles/apac/hotelFiles/012017 012017 hotel
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar convert /workplace/yzhao/googleFiles/apac/hotelFiles/022017/ /workplace/yzhao/netezzaFiles/apac/hotelFiles/022017 022017 hotel
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar convert /workplace/yzhao/googleFiles/apac/hotelFiles/032017/ /workplace/yzhao/netezzaFiles/apac/hotelFiles/032017 032017 hotel
 *         <p>
 *         priceline
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar convert /workplace/yzhao/googleFiles/priceline/flightFiles/042016/ /workplace/yzhao/netezzaFiles/priceline/flightFiles/042016 042016 flight
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar convert /workplace/yzhao/googleFiles/priceline/flightFiles/052016/ /workplace/yzhao/netezzaFiles/priceline/flightFiles/052016 052016 flight
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar convert /workplace/yzhao/googleFiles/priceline/flightFiles/062016/ /workplace/yzhao/netezzaFiles/priceline/flightFiles/062016 062016 flight
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar convert /workplace/yzhao/googleFiles/priceline/flightFiles/072016/ /workplace/yzhao/netezzaFiles/priceline/flightFiles/072016 072016 flight
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar convert /workplace/yzhao/googleFiles/priceline/flightFiles/082016/ /workplace/yzhao/netezzaFiles/priceline/flightFiles/082016 082016 flight
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar convert /workplace/yzhao/googleFiles/priceline/flightFiles/092016/ /workplace/yzhao/netezzaFiles/priceline/flightFiles/092016 092016 flight
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar convert /workplace/yzhao/googleFiles/priceline/flightFiles/102016/ /workplace/yzhao/netezzaFiles/priceline/flightFiles/102016 102016 flight
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar convert /workplace/yzhao/googleFiles/priceline/flightFiles/112016/ /workplace/yzhao/netezzaFiles/priceline/flightFiles/112016 112016 flight
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar convert /workplace/yzhao/googleFiles/priceline/flightFiles/122016/ /workplace/yzhao/netezzaFiles/priceline/flightFiles/122016 122016 flight
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar convert /workplace/yzhao/googleFiles/priceline/flightFiles/012017/ /workplace/yzhao/netezzaFiles/priceline/flightFiles/012017 012017 flight
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar convert /workplace/yzhao/googleFiles/priceline/flightFiles/022017/ /workplace/yzhao/netezzaFiles/priceline/flightFiles/022017 022017 flight
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar convert /workplace/yzhao/googleFiles/priceline/flightFiles/032017/ /workplace/yzhao/netezzaFiles/priceline/flightFiles/032017 032017 flight
 *         <p>
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar convert /workplace/yzhao/googleFiles/priceline/hotelFiles/042016/ /workplace/yzhao/netezzaFiles/priceline/hotelFiles/042016 042016 hotel
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar convert /workplace/yzhao/googleFiles/priceline/hotelFiles/052016/ /workplace/yzhao/netezzaFiles/priceline/hotelFiles/052016 052016 hotel
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar convert /workplace/yzhao/googleFiles/priceline/hotelFiles/062016/ /workplace/yzhao/netezzaFiles/priceline/hotelFiles/062016 062016 hotel
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar convert /workplace/yzhao/googleFiles/priceline/hotelFiles/072016/ /workplace/yzhao/netezzaFiles/priceline/hotelFiles/072016 072016 hotel
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar convert /workplace/yzhao/googleFiles/priceline/hotelFiles/082016/ /workplace/yzhao/netezzaFiles/priceline/hotelFiles/082016 082016 hotel
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar convert /workplace/yzhao/googleFiles/priceline/hotelFiles/092016/ /workplace/yzhao/netezzaFiles/priceline/hotelFiles/092016 092016 hotel
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar convert /workplace/yzhao/googleFiles/priceline/hotelFiles/102016/ /workplace/yzhao/netezzaFiles/priceline/hotelFiles/102016 102016 hotel
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar convert /workplace/yzhao/googleFiles/priceline/hotelFiles/112016/ /workplace/yzhao/netezzaFiles/priceline/hotelFiles/112016 112016 hotel
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar convert /workplace/yzhao/googleFiles/priceline/hotelFiles/122016/ /workplace/yzhao/netezzaFiles/priceline/hotelFiles/122016 122016 hotel
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar convert /workplace/yzhao/googleFiles/priceline/hotelFiles/012017/ /workplace/yzhao/netezzaFiles/priceline/hotelFiles/012017 012017 hotel
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar convert /workplace/yzhao/googleFiles/priceline/hotelFiles/022017/ /workplace/yzhao/netezzaFiles/priceline/hotelFiles/022017 022017 hotel
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar convert /workplace/yzhao/googleFiles/priceline/hotelFiles/032017/ /workplace/yzhao/netezzaFiles/priceline/hotelFiles/032017 032017 hotel
 *         <p>
 *         <p>
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar dump d eng759_backfill_apac 2016-12 2017-03
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar dump d eng759_backfill_apac  2016-12
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar dump r eng759_backfill_apac  0
 *         <p>
 *         <p>
 *         <p>
 *         /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar dump d ENG759_BACKFILL_PRICELINE 2016-04 2017-03
 */
public class BackfillController {
    private static final Logger log = Logger.getLogger(BackfillController.class);
    private static final String DEFAULT_FILE_PATH = "/tmp/backfill/";
    private static final MyWaitNotify mMyWaitNotify = new MyWaitNotify();
    public static String EKVRAW_FILE_PATH = DEFAULT_FILE_PATH + "ekvrawFile/ekvraw.csv";
    private static Map<String, ExecutorService> threadPools = new HashMap<String, ExecutorService>();
    public EkvrawToFastrackFileConvertor ekvrawToFastrackFileConvertor = null;
    private String fastrackFileOutputPath = DEFAULT_FILE_PATH + "fastrackFile/";
    private String googleCloudFiles = DEFAULT_FILE_PATH + "processedFiles/googleCloud/";
    private String netezzaCloudFiles = DEFAULT_FILE_PATH + "processedFiles/netezza/";
    private GoogleCloudFileToNetezzaFileConvertor googleCloudFileToNetezzaFileConvertor = null;
    private NetezzaConnector netezzaConnector = null;

    public static MyWaitNotify getmMyWaitNotify() {
        return mMyWaitNotify;
    }

    /**
     *
     * @param mode
     * @param option
     * @param table
     * @param startDate
     * @param endDate
     * @param partition
     * @throws Exception
     */
    public void runModeBackfillOrDumpEKVraw(String mode, String option, String table, String startDate, String endDate, String partition) throws Exception {
        if (mode == null) {
            log.error("mode is null");
            return;
        }

        if (option == null) {
            log.error("option is null");
            return;
        }

        if (option.equals("d")) {
            if (startDate == null) {
                log.error("startDate is null");
                return;
            }
        } else if (option.equals("r")) {
            if (partition == null) {
                return;
            }
        }

        if (table == null) {
            log.error("table is null");
            return;
        }

        String startYear = null;
        String startYearMonth = null;
        String endYear = null;
        String endYearMonth = null;
        if (option.equals("d")) {
            if (startDate != null) {
                String[] startYearDateStr = startDate.split("-");
                startYear = startYearDateStr[0];
                startYearMonth = startYearDateStr[1];
            }

            if (endDate != null) {
                String[] endYearDateStr = endDate.split("-");
                endYear = endYearDateStr[0];
                endYearMonth = endYearDateStr[1];
            }
        }

        if (option.equals("d")) {
            log.info("[BackfillController.runModeBackfillOrDumpEKVraw] startYear:" + startYear + " ,startYearMonth:" + startYearMonth + " ,endYear:" + endYear + " ,endYearMonth:" + endYearMonth);

            if (mode.equals("backfill")) {
                if (endDate != null) {
                    unusedFileCLeanThread();
                    String curYear = startYear;
                    String curYearMonth = startYearMonth;
                    while (!curYear.equals(endYear) || !curYearMonth.equals(endYearMonth)) {
                        runBackfill(table, null, curYear, curYearMonth);
                        curYearMonth = curYearMonthPlusOne(curYear, curYearMonth, endYear, endYearMonth);
                    }
                    // run for the final month
                    runBackfill(table, null, curYear, curYearMonth);
                } else {
                    // only get one month
                    runBackfill(table, null, startYear, startYearMonth);
                }
            } else if (mode.equals("dump_ekvraw")) {
                if (endDate != null) {
                    String curYear = startYear;
                    String curYearMonth = startYearMonth;
                    while (!curYear.equals(endYear) || !curYearMonth.equals(endYearMonth)) {
                        dumpEkvrawFromNetezza(table, null, curYear, curYearMonth);
                        curYearMonth = curYearMonthPlusOne(curYear, curYearMonth, endYear, endYearMonth);
                    }

                    // run for the final month
                    dumpEkvrawFromNetezza(table, partition, curYear, curYearMonth);
                } else {
                    // only get one month
                    dumpEkvrawFromNetezza(table, partition, startYear, startYearMonth);
                }
            }
        } else if (option.equals("r")) {
            if (mode.equals("backfill")) {
                unusedFileCLeanThread();
                if (partition == null) {
                    int i = 0;
                    while (i < 10) {
                        runBackfill(table, String.valueOf(i), null, null);
                        i++;
                    }
                } else {
                    runBackfill(table, partition, null, null);
                }
            } else if (mode.equals("dump_ekvraw")) {
                if (partition == null) {
                    int i = 0;
                    while (i < 10) {
                        dumpEkvrawFromNetezza(table, String.valueOf(i), null, null);
                        i++;
                    }
                } else {
                    dumpEkvrawFromNetezza(table, partition, null, null);
                }
            }
        }
    }

    private void runBackfill(String table, String partition, String curYear, String curYearMonth) throws Exception {
        log.info("[BackfillController.runBackfill] with table:" + table + " ,partition:" + partition + " ,curYear:" + curYear + " ,curYearMonth:" + curYearMonth);

        String processedGoogleCloudHotelFilePath = googleCloudFiles + table + "/hotel/" + curYear + "-" + curYearMonth;
        String processedGoogleCloudFlightFilePath = googleCloudFiles + table + "/flight/" + curYear + "-" + curYearMonth;
        String processedNetezzaHotelFilePath = netezzaCloudFiles + table + "/hotel/" + curYear + "-" + curYearMonth;
        String processedNetezzaFlightFilePath = netezzaCloudFiles + table + "/flight/" + curYear + "-" + curYearMonth;


        // Step 1 - dumpEkvrawFromNetezza
        log.info("------------Executing Step 1 - dumpEkvrawFromNetezza------------");
        dumpEkvrawFromNetezza(table, partition, curYear, curYearMonth);

        // Step 2 - processEkvrawToGenerateFastrackFile
        log.info("------------Executing Step 2 - processEkvrawToGenerateFastrackFile------------");
        processEkvrawToGenerateFastrackFile();

        // Step 3 - move fastrack file to udcuv2 inbox
        log.info("------------Executing Step 3 - move fastrack file to udcuv2 inbox------------");
        File toDirectory = new File("/opt/opinmind/var/udcuv2/inbox");
        FileMoveUtil.moveFilesUnderDir(fastrackFileOutputPath, ".force", toDirectory);

        // Step 4 - to know the udcuv2 finish up processing the file
        log.info("------------Executing Step 4 - to know the udcuv2 finish up processing the file------------");
        while (DirGetAllFiles.getAllFilesInDir("/opt/opinmind/var/udcuv2/inbox", ".force").length != 0) { // when inbox has no file, then udcuv2 finished
            // do nothing keep running
        }

        // Step 5 - move hotel files
        log.info("------------Executing Step 5 - move hotel files------------");
        log.info("Thread.sleep(300000)");
        Thread.sleep(300000);
        FileMoveUtil.moveFilesUnderDir("/opt/opinmind/var/google/ekvhotel/concat", ".csv", new File("/opt/opinmind/var/google/ekvhotel/error"));
        DirCreateUtil.createDirectory(new File(processedGoogleCloudHotelFilePath));
        FileMoveUtil.moveFilesUnderDir("/opt/opinmind/var/google/ekvhotel/error", ".csv", new File(processedGoogleCloudHotelFilePath));

        // Step 6 - move flight files
        log.info("------------Executing Step 6 - move flight files------------");
        FileMoveUtil.moveFilesUnderDir("/opt/opinmind/var/google/ekvflight/concat", ".csv", new File("/opt/opinmind/var/google/ekvflight/error"));
        DirCreateUtil.createDirectory(new File(processedGoogleCloudFlightFilePath));
        FileMoveUtil.moveFilesUnderDir("/opt/opinmind/var/google/ekvflight/error", ".csv", new File(processedGoogleCloudFlightFilePath));

        // Step 7 - convert google hotel file to Netezza file
        log.info("------------Executing Step 7 - convert google hotel file to Netezza file------------");
        DirCreateUtil.createDirectory(new File(processedNetezzaHotelFilePath));
        runModeConvert(processedGoogleCloudHotelFilePath, processedNetezzaHotelFilePath + "/ekv_hotel" + "_all_netezza-" + curYear + "-" + curYearMonth + "_" + Constants.Type.HOTEL + "_001.csv", Constants.Type.HOTEL);

        // Step 8 - convert google flight file to Netezza file
        log.info("------------Executing Step 8 - convert google flight file to Netezza file------------");
        DirCreateUtil.createDirectory(new File(processedNetezzaFlightFilePath));
        runModeConvert(processedGoogleCloudFlightFilePath, processedNetezzaFlightFilePath + "/ekv_flight" + "_all_netezza-" + curYear + "-" + curYearMonth + "_" + Constants.Type.FLIGHT + "_001.csv", Constants.Type.FLIGHT);
    }

    public void runModeConvert(String inputPath, String outPutPath, String type) throws Exception {
        if (inputPath == null) {
            log.error("[BackfillController.runModeConvert]: inputPath is null");
            return;
        }

        if (outPutPath == null) {
            log.error("[BackfillController.runModeConvert]: outPutPath is null");
            return;
        }

        if (type == null) {
            log.error("[BackfillController.runModeConvert]: type is null");
            return;
        }

        if (DirGetAllFiles.getAllFilesInDir(inputPath, ".csv").length == 0) {
            log.info("[BackfillController.runModeConvert] inputPath:" + inputPath + " with fileEndWith:" + ".csv" + " is empty");
            return;
        }

        googleCloudFileToNetezzaFileConvertor.process(inputPath, outPutPath, type);
    }

    private void dumpEkvrawFromNetezza(String table, String partition, String curYear, String curYearMonth) throws Exception {
        // true then partition by date, false then partition by reminder of event_id,
        if (partition == null) {
            netezzaConnector.dataToCsvPartitionByYearMonth(table, curYear, curYearMonth);
        } else {
            netezzaConnector.dataToCsvPartitionByMod(table, partition);
        }
    }

    private void processEkvrawToGenerateFastrackFile() throws Exception {
        log.info("done with ekv raws to CSV file \n");
        /**
         * hostName has startwith properties:
         *  hdu.include.only.sources=localhost,dmining,modata,ps,ag,bidder,udcuweb,qa1-ps1,qa-yoweb1,qa2-ps1,qa2-yoweb1,qa4-ps1,qa4-yoweb1,qa-ag1,qa2-ag1,qa4-ag1,qa-bidder,qa2-bidder,qa4-bidder,qa-googlebidder,qa-googlebid,qa2-googlebid,qa4-googlebid,qa1-modata1,qa2-modata1,qa4-modata1
         */
        String CurrentHostName = InetAddress.getLocalHost().getHostName();
        String fileHostName = null;
        // hdu.include.only.sources in common.properties
        if (CurrentHostName.contains("qa") || CurrentHostName.contains("manager")) {
            fileHostName = "qa1-ps1-lax1";
        } else {
            fileHostName = "ps";
        }


        String currentDate = DateUtil.getCurrentDate("yyyyMMdd");
        String timeStamp = DateCalendar.getUnixTimeStamp();
        ekvrawToFastrackFileConvertor.execute(EKVRAW_FILE_PATH, fastrackFileOutputPath + currentDate + "-000000" + "." + fileHostName + "." + timeStamp + "000" + ".csv.force");
        log.info("done with CSV file to fastrack file\n");
        File f = new File(EKVRAW_FILE_PATH);
        if (FileDeleteUtil.deleteFile(f) == 1) {
            log.info(EKVRAW_FILE_PATH + " has deleted" + "\n");
        } else {
            log.info(EKVRAW_FILE_PATH + " has failed to delete" + "\n");
        }
    }

    private void dirCleanThread(String fileSourceInput) {
        ExecutorService threadPool = ThreadUtil.newThread(threadPools, fileSourceInput, true, Thread.NORM_PRIORITY);
        File inputDir = new File(fileSourceInput);
        File outputDir = null;

        BlockingQueue blockingQueue = new ArrayBlockingQueue(5);
        threadPool.execute(new FileCrawler(blockingQueue, new fastrackFileFilter(), inputDir));
        threadPool.execute(new FileProcessor(blockingQueue, outputDir, "delete"));
    }

    private void detectUdcuv2Finish(String fileSourceInput) {
        ExecutorService threadPool = ThreadUtil.newThread(threadPools, fileSourceInput, true, Thread.NORM_PRIORITY);
        File inputDir = new File(fileSourceInput);
        File outputDir = null;

        BlockingQueue blockingQueue = new ArrayBlockingQueue(5);
        threadPool.execute(new FileCrawler(blockingQueue, new fastrackFileFilter(), inputDir));
        threadPool.execute(new FileProcessor(blockingQueue, outputDir, "foundNewFileInDir"));
    }

    private String curYearMonthPlusOne(String curYear, String curYearMonth, String endYear, String endYearMonth){
        if (!curYear.equals(endYear) && !curYearMonth.equals("12")) {
            curYearMonth = new String(MathUtil.plusOne(curYearMonth.toCharArray()));
        } else if (!curYear.equals(endYear) && curYearMonth.equals("12")) {
            curYear = new String(MathUtil.plusOne(curYear.toCharArray()));
            curYearMonth = "01";
        } else if (curYear.equals(endYear) && !curYearMonth.equals(endYearMonth)) {
            curYearMonth = new String(MathUtil.plusOne(curYearMonth.toCharArray()));
        } else {
            log.info("curYear and curYearMonth are same as endYear and endYearMonth");
        }
        return curYearMonth;
    }

    public void setGoogleCloudFileToNetezzaFileConvertor(GoogleCloudFileToNetezzaFileConvertor googleCloudFileToNetezzaFileConvertor) {
        this.googleCloudFileToNetezzaFileConvertor = googleCloudFileToNetezzaFileConvertor;
    }

    public void setEkvrawToFastrackFileConvertor(EkvrawToFastrackFileConvertor ekvrawToFastrackFileConvertor) {
        this.ekvrawToFastrackFileConvertor = ekvrawToFastrackFileConvertor;
    }

    public void setNetezzaConnector(NetezzaConnector netezzaConnector) {
        this.netezzaConnector = netezzaConnector;
    }

    private void unusedFileCLeanThread() {
        dirCleanThread("/opt/opinmind/var/hdfs/ekv/archive");
        dirCleanThread("/opt/opinmind/var/hdfs/ekv/concat");
        dirCleanThread("/opt/opinmind/var/google/ekvraw/error");
        dirCleanThread("/opt/opinmind/var/google/ekvraw/concat");
        dirCleanThread("/opt/opinmind/var/udcuv2/archive");
    }

    public void init() {
        try {
            FileDeleteUtil.deleteDirAndItsSubDirs(new File(DEFAULT_FILE_PATH));
        } catch (Exception e) {
            log.error("[BackfillController.init]: ", e);
        }

        try {
            DirCreateUtil.createDirectory(new File(DEFAULT_FILE_PATH));
            DirCreateUtil.createDirectory(new File(EKVRAW_FILE_PATH));
            DirCreateUtil.createDirectory(new File(fastrackFileOutputPath));
            DirCreateUtil.createDirectory(new File(googleCloudFiles));
            DirCreateUtil.createDirectory(new File(netezzaCloudFiles));
        } catch (Exception e) {
            log.error("[BackfillController.init]: ", e);
        }
    }

    public void destroy() {
        ThreadUtil.stopAllThreads(threadPools, "BackfillMain", 5000L, TimeUnit.MILLISECONDS);
    }
}
