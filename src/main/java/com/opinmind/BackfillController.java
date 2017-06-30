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
import com.opinmind.Util.EmailUtils.SendEmail;
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

mvn clean package
scp /Users/yzhao/IdeaProjects/ENG835_Backfill/target/backfill.war manager:/home/yzhao/
ssh manager
scp backfill.war 68.67.146.136:/home/yzhao
ssh 68.67.146.136
sudo rm /opt/apache-tomcat/webapps/backfill.war
sudo chown -R om:om backfill.war
sudo mv backfill.war /opt/apache-tomcat/webapps/
sudo rm -rf /opt/opinmind/var/
sudo rm -rf /opt/opinmind/logs/hdu/
sudo rm -rf /opt/opinmind/logs/backfill/
sudo rm -rf /opt/opinmind/var/udcuv2/archive
sudo rm -rf /opt/opinmind/var/udcuv2/inbox
sudo /sbin/service tomcat stop_force
sudo /sbin/service tomcat start
ls -la /opt/opinmind/var/

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
    private String mode = null;


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
    public void runModeBackfillOrDumpEKVraw(String mode, String option, String table, String startDate, String endDate, String partition, SendEmail sendEmail) {
        try {
            init();
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
            this.mode = mode;

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

                if (mode.equals(Constants.Mode.TESTING_BACKFILL) || mode.equals(Constants.Mode.BACKFILL)) {
                    unusedFileCLeanThread();
                    if (endDate != null) {
                        String curYear = startYear;
                        String curYearMonth = startYearMonth;
                        while (!curYear.equals(endYear) || !curYearMonth.equals(endYearMonth)) {
                            runBackfill(table, null, curYear, curYearMonth);
                            String[] yearMonth = curYearMonthPlusOne(curYear, curYearMonth, endYear, endYearMonth);
                            curYear = yearMonth[0];
                            curYearMonth = yearMonth[1];
                            sendEmail.send(table, null, curYear, curYearMonth, false);

                        }
                        // run for the final month
                        runBackfill(table, null, curYear, curYearMonth);
                        sendEmail.send(table, null, startYear, startYearMonth, false);
                    } else {
                        // only get one month
                        runBackfill(table, null, startYear, startYearMonth);
                        sendEmail.send(table, null, startYear, startYearMonth, false);
                    }
                } else if (mode.equals(Constants.Mode.DUMP_EKVRAW)) {
                    if (endDate != null) {
                        String curYear = startYear;
                        String curYearMonth = startYearMonth;
                        while (!curYear.equals(endYear) || !curYearMonth.equals(endYearMonth)) {
                            runModedumpEkvrawFromNetezza(table, null, curYear, curYearMonth);
                            String[] yearMonth = curYearMonthPlusOne(curYear, curYearMonth, endYear, endYearMonth);
                            curYear = yearMonth[0];
                            curYearMonth = yearMonth[1];
                        }

                        // run for the final month
                        runModedumpEkvrawFromNetezza(table, partition, curYear, curYearMonth);
                    } else {
                        // only get one month
                        runModedumpEkvrawFromNetezza(table, partition, startYear, startYearMonth);
                    }
                }
            } else if (option.equals("r")) {
                if (mode.equals(Constants.Mode.BACKFILL)) {
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
                } else if (mode.equals(Constants.Mode.DUMP_EKVRAW)) {
                    if (partition == null) {
                        int i = 0;
                        while (i < 10) {
                            runModedumpEkvrawFromNetezza(table, String.valueOf(i), null, null);
                            i++;
                        }
                    } else {
                        runModedumpEkvrawFromNetezza(table, partition, null, null);
                    }
                }
            }

        }catch(Exception e){
            log.error("[BackfillController.runModeBackfillOrDumpEKVraw]: ", e);
        }finally {
            destroy();
        }
    }

    private void runBackfill(String table, String partition, String curYear, String curYearMonth) throws Exception {
        log.info("[BackfillController.runBackfill] with table:" + table + " ,partition:" + partition + " ,curYear:" + curYear + " ,curYearMonth:" + curYearMonth);

        String processedGoogleCloudHotelFilePath = googleCloudFiles + table + "/hotel/" + curYear + "-" + curYearMonth;
        String processedGoogleCloudFlightFilePath = googleCloudFiles + table + "/flight/" + curYear + "-" + curYearMonth;
        String processedNetezzaHotelFilePath = netezzaCloudFiles + table + "/hotel/" + curYear + "-" + curYearMonth;
        String processedNetezzaFlightFilePath = netezzaCloudFiles + table + "/flight/" + curYear + "-" + curYearMonth;


        // Step 1 - runModedumpEkvrawFromNetezza
        log.info("------------Executing Step 1 - runModedumpEkvrawFromNetezza------------");
        runModedumpEkvrawFromNetezza(table, partition, curYear, curYearMonth);

        // Step 2 - runModeEkvrawToFastrack
        log.info("------------Executing Step 2 - runModeEkvrawToFastrack------------");
        runModeEkvrawToFastrack(true);

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

    public void runModeEkvrawToFastrack(boolean deleteEKVRAW) throws Exception {
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

        if(deleteEKVRAW) {
            File f = new File(EKVRAW_FILE_PATH);
            if(f.exists()) {
                if (FileDeleteUtil.deleteFile(f) == 1) {
                    log.info(EKVRAW_FILE_PATH + " has deleted");
                } else {
                    log.info(EKVRAW_FILE_PATH + " has failed to delete");
                }
            }else{
                log.info(EKVRAW_FILE_PATH + " does not exist");
            }
        }
    }

    public void runModeConvert(String inputPath, String outPutPath, String type) throws Exception {
        if (DirGetAllFiles.getAllFilesInDir(inputPath, ".csv").length == 0) {
            log.info("[BackfillController.runModeConvert] inputPath:" + inputPath + " with fileEndWith:" + ".csv" + " is empty");
            return;
        }

        googleCloudFileToNetezzaFileConvertor.process(inputPath, outPutPath, type);
    }

    private void runModedumpEkvrawFromNetezza(String table, String partition, String curYear, String curYearMonth) throws Exception {
        // true then partition by date, false then partition by reminder of event_id,
        if (partition == null) {
            netezzaConnector.dataToCsvPartitionByYearMonth(this.mode, table, curYear, curYearMonth);
        } else {
            netezzaConnector.dataToCsvPartitionByMod(table, partition);
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

    private String[] curYearMonthPlusOne(String curYear, String curYearMonth, String endYear, String endYearMonth){
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
        String[] result = new String[2];
        result[0] = curYear;
        result[1] = curYearMonth;
        return result;
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
            DirCreateUtil.createDirectory(new File(DEFAULT_FILE_PATH + "ekvrawFile/"));
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
