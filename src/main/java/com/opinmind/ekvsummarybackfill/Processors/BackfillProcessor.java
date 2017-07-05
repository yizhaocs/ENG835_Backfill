package com.opinmind.ekvsummarybackfill.Processors;


import com.opinmind.ekvsummarybackfill.Util.Constants;
import com.opinmind.ekvsummarybackfill.Connector.NetezzaConnector;
import com.opinmind.ekvsummarybackfill.Crawler.FileCrawler.FileCrawler;
import com.opinmind.ekvsummarybackfill.Crawler.FileFilter.fastrackFileFilter;
import com.opinmind.ekvsummarybackfill.Crawler.FileProcessor.FileProcessor;
import com.opinmind.ekvsummarybackfill.Util.EmailUtils.SendEmail;
import com.opinmind.ekvsummarybackfill.Util.FileUtils.DirCreateUtil;
import com.opinmind.ekvsummarybackfill.Util.FileUtils.DirGetAllFiles;
import com.opinmind.ekvsummarybackfill.Util.FileUtils.FileDeleteUtil;
import com.opinmind.ekvsummarybackfill.Util.FileUtils.FileMoveUtil;
import com.opinmind.ekvsummarybackfill.Util.MathUtil;
import com.opinmind.ekvsummarybackfill.Util.ThreadUtils.general.ThreadUtil;
import com.opinmind.ekvsummarybackfill.Util.ThreadUtils.threadsignaling.MyWaitNotify;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * @author YI ZHAO

scp /Users/yzhao/IdeaProjects/ENG835_Backfill/target/ekvsummarybackfill.war manager:/home/yzhao/
ssh manager
scp ekvsummarybackfill.war 68.67.146.136:/home/yzhao
ssh 68.67.146.136
sudo rm /opt/apache-tomcat/webapps/ekvsummarybackfill.war
sudo chown -R om:om ekvsummarybackfill.war
sudo mv ekvsummarybackfill.war /opt/apache-tomcat/webapps/
sudo rm -rf /opt/opinmind/var/
sudo rm -rf /opt/opinmind/logs/hdu/
sudo rm -rf /opt/opinmind/logs/ekvsummarybackfill/
sudo rm -rf /opt/opinmind/var/udcuv2/archive
sudo rm -rf /opt/opinmind/var/udcuv2/inbox
sudo /sbin/service tomcat stop_force
sudo /sbin/service tomcat start
ls -la /opt/opinmind/var/

 */
public class BackfillProcessor {
    private static final Logger log = Logger.getLogger(BackfillProcessor.class);

    private static final MyWaitNotify mMyWaitNotify = new MyWaitNotify();
    private static Map<String, ExecutorService> threadPools = new HashMap<String, ExecutorService>();
    private NetezzaConnector netezzaConnector = null;
    private EkvrawToFastrackProcessor ekvrawToFastrackProcessor = null;
    private ConvertProcessor convertProcessor = null;
    private String mode = null;
    private boolean isCleanStarted = false;


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
    public void execute(String mode, String option, String table, String startDate, String endDate, String partition, SendEmail sendEmail) {
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
                log.info("[BackfillController.execute] startYear:" + startYear + " ,startYearMonth:" + startYearMonth + " ,endYear:" + endYear + " ,endYearMonth:" + endYearMonth);

                if (mode.equals(Constants.Mode.TESTING_BACKFILL) || mode.equals(Constants.Mode.BACKFILL)) {
                    // unusedFileCLeanThread();
                    if (endDate != null) {
                        String curYear = startYear;
                        String curYearMonth = startYearMonth;
                        while (!curYear.equals(endYear) || !curYearMonth.equals(endYearMonth)) {
                            runBackfill(table, null, curYear, curYearMonth, sendEmail);
                            String[] yearMonth = curYearMonthPlusOne(curYear, curYearMonth, endYear, endYearMonth);
                            curYear = yearMonth[0];
                            curYearMonth = yearMonth[1];
                        }
                        // run for the final month
                        runBackfill(table, null, curYear, curYearMonth, sendEmail);
                    } else {
                        // only get one month
                        runBackfill(table, null, startYear, startYearMonth, sendEmail);
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
                    // unusedFileCLeanThread();
                    if (partition == null) {
                        int i = 0;
                        while (i < 10) {
                            runBackfill(table, String.valueOf(i), null, null, sendEmail);
                            i++;
                        }
                    } else {
                        runBackfill(table, partition, null, null, sendEmail);
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
            sendEmail.send(table, null, null, null,false, true);
            destroy();
            log.error("[BackfillController.execute]: ", e);
        }finally {

        }
    }

    private void runBackfill(String table, String partition, String curYear, String curYearMonth, SendEmail sendEmail) throws Exception {
        log.info("[BackfillController.runBackfill] with table:" + table + " ,partition:" + partition + " ,curYear:" + curYear + " ,curYearMonth:" + curYearMonth);

        String processedGoogleCloudHotelFilePath = Constants.Files.googleCloudFiles + table + "/hotel/" + curYear + "-" + curYearMonth;
        String processedGoogleCloudFlightFilePath = Constants.Files.googleCloudFiles + table + "/flight/" + curYear + "-" + curYearMonth;
        String processedNetezzaHotelFilePath = Constants.Files.netezzaCloudFiles + table + "/hotel/" + curYear + "-" + curYearMonth;
        String processedNetezzaFlightFilePath = Constants.Files.netezzaCloudFiles + table + "/flight/" + curYear + "-" + curYearMonth;


        // Step 1 - runModedumpEkvrawFromNetezza
        log.info("------------Executing Step 1 - runModedumpEkvrawFromNetezza------------");
        runModedumpEkvrawFromNetezza(table, partition, curYear, curYearMonth);

        // Step 2 - ekvrawToFastrackProcessor.execute
        log.info("------------Executing Step 2 - ekvrawToFastrackProcessor.execute------------");
        ekvrawToFastrackProcessor.execute(true);

        // Step 3 - move fastrack file to udcuv2 inbox
        log.info("------------Executing Step 3 - move fastrack file to udcuv2 inbox------------");
        File toDirectory = new File("/opt/opinmind/var/udcuv2/inbox");
        FileMoveUtil.moveFilesUnderDir(Constants.Files.fastrackFileOutputPath, ".force", toDirectory);

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
        convertProcessor.execute(processedGoogleCloudHotelFilePath, processedNetezzaHotelFilePath + "/ekv_hotel" + "_all_netezza-" + curYear + "-" + curYearMonth + "_" + Constants.Type.HOTEL + "_001.csv", Constants.Type.HOTEL);

        // Step 8 - convert google flight file to Netezza file
        log.info("------------Executing Step 8 - convert google flight file to Netezza file------------");
        DirCreateUtil.createDirectory(new File(processedNetezzaFlightFilePath));
        convertProcessor.execute(processedGoogleCloudFlightFilePath, processedNetezzaFlightFilePath + "/ekv_flight" + "_all_netezza-" + curYear + "-" + curYearMonth + "_" + Constants.Type.FLIGHT + "_001.csv", Constants.Type.FLIGHT);

        // Step 9 - sendEmail
        sendEmail.send(table, null, curYear, curYearMonth, false, false);
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

    public EkvrawToFastrackProcessor getEkvrawToFastrackProcessor() {
        return ekvrawToFastrackProcessor;
    }

    public void setEkvrawToFastrackProcessor(EkvrawToFastrackProcessor ekvrawToFastrackProcessor) {
        this.ekvrawToFastrackProcessor = ekvrawToFastrackProcessor;
    }

    public ConvertProcessor getConvertProcessor() {
        return convertProcessor;
    }

    public void setConvertProcessor(ConvertProcessor convertProcessor) {
        this.convertProcessor = convertProcessor;
    }

    public NetezzaConnector getNetezzaConnector() {
        return netezzaConnector;
    }


    public void setNetezzaConnector(NetezzaConnector netezzaConnector) {
        this.netezzaConnector = netezzaConnector;
    }

    private void unusedFileCLeanThread() {
        if(!isCleanStarted) {
            isCleanStarted = true;
            dirCleanThread("/opt/opinmind/var/hdfs/ekv/archive");
            dirCleanThread("/opt/opinmind/var/hdfs/ekv/concat");
            dirCleanThread("/opt/opinmind/var/google/ekvraw/error");
            dirCleanThread("/opt/opinmind/var/google/ekvraw/concat");
            dirCleanThread("/opt/opinmind/var/udcuv2/archive");
        }
    }

    public void init() {
        try {
            FileDeleteUtil.deleteDirAndItsSubDirs(new File(Constants.Files.DEFAULT_FILE_PATH));
        } catch (Exception e) {
            log.error("[BackfillController.init]: ", e);
        }

        try {
            DirCreateUtil.createDirectory(new File(Constants.Files.DEFAULT_FILE_PATH));
            DirCreateUtil.createDirectory(new File(Constants.Files.DEFAULT_FILE_PATH + "ekvrawFile/"));
            DirCreateUtil.createDirectory(new File(Constants.Files.fastrackFileOutputPath));
            DirCreateUtil.createDirectory(new File(Constants.Files.googleCloudFiles));
            DirCreateUtil.createDirectory(new File(Constants.Files.netezzaCloudFiles));
        } catch (Exception e) {
            log.error("[BackfillController.init]: ", e);
        }

        unusedFileCLeanThread();
    }

    public void destroy() {
        ThreadUtil.stopAllThreads(threadPools, "BackfillMain", 5000L, TimeUnit.MILLISECONDS);
    }
}
