package com.yizhao.apps;


import com.yizhao.apps.Connector.NetezzaConnector;
import com.yizhao.apps.Converter.GoogleCloudFileToNetezzaFileConvertor;
import com.yizhao.apps.Processor.FastrackFileProcessor;
import com.yizhao.apps.Scanner.FileCrawler.FileCrawler;
import com.yizhao.apps.Scanner.FileFilter.fastrackFileFilter;
import com.yizhao.apps.Scanner.FileProcessor.FileProcessor;
import com.yizhao.apps.Util.DateUtil;
import com.yizhao.apps.Util.FileUtils.DirCreateUtil;
import com.yizhao.apps.Util.FileUtils.FileDeleteUtil;
import com.yizhao.apps.Util.FileUtils.FileMoveUtil;
import com.yizhao.apps.Util.MathUtil;
import com.yizhao.apps.Util.ThreadUtils.general.ThreadUtil;
import com.yizhao.apps.Util.ThreadUtils.threadsignaling.MyWaitNotify;
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

    private static final MyWaitNotify mMyWaitNotify = new MyWaitNotify();
    private static Map<String, ExecutorService> threadPools = new HashMap<String, ExecutorService>();
    public GoogleCloudFileToNetezzaFileConvertor googleCloudFileToNetezzaFileConvertor = null;
    public FastrackFileProcessor fastrackFileProcessor = null;
    private NetezzaConnector netezzaConnector = null;

    /**
     * for general
     */
    private String mode = null; // convert is convert google cloud files to Netezza file, dump is dump the ekvraw and consolited them by event_id

    /**
     * for mode=backfill & dump
     */
    private String option = null; // r is partition by reminder, d is partition by date
    private String table = null;
    private String startDate = null;
    private String endDate = null;

    /**
     * for mode=convert
     */
    private String inputPath = null;
    private String outPutPath = null;
    private String monthYear = null;
    private String type = null; // hotel or flight
    private String partition = null;

    public static MyWaitNotify getmMyWaitNotify() {
        return mMyWaitNotify;
    }

    public void runBackfill(String table, String csvFileOutputPath, String partition, String curYear, String curYearMonth, String fastrackFileOutputPath, String fileHostName) throws Exception {
        String processedGoogleCloudHotelFilePath = "/home/yzhao/processedFiles/googleCloud/" + table + "/hotel/" + curYear + "-" + curYearMonth;
        String processedGoogleCloudFlightFilePath = "/home/yzhao/processedFiles/googleCloud/" + table + "/flight/" + curYear + "-" + curYearMonth;
        String processedNetezzaHotelFilePath = "/home/yzhao/processedFiles/netezza/" + table + "/hotel/" + curYear + "-" + curYearMonth;
        String processedNetezzaFlightFilePath = "/home/yzhao/processedFiles/netezza/" + table + "/flight/" + curYear + "-" + curYearMonth;


        // Step 1 - dumpEkvrawFromNetezza
        log.info("------------Executing Step 1------------");
        dumpEkvrawFromNetezza(table, csvFileOutputPath, partition, curYear, curYearMonth);

        // Step 2 - processEkvrawToGenerateFastrackFile
        log.info("------------Executing Step 2------------");
        processEkvrawToGenerateFastrackFile(csvFileOutputPath, fastrackFileOutputPath, fileHostName);

        // Step 3 - move fastrack file to udcuv2 inbox
        log.info("------------Executing Step 3------------");
        File file = new File(fastrackFileOutputPath);
        File toDirectory = new File("/opt/opinmind/var/udcuv2/inbox");
        FileMoveUtil.moveFile(file, toDirectory);

        // Step 4 - make sure there is no files in following dirs
        log.info("------------Executing Step 4------------");
        dirCleanThread("/opt/opinmind/var/hdfs/ekv/archive");
        dirCleanThread("/opt/opinmind/var/google/ekvraw/error");

        // Step 5 - to know the udcuv2 finish up processing the file
        log.info("------------Executing Step 5------------");
        detectUdcuv2Finish("/opt/opinmind/var/udcuv2/archive");

        // Step 6 - move hotel files
        log.info("------------Executing Step 6------------");
        FileMoveUtil.moveFilesUnderDir("/opt/opinmind/var/google/ekvhotel/concat", ".csv", new File("/opt/opinmind/var/google/ekvhotel/error"));

        DirCreateUtil.createDirectory(new File(processedGoogleCloudHotelFilePath));
        FileMoveUtil.moveFilesUnderDir("/opt/opinmind/var/google/ekvhotel/error", ".csv", new File(processedGoogleCloudHotelFilePath));

        // Step 7 - move flight files
        log.info("------------Executing Step 7------------");
        FileMoveUtil.moveFilesUnderDir("/opt/opinmind/var/google/ekvflight/concat", ".csv", new File("/opt/opinmind/var/google/ekvflight/error"));

        DirCreateUtil.createDirectory(new File(processedGoogleCloudFlightFilePath));
        FileMoveUtil.moveFilesUnderDir("/opt/opinmind/var/google/ekvflight/error", ".csv", new File(processedGoogleCloudFlightFilePath));

        // Step 8 - convert google hotel file to Netezza file
        log.info("------------Executing Step 8------------");
        googleCloudFileToNetezzaFileConvertor.process(processedGoogleCloudHotelFilePath, processedNetezzaHotelFilePath, "hotel");

        // Step 9 - convert google flight file to Netezza file
        log.info("------------Executing Step 9------------");
        googleCloudFileToNetezzaFileConvertor.process(processedGoogleCloudFlightFilePath, processedNetezzaFlightFilePath, "flight");


    }

    public void dumpEkvrawFromNetezza(String table, String csvFileOutputPath, String partition, String curYear, String curYearMonth) throws Exception {
        // true then partition by date, false then partition by reminder of event_id,
        if (partition == null) {
            netezzaConnector.dataToCsvPartitionByYearMonth(table, csvFileOutputPath, curYear, curYearMonth);
        } else {
            netezzaConnector.dataToCsvPartitionByMod(table, csvFileOutputPath, partition);
        }
    }

    private void processEkvrawToGenerateFastrackFile(String csvFileOutputPath, String fastrackFileOutputPath, String fileHostName) {
        log.info("done with ekv raws to CSV file \n");
        String currentDate = DateUtil.getCurrentDate("yyyyMMdd");
        String timeStamp = null;

        fastrackFileProcessor.execute(csvFileOutputPath, fastrackFileOutputPath + currentDate + "-000000" + "." + fileHostName + "." + timeStamp + "000" + ".csv.force");
        log.info("done with CSV file to fastrack file\n");
        File f = new File(csvFileOutputPath);
        if (FileDeleteUtil.deleteFile(f) == 1) {
            log.info(csvFileOutputPath + " has deleted" + "\n");
        } else {
            log.info(csvFileOutputPath + " has failed to delete" + "\n");
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

    public GoogleCloudFileToNetezzaFileConvertor getGoogleCloudFileToNetezzaFileConvertor() {
        return googleCloudFileToNetezzaFileConvertor;
    }

    public void setGoogleCloudFileToNetezzaFileConvertor(GoogleCloudFileToNetezzaFileConvertor googleCloudFileToNetezzaFileConvertor) {
        this.googleCloudFileToNetezzaFileConvertor = googleCloudFileToNetezzaFileConvertor;
    }

    public FastrackFileProcessor getFastrackFileProcessor() {
        return fastrackFileProcessor;
    }

    public void setFastrackFileProcessor(FastrackFileProcessor fastrackFileProcessor) {
        this.fastrackFileProcessor = fastrackFileProcessor;
    }

    public NetezzaConnector getNetezzaConnector() {
        return netezzaConnector;
    }

    public void setNetezzaConnector(NetezzaConnector netezzaConnector) {
        this.netezzaConnector = netezzaConnector;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getOption() {
        return option;
    }

    public void setOption(String option) {
        this.option = option;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getStartDate() {
        return startDate;
    }

    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    public String getEndDate() {
        return endDate;
    }

    public void setEndDate(String endDate) {
        this.endDate = endDate;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getOutPutPath() {
        return outPutPath;
    }

    public void setOutPutPath(String outPutPath) {
        this.outPutPath = outPutPath;
    }

    public String getInputPath() {
        return inputPath;
    }

    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }

    public String getMonthYear() {
        return monthYear;
    }

    public void setMonthYear(String monthYear) {
        this.monthYear = monthYear;
    }

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public void init() {
        System.out.println("hahahahahahahahahahahahahahahahahahahahahahahahahahahahahahahahahahahahahahahahahahahahahaha");




    }

    public void destroy() {
        ThreadUtil.stopAllThreads(threadPools, "BackfillMain", 5000L, TimeUnit.MILLISECONDS);
    }
}
