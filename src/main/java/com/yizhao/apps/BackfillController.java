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
 *
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
 *
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
 *
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
    private static Map<String, ExecutorService> threadPools = new HashMap<String, ExecutorService>();
    private static final String DEFAULT_FILE_PATH = "/home/yzhao/ENG835/";
    private static final MyWaitNotify mMyWaitNotify = new MyWaitNotify();

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

    /**
     * @param argv
     */
    public static void main(String[] argv) {

    }

    private static void runBackfill(String table, String csvFileOutputPath, String partition, String curYear, String curYearMonth, String fastrackFileOutputPath, String fileHostName) throws Exception{
        String processedGoogleCloudHotelFilePath = "/home/yzhao/processedFiles/googleCloud/" + table + "/hotel/" + curYear + "-" + curYearMonth;
        String processedGoogleCloudFlightFilePath = "/home/yzhao/processedFiles/googleCloud/" + table + "/flight/" + curYear + "-" + curYearMonth;
        String processedNetezzaHotelFilePath = "/home/yzhao/processedFiles/netezza/" + table + "/hotel/" + curYear + "-" + curYearMonth;
        String processedNetezzaFlightFilePath = "/home/yzhao/processedFiles/netezza/" + table + "/flight/" + curYear + "-" + curYearMonth;


        // Step 1 - dumpEkvrawFromNetezza
        System.out.println("------------Executing Step 1------------");
        dumpEkvrawFromNetezza(table, csvFileOutputPath,  partition, curYear, curYearMonth);

        // Step 2 - processEkvrawToGenerateFastrackFile
        System.out.println("------------Executing Step 2------------");
        processEkvrawToGenerateFastrackFile(csvFileOutputPath, fastrackFileOutputPath, fileHostName);

        // Step 3 - move fastrack file to udcuv2 inbox
        System.out.println("------------Executing Step 3------------");
        File file = new File(fastrackFileOutputPath);
        File toDirectory = new File("/opt/opinmind/var/udcuv2/inbox");
        FileMoveUtil.moveFile(file, toDirectory);

        // Step 4 - make sure there is no files in following dirs
        System.out.println("------------Executing Step 4------------");
        dirCleanThread("/opt/opinmind/var/hdfs/ekv/archive");
        dirCleanThread("/opt/opinmind/var/google/ekvraw/error");

        // Step 5 - to know the udcuv2 finish up processing the file
        System.out.println("------------Executing Step 5------------");
        detectUdcuv2Finish("/opt/opinmind/var/udcuv2/archive");

        // Step 6 - move hotel files
        System.out.println("------------Executing Step 6------------");
        FileMoveUtil.moveFilesUnderDir("/opt/opinmind/var/google/ekvhotel/concat",".csv", new File("/opt/opinmind/var/google/ekvhotel/error"));

        DirCreateUtil.createDirectory(new File(processedGoogleCloudHotelFilePath));
        FileMoveUtil.moveFilesUnderDir("/opt/opinmind/var/google/ekvhotel/error",".csv", new File(processedGoogleCloudHotelFilePath));

        // Step 7 - move flight files
        System.out.println("------------Executing Step 7------------");
        FileMoveUtil.moveFilesUnderDir("/opt/opinmind/var/google/ekvflight/concat",".csv", new File("/opt/opinmind/var/google/ekvflight/error"));

        DirCreateUtil.createDirectory(new File(processedGoogleCloudFlightFilePath));
        FileMoveUtil.moveFilesUnderDir("/opt/opinmind/var/google/ekvflight/error",".csv", new File(processedGoogleCloudFlightFilePath));

        // Step 8 - convert google hotel file to Netezza file
        System.out.println("------------Executing Step 8------------");
        GoogleCloudFileToNetezzaFileConvertor.process(processedGoogleCloudHotelFilePath, processedNetezzaHotelFilePath, "hotel");

        // Step 9 - convert google flight file to Netezza file
        System.out.println("------------Executing Step 9------------");
        GoogleCloudFileToNetezzaFileConvertor.process(processedGoogleCloudFlightFilePath, processedNetezzaFlightFilePath, "flight");


    }

    private static void dumpEkvrawFromNetezza(String table, String csvFileOutputPath, String partition, String curYear, String curYearMonth) throws Exception {
        // true then partition by date, false then partition by reminder of event_id,
        if(partition == null) {
            NetezzaConnector.dataToCsvPartitionByYearMonth(table, csvFileOutputPath, curYear, curYearMonth);
        }else {
            NetezzaConnector.dataToCsvPartitionByMod(table, csvFileOutputPath, partition);
        }
    }


    private static void processEkvrawToGenerateFastrackFile(String csvFileOutputPath, String fastrackFileOutputPath, String fileHostName){
        System.out.println("done with ekv raws to CSV file \n");
        String currentDate = DateUtil.getCurrentDate("yyyyMMdd");
        String timeStamp = null;

        FastrackFileProcessor.execute(csvFileOutputPath, fastrackFileOutputPath + currentDate + "-000000" + "." + fileHostName + "." + timeStamp + "000" + ".csv.force");
        System.out.println("done with CSV file to fastrack file\n");
        File f = new File(csvFileOutputPath);
        if (FileDeleteUtil.deleteFile(f) == 1) {
            System.out.println(csvFileOutputPath + " has deleted" + "\n");
        } else {
            System.out.println(csvFileOutputPath + " has failed to delete" + "\n");
        }
    }


    private static void dirCleanThread(String fileSourceInput){
        ExecutorService threadPool = ThreadUtil.newThread(threadPools, fileSourceInput, true, Thread.NORM_PRIORITY);
        File inputDir = new File(fileSourceInput);
        File outputDir = null;

        BlockingQueue blockingQueue = new ArrayBlockingQueue(5);
        threadPool.execute(new FileCrawler(blockingQueue, new fastrackFileFilter(), inputDir));
        threadPool.execute(new FileProcessor(blockingQueue, outputDir, "delete"));
    }

    private static void detectUdcuv2Finish(String fileSourceInput){
        ExecutorService threadPool = ThreadUtil.newThread(threadPools, fileSourceInput, true, Thread.NORM_PRIORITY);
        File inputDir = new File(fileSourceInput);
        File outputDir = null;

        BlockingQueue blockingQueue = new ArrayBlockingQueue(5);
        threadPool.execute(new FileCrawler(blockingQueue, new fastrackFileFilter(), inputDir));
        threadPool.execute(new FileProcessor(blockingQueue, outputDir, "foundNewFileInDir"));
    }

    public static MyWaitNotify getmMyWaitNotify() {
        return mMyWaitNotify;
    }

    public void init() {

        if(mode == null){
            return;
        }

        try {
            if (mode.equals("convert")) {
                if(inputPath == null){
                    return;
                }

                if(outPutPath == null){
                    return;
                }

                if(monthYear == null){
                    return;
                }

                if(type == null){
                    return;
                }

                GoogleCloudFileToNetezzaFileConvertor.process(inputPath, outPutPath + "/ekv_" + type + "_all_netezza-" + monthYear + "_"  + type + "_001.csv", type);
            } else if (mode.equals("dump")) {
                if(option == null){
                    return;
                }

                if(option.equals("d")){
                    if (startDate == null) {
                        System.out.println("argument 2 startYearDate is missing");
                        return;
                    }
                } else if (option.equals("r")) {
                    if(partition == null){
                        return;
                    }
                }

                String partition = null;
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

                if (table == null) {
                    System.out.println("argument 1 taleName is missing");
                    return;
                }


                String csvFileOutputPath = DEFAULT_FILE_PATH + table + "_csvFileOutputPath.csv";
                String fastrackFileOutputPath = DEFAULT_FILE_PATH;


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

                if (option.equals("d")) {
                    System.out.println("startYear:" + startYear);
                    System.out.println("startYearMonth:" + startYearMonth);
                    System.out.println("endYear:" + endYear);
                    System.out.println("endYearMonth:" + endYearMonth);


                    if (endDate != null) {
                        int count = 0;
                        String curYear = startYear;
                        String curYearMonth = startYearMonth;
                        while (!curYear.equals(endYear) || !curYearMonth.equals(endYearMonth)) {
                            runBackfill(table, csvFileOutputPath, null, curYear, curYearMonth, fastrackFileOutputPath, fileHostName);

                            count++;
                            if (!curYear.equals(endYear) && !curYearMonth.equals("12")) {
                                curYearMonth = new String(MathUtil.plusOne(curYearMonth.toCharArray()));
                            } else if (!curYear.equals(endYear) && curYearMonth.equals("12")) {
                                curYear = new String(MathUtil.plusOne(curYear.toCharArray()));
                                curYearMonth = "01";
                            } else if (curYear.equals(endYear) && !curYearMonth.equals(endYearMonth)) {
                                curYearMonth = new String(MathUtil.plusOne(curYearMonth.toCharArray()));
                            } else {
                                System.out.println("curYear and curYearMonth are same as endYear and endYearMonth");
                            }
                        }

                        // run for the final month
                        runBackfill(table, csvFileOutputPath, null, curYear, curYearMonth, fastrackFileOutputPath, fileHostName);
                    } else {
                        // only get one month
                        runBackfill(table, csvFileOutputPath, null, startYear, startYearMonth, fastrackFileOutputPath, fileHostName);
                    }
                } else if (option.equals("r")) {
                    if (partition == null) {
                        int i = 0;
                        while (i < 10) {
                            runBackfill(table, csvFileOutputPath,  String.valueOf(i), null, null, fastrackFileOutputPath, fileHostName);
                            i++;
                        }
                    } else {
                        runBackfill(table, csvFileOutputPath,  partition, null, null, fastrackFileOutputPath, fileHostName);
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Exception in Main:" + "\n");
            e.printStackTrace();
        }

    }

    public void destroy() {
        ThreadUtil.stopAllThreads(threadPools, "BackfillMain", 5000L, TimeUnit.MILLISECONDS);
    }
}
