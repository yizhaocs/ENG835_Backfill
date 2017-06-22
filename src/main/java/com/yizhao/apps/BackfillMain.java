package com.yizhao.apps;


import com.yizhao.apps.Connector.NetezzaConnector;
import com.yizhao.apps.Converter.GoogleCloudFileToNetezzaFileConvertor;
import com.yizhao.apps.Processor.FastrackFileProcessor;
import com.yizhao.apps.Scanner.FileCrawler.FileCrawler;
import com.yizhao.apps.Scanner.FileFilter.fastrackFileFilter;
import com.yizhao.apps.Scanner.FileProcessor.FileProcessor;
import com.yizhao.apps.Util.DateUtil;
import com.yizhao.apps.Util.FileUtils.FileDeleteUtil;
import com.yizhao.apps.Util.FileUtils.FileMoveUtil;
import com.yizhao.apps.Util.MathUtil;
import com.yizhao.apps.Util.ThreadUtils.ThreadUtil;

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
public class BackfillMain {
    static Map<String, ExecutorService> threadPools = new HashMap<String, ExecutorService>();
    private static final String DEFAULT_FILE_PATH = "/home/yzhao/ENG835/";

    /**
     * @param argv
     */
    public static void main(String[] argv) {
        String mode = null; // convert is convert google cloud files to Netezza file, dump is dump the ekvraw and consolited them by event_id

        try {
            mode = argv[0];
            if (mode.equals("convert")) {
                String inputPath = argv[1];
                String outPutPath = argv[2];
                String monthYear = argv[3];
                String type = argv[4];
                GoogleCloudFileToNetezzaFileConvertor.process(inputPath, outPutPath + "/ekv_" + type + "_all_netezza-" + monthYear + "_"  + type + "_001.csv", type);
            } else if (mode.equals("dump")) {

                String option = null; // r is partition by reminder, d is partition by date
                String table = null;
                String partition = null;
                String startYearDate = null;
                String startYear = null;
                String startYearMonth = null;
                String endYearDate = null;
                String endYear = null;
                String endYearMonth = null;

                option = argv[1];
                if (argv.length >= 3) {
                    table = argv[2];
                }

                if (option.equals("d")) {
                    if (argv.length >= 4) {
                        startYearDate = argv[5];
                        String[] startYearDateStr = startYearDate.split("-");
                        startYear = startYearDateStr[0];
                        startYearMonth = startYearDateStr[1];
                    }

                    if (argv.length == 5) {
                        endYearDate = argv[4];
                        String[] endYearDateStr = endYearDate.split("-");
                        endYear = endYearDateStr[0];
                        endYearMonth = endYearDateStr[1];
                    }

                    if (startYearDate == null) {
                        System.out.println("argument 2 startYearDate is missing");
                        return;
                    }
                } else if (option.equals("r")) {
                    if (argv.length >= 4) {
                        partition = argv[3];
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


                    if (endYearDate != null) {
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

    private static void runBackfill(String table, String csvFileOutputPath, String partition, String curYear, String curYearMonth, String fastrackFileOutputPath, String fileHostName) throws Exception{
            // Step 1 - dumpEkvrawFromNetezza
            dumpEkvrawFromNetezza(table, csvFileOutputPath,  partition, curYear, curYearMonth);
            // Step 2 - processEkvrawToGenerateFastrackFile
            processEkvrawToGenerateFastrackFile(csvFileOutputPath, fastrackFileOutputPath, fileHostName);
            // Step 3 - move fastrack file to udcuv2 inbox
            File file = new File(fastrackFileOutputPath);
            File toDirectory = new File("/opt/opinmind/var/udcuv2/inbox");
            FileMoveUtil.moveFile(file, toDirectory);
            // Step 4 - make sure there is no files in following dirs
            dirCleanThread("/opt/opinmind/var/hdfs/ekv/archive");
            dirCleanThread("/opt/opinmind/var/google/ekvraw/error");
            // Step 5 - to know the udcuv2 finish up processing the file
            detectUdcuv2Finish("/opt/opinmind/var/udcuv2/archive");


            // Step final - clean up all concat dir
            FileDeleteUtil.deleteFilesUnderDir("/opt/opinmind/var/google/ekvraw/concat", ".csv");
            FileDeleteUtil.deleteFilesUnderDir("/opt/opinmind/var/hdfs/ekv/concat", ".csv");
            // Step final - clean up all thread
            ThreadUtil.stopAllThreads(threadPools, "BackfillMain", 5000L, TimeUnit.MILLISECONDS);
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
}
