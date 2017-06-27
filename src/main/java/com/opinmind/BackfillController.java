package com.opinmind;


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
    private static Map<String, ExecutorService> threadPools = new HashMap<String, ExecutorService>();
    public EkvrawToFastrackFileConvertor ekvrawToFastrackFileConvertor = null;
    private String ekvrawFileOutputPath = DEFAULT_FILE_PATH + "ekvrawFile/";
    private String fastrackFileOutputPath = DEFAULT_FILE_PATH + "fastrackFile/";
    private String googleCloudFiles = DEFAULT_FILE_PATH + "processedFiles/googleCloud/";
    private String netezzaCloudFiles = DEFAULT_FILE_PATH + "processedFiles/netezza/";
    private GoogleCloudFileToNetezzaFileConvertor googleCloudFileToNetezzaFileConvertor = null;
    private NetezzaConnector netezzaConnector = null;

    public static MyWaitNotify getmMyWaitNotify() {
        return mMyWaitNotify;
    }

    public void runModeBackfill(String option, String table, String startDate, String endDate, String partition) throws Exception {
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


        ekvrawFileOutputPath = table + "_ekvraw.csv";

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
            log.info("startYear:" + startYear);
            log.info("startYearMonth:" + startYearMonth);
            log.info("endYear:" + endYear);
            log.info("endYearMonth:" + endYearMonth);


            if (endDate != null) {
                int count = 0;
                String curYear = startYear;
                String curYearMonth = startYearMonth;
                while (!curYear.equals(endYear) || !curYearMonth.equals(endYearMonth)) {
                    runBackfill(table, ekvrawFileOutputPath, null, curYear, curYearMonth, fastrackFileOutputPath, fileHostName);

                    count++;
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
                }

                // run for the final month
                runBackfill(table, ekvrawFileOutputPath, null, curYear, curYearMonth, fastrackFileOutputPath, fileHostName);
            } else {
                // only get one month
                runBackfill(table, ekvrawFileOutputPath, null, startYear, startYearMonth, fastrackFileOutputPath, fileHostName);
            }
        } else if (option.equals("r")) {
            if (partition == null) {
                int i = 0;
                while (i < 10) {
                    runBackfill(table, ekvrawFileOutputPath, String.valueOf(i), null, null, fastrackFileOutputPath, fileHostName);
                    i++;
                }
            } else {
                runBackfill(table, ekvrawFileOutputPath, partition, null, null, fastrackFileOutputPath, fileHostName);
            }
        }
    }

    public void runModeDumpEKVraw(String option, String table, String startDate, String endDate, String partition) throws Exception {
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


        ekvrawFileOutputPath = table + "_ekvraw.csv";

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
            log.info("startYear:" + startYear);
            log.info("startYearMonth:" + startYearMonth);
            log.info("endYear:" + endYear);
            log.info("endYearMonth:" + endYearMonth);


            if (endDate != null) {
                int count = 0;
                String curYear = startYear;
                String curYearMonth = startYearMonth;
                while (!curYear.equals(endYear) || !curYearMonth.equals(endYearMonth)) {
                    runBackfill(table, ekvrawFileOutputPath, null, curYear, curYearMonth, fastrackFileOutputPath, fileHostName);

                    count++;
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
                }

                // run for the final month
                dumpEkvrawFromNetezza(table, ekvrawFileOutputPath, partition, curYear, curYearMonth);
            } else {
                // only get one month
                dumpEkvrawFromNetezza(table, ekvrawFileOutputPath, partition, startYear, startYearMonth);
            }
        } else if (option.equals("r")) {
            if (partition == null) {
                int i = 0;
                while (i < 10) {
                    dumpEkvrawFromNetezza(table, ekvrawFileOutputPath, String.valueOf(i), null, null);
                    i++;
                }
            } else {
                dumpEkvrawFromNetezza(table, ekvrawFileOutputPath, partition, null, null);
            }
        }
    }

    public void runModeConvert(String inputPath, String outPutPath, String monthYear, String type) throws Exception {
        if (inputPath == null) {
            log.error("inputPath is null");
            return;
        }

        if (outPutPath == null) {
            log.error("outPutPath is null");
            return;
        }

        if (monthYear == null) {
            log.error("monthYear is null");
            return;
        }

        if (type == null) {
            log.error("type is null");
            return;
        }

        googleCloudFileToNetezzaFileConvertor.process(inputPath, outPutPath + "/ekv_" + type + "_all_netezza-" + monthYear + "_" + type + "_001.csv", type);
    }

    private void runBackfill(String table, String csvFileOutputPath, String partition, String curYear, String curYearMonth, String fastrackFileOutputPath, String fileHostName) throws Exception {
        log.info("[BackfillController.runBackfill] with table:" + table + " ,csvFileOutputPath:" + csvFileOutputPath + " ,partition:" + partition + " ,curYear:" + curYear + " ,curYearMonth:" + curYearMonth + " ,fastrackFileOutputPath:" + fastrackFileOutputPath + " ,fileHostName:" + fileHostName);

        String processedGoogleCloudHotelFilePath = googleCloudFiles + table + "/hotel/" + curYear + "-" + curYearMonth;
        String processedGoogleCloudFlightFilePath = googleCloudFiles + table + "/flight/" + curYear + "-" + curYearMonth;
        String processedNetezzaHotelFilePath = netezzaCloudFiles + table + "/hotel/" + curYear + "-" + curYearMonth;
        String processedNetezzaFlightFilePath = netezzaCloudFiles + table + "/flight/" + curYear + "-" + curYearMonth;


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
        FileMoveUtil.moveFilesUnderDir(fastrackFileOutputPath, ".force", toDirectory);

        // Step 4 - make sure there is no files in following dirs
        log.info("------------Executing Step 4------------");
        dirCleanThread("/opt/opinmind/var/hdfs/ekv/archive");
        dirCleanThread("/opt/opinmind/var/google/ekvraw/error");

        // Step 5 - to know the udcuv2 finish up processing the file
        log.info("------------Executing Step 5------------");
        while (DirGetAllFiles.getAllFilesInDir("/opt/opinmind/var/udcuv2/inbox", ".force").length != 0) { // when inbox has no file, then udcuv2 finished
            // do nothing keep running
        }
        // detectUdcuv2Finish("/opt/opinmind/var/udcuv2/archive");
        // detectUdcuv2Finish("/opt/opinmind/var/google/ekvhotel/concat");

        // Step 6 - move hotel files
        log.info("------------Executing Step 6------------");
        while (DirGetAllFiles.getAllFilesInDir("/opt/opinmind/var/google/ekvhotel/concat", ".csv").length != 0) {
            FileMoveUtil.moveFilesUnderDir("/opt/opinmind/var/google/ekvhotel/concat", ".csv", new File("/opt/opinmind/var/google/ekvhotel/error"));
            break;
        }
        DirCreateUtil.createDirectory(new File(processedGoogleCloudHotelFilePath));
        if (DirGetAllFiles.getAllFilesInDir("/opt/opinmind/var/google/ekvhotel/error", ".csv").length != 0) {
            FileMoveUtil.moveFilesUnderDir("/opt/opinmind/var/google/ekvhotel/error", ".csv", new File(processedGoogleCloudHotelFilePath));
        }


        // Step 7 - move flight files
        log.info("------------Executing Step 7------------");
        while (DirGetAllFiles.getAllFilesInDir("/opt/opinmind/var/google/ekvflight/concat", ".csv").length != 0) {
            FileMoveUtil.moveFilesUnderDir("/opt/opinmind/var/google/ekvflight/concat", ".csv", new File("/opt/opinmind/var/google/ekvflight/error"));
            break;
        }
        DirCreateUtil.createDirectory(new File(processedGoogleCloudFlightFilePath));
        if (DirGetAllFiles.getAllFilesInDir("/opt/opinmind/var/google/ekvflight/error", ".csv").length != 0) {
            FileMoveUtil.moveFilesUnderDir("/opt/opinmind/var/google/ekvflight/error", ".csv", new File(processedGoogleCloudFlightFilePath));
        }

        // Step 8 - convert google hotel file to Netezza file
        log.info("------------Executing Step 8------------");
        DirCreateUtil.createDirectory(new File(processedNetezzaHotelFilePath));
        if (DirGetAllFiles.getAllFilesInDir(processedGoogleCloudHotelFilePath, ".csv").length != 0) {
            googleCloudFileToNetezzaFileConvertor.process(processedGoogleCloudHotelFilePath, processedNetezzaHotelFilePath + "/ekv_hotel" + "_all_netezza-" + curYear + "-" + curYearMonth + "_" + "hotel" + "_001.csv", "hotel");
        }

        // Step 9 - convert google flight file to Netezza file
        log.info("------------Executing Step 9------------");
        DirCreateUtil.createDirectory(new File(processedNetezzaFlightFilePath));
        if (DirGetAllFiles.getAllFilesInDir(processedGoogleCloudFlightFilePath, ".csv").length != 0) {
            googleCloudFileToNetezzaFileConvertor.process(processedGoogleCloudFlightFilePath, processedNetezzaFlightFilePath + "/ekv_flight" + "_all_netezza-" + curYear + "-" + curYearMonth + "_" + "flight" + "_001.csv", "flight");
        }

    }

    private void dumpEkvrawFromNetezza(String table, String csvFileOutputPath, String partition, String curYear, String curYearMonth) throws Exception {
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
        String timeStamp = DateCalendar.getUnixTimeStamp();

        ekvrawToFastrackFileConvertor.execute(csvFileOutputPath, fastrackFileOutputPath + currentDate + "-000000" + "." + fileHostName + "." + timeStamp + "000" + ".csv.force");
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

    public EkvrawToFastrackFileConvertor getEkvrawToFastrackFileConvertor() {
        return ekvrawToFastrackFileConvertor;
    }

    public void setEkvrawToFastrackFileConvertor(EkvrawToFastrackFileConvertor ekvrawToFastrackFileConvertor) {
        this.ekvrawToFastrackFileConvertor = ekvrawToFastrackFileConvertor;
    }

    public NetezzaConnector getNetezzaConnector() {
        return netezzaConnector;
    }

    public void setNetezzaConnector(NetezzaConnector netezzaConnector) {
        this.netezzaConnector = netezzaConnector;
    }

    public void init() {
        try {
            FileDeleteUtil.deleteDirAndItsSubDirs(new File(DEFAULT_FILE_PATH));
        } catch (Exception e) {
            log.error("[BackfillController.init]: ", e);
        }
      /*  try {
            FileDeleteUtil.deleteFilesUnderDir(ekvrawFileOutputPath, ".csv");
        } catch (Exception e) {
            log.error("[BackfillController.init]: ", e);
        }

        try {
            FileDeleteUtil.deleteFilesUnderDir(fastrackFileOutputPath, ".csv.force");
        } catch (Exception e) {
            log.error("[BackfillController.init]: ", e);
        }
*/
        try {
            DirCreateUtil.createDirectory(new File(DEFAULT_FILE_PATH));
            DirCreateUtil.createDirectory(new File(ekvrawFileOutputPath));
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
