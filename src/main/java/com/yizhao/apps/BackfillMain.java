package com.yizhao.apps;


import com.yizhao.apps.Connector.NetezzaConnector;
import com.yizhao.apps.Processor.FastrackFileProcessor;
import com.yizhao.apps.Util.DateUtil;
import com.yizhao.apps.Util.FileDeleteUtil;
import com.yizhao.apps.Util.MathUtil;

import java.io.File;
import java.net.InetAddress;


/**
 * @author YI ZHAO
 *         <p>
 *         build it:
 *              mvn clean package
 *              scp /Users/yzhao/IdeaProjects/ENG835_Backfill/target/Backfill-jar-with-dependencies.jar manager:/home/yzhao/
 *         <p>
 *         Run it:
 *              java -jar Backfill-jar-with-dependencies.jar d eng759_backfill_apac 2016-12 2017-03
 *              java -jar Backfill-jar-with-dependencies.jar d eng759_backfill_apac  2016-12
 *              java -jar Backfill-jar-with-dependencies.jar r eng759_backfill_apac  0
 *
 *
 *
 *              java -jar Backfill-jar-with-dependencies.jar d ENG759_BACKFILL_PRICELINE 2016-03 2017-03
 */
public class BackfillMain {
    private static final String DEFAULT_FILE_PATH = "/workplace/yzhao/";
    /**
     * @param argv
     */
    public static void main(String[] argv) {
        String mode = null; // r is partition by reminder, d is partition by date
        String table = null;
        String partition = null;
        String startYearDate = null;
        String startYear = null;
        String startYearMonth = null;
        String endYearDate = null;
        String endYear = null;
        String endYearMonth = null;
        try {
            mode = argv[0];
            if(argv.length >= 2) {
                table = argv[1];
            }

            if(mode.equals("d")) {
                if (argv.length >= 3) {
                    startYearDate = argv[2];
                    String[] startYearDateStr = startYearDate.split("-");
                    startYear = startYearDateStr[0];
                    startYearMonth = startYearDateStr[1];
                }

                if (argv.length == 4) {
                    endYearDate = argv[3];
                    String[] endYearDateStr = endYearDate.split("-");
                    endYear = endYearDateStr[0];
                    endYearMonth = endYearDateStr[1];
                }

                if(startYearDate == null){
                    System.out.println("argument 2 startYearDate is missing");
                    return;
                }
            }else if(mode.equals("r")){
                if (argv.length >= 3) {
                    partition = argv[2];
                }
            }

            if(table == null){
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
            if(CurrentHostName.contains("qa") || CurrentHostName.contains("manager")){
                fileHostName = "qa1-ps1-lax1";
            }else{
                fileHostName = "ps";
            }

            if(mode.equals("d")) {
                System.out.println("startYear:" + startYear);
                System.out.println("startYearMonth:" + startYearMonth);
                System.out.println("endYear:" + endYear);
                System.out.println("endYearMonth:" + endYearMonth);


                if (endYearDate != null) {
                    int count = 0;
                    String curYear = startYear;
                    String curYearMonth = startYearMonth;
                    while (!curYear.equals(endYear) || !curYearMonth.equals(endYearMonth)) {
                        NetezzaConnector.dataToCsvPartitionByYearMonth(table, csvFileOutputPath, curYear, curYearMonth);
                        System.out.println("done with ekv raws to CSV file \n");
                        String currentDate = DateUtil.getCurrentDate();
                        String timeStamp = String.valueOf(DateUtil.getCurrentTimeInUnixTimestamp());


                        FastrackFileProcessor.execute(csvFileOutputPath, fastrackFileOutputPath + currentDate + "-00000" + count + "." + fileHostName + "." + timeStamp + "000" + ".csv");
                        System.out.println("done with CSV file to fastrack file\n");
                        File f = new File(csvFileOutputPath);
                        if (FileDeleteUtil.deleteFile(f) == 1) {
                            System.out.println(csvFileOutputPath + " has deleted" + "\n");
                        } else {
                            System.out.println(csvFileOutputPath + " has failed to delete" + "\n");
                        }

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
                } else {
                    String currentDate = DateUtil.getCurrentDate();
                    String timeStamp = String.valueOf(DateUtil.getCurrentTimeInUnixTimestamp());

                    NetezzaConnector.dataToCsvPartitionByYearMonth(table, csvFileOutputPath, startYear, startYearMonth);
                    System.out.println("done with ekv raws to CSV file \n");
                    FastrackFileProcessor.execute(csvFileOutputPath, fastrackFileOutputPath + currentDate + "-00000" + "0" + "." + fileHostName + "." + timeStamp + "000" + ".csv");
                    System.out.println("done with CSV file to fastrack file\n");

                    File f = new File(csvFileOutputPath);
                    if (FileDeleteUtil.deleteFile(f) == 1) {
                        System.out.println(csvFileOutputPath + " has deleted" + "\n");
                    } else {
                        System.out.println(csvFileOutputPath + " has failed to delete" + "\n");
                    }
                }
            }else if(mode.equals("r")){
                if (partition == null) {
                    int i = 0;
                    while(i < 10){
                        NetezzaConnector.dataToCsvPartitionByMod(table, csvFileOutputPath, String.valueOf(i));
                        System.out.println("done with ekv raws to CSV file \n");
                        String currentDate = DateUtil.getCurrentDate();
                        String timeStamp = String.valueOf(DateUtil.getCurrentTimeInUnixTimestamp());


                        FastrackFileProcessor.execute(csvFileOutputPath, fastrackFileOutputPath + currentDate + "-00000" + i + "." + fileHostName + "." + timeStamp + "000" + ".csv");
                        System.out.println("done with CSV file to fastrack file\n");
                        File f = new File(csvFileOutputPath);
                        if(FileDeleteUtil.deleteFile(f) == 1){
                            System.out.println(csvFileOutputPath + " has deleted" + "\n");
                        }else{
                            System.out.println(csvFileOutputPath + " has failed to delete" + "\n");
                        }

                        i++;
                    }
                } else {
                    String currentDate = DateUtil.getCurrentDate();
                    String timeStamp = String.valueOf(DateUtil.getCurrentTimeInUnixTimestamp());

                    NetezzaConnector.dataToCsvPartitionByMod(table, csvFileOutputPath, partition);
                    System.out.println("done with ekv raws to CSV file \n");
                    FastrackFileProcessor.execute(csvFileOutputPath, fastrackFileOutputPath + currentDate + "-00000" + partition + "." + fileHostName + "." + timeStamp + "000" + ".csv" );
                    System.out.println("done with CSV file to fastrack file\n");

                    File f = new File(csvFileOutputPath);
                    if(FileDeleteUtil.deleteFile(f) == 1){
                        System.out.println(csvFileOutputPath + " has deleted" + "\n");
                    }else{
                        System.out.println(csvFileOutputPath + " has failed to delete" + "\n");
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Exception in Main:" + "\n");
            e.printStackTrace();
        }
    }
}
