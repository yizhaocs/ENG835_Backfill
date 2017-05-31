package com.yizhao.apps;


import com.yizhao.apps.Connector.NetezzaConnector;
import com.yizhao.apps.Util.DateUtil;
import com.yizhao.apps.Util.FileDeleteUtil;

import java.io.File;


/**
 * @author YI ZHAO
 *         <p>
 *         build it:
 *              mvn clean package
 *         <p>
 *         Run it:
 *              java -jar Backfill-jar-with-dependencies.jar eng759_backfill_apac
 *              java -jar Backfill-jar-with-dependencies.jar eng759_backfill_apac  0
 *              java -jar Backfill-jar-with-dependencies.jar eng759_backfill_apac /workplace/yzhao/eng759_backfill_apac.csv /workplace/yzhao/apac_fastrack.csv 1
 *              java -jar Backfill-jar-with-dependencies.jar eng759_backfill_apac /workplace/yzhao/eng759_backfill_apac.csv /workplace/yzhao/apac_fastrack.csv 2
 *              java -jar Backfill-jar-with-dependencies.jar eng759_backfill_apac /workplace/yzhao/eng759_backfill_apac.csv /workplace/yzhao/apac_fastrack.csv 9
 */
public class BackfillMain {
    private static final String DEFAULT_FILE_PATH = "/workplace/yzhao/";
    /**
     * @param argv
     */
    public static void main(String[] argv) {
        String table = null;

        String partition = null;
        try {
             table = argv[0];
            if(argv.length == 2) {
                partition = argv[1];
            }

            if(table == null){
                System.out.println("argument 1 is missing for table name");
                return;
            }

            String csvFileOutputPath = DEFAULT_FILE_PATH + table + "_csvFileOutputPath.csv";
            String fastrackFileOutputPath = DEFAULT_FILE_PATH;

            if(partition == null){
                int i = 0;
                while(i < 10){
                    NetezzaConnector.dataToCsv(table, csvFileOutputPath, String.valueOf(i));
                    System.out.println("done with ekv raws to CSV file \n");
                    String currentDate = DateUtil.getCurrentDate();
                    String timeStamp = String.valueOf(DateUtil.getCurrentTimeInUnixTimestamp());
                    /**
                     * hostName has startwith properties:
                     *  hdu.include.only.sources=localhost,dmining,modata,ps,ag,bidder,udcuweb,qa1-ps1,qa-yoweb1,qa2-ps1,qa2-yoweb1,qa4-ps1,qa4-yoweb1,qa-ag1,qa2-ag1,qa4-ag1,qa-bidder,qa2-bidder,qa4-bidder,qa-googlebidder,qa-googlebid,qa2-googlebid,qa4-googlebid,qa1-modata1,qa2-modata1,qa4-modata1
                     */
                    String hostName = "localhost";
                    FastrackFileProcessor.execute(csvFileOutputPath, fastrackFileOutputPath + currentDate + "-00000" + i + "." + hostName + "." + timeStamp + "000" + ".csv");
                    System.out.println("done with CSV file to fastrack file\n");
                    File f = new File(csvFileOutputPath);
                    if(FileDeleteUtil.deleteFile(f) == 1){
                        System.out.println(csvFileOutputPath + " has deleted" + "\n");
                    }else{
                        System.out.println(csvFileOutputPath + " has failed to delete" + "\n");
                    }

                    i++;
                }
            }else{
                NetezzaConnector.dataToCsv(table, csvFileOutputPath, partition);
                System.out.println("done with ekv raws to CSV file \n");
                FastrackFileProcessor.execute(csvFileOutputPath, fastrackFileOutputPath );
                System.out.println("done with CSV file to fastrack file\n");
            }


        } catch (Exception e) {
            System.out.println("Exception in Main:" + "\n");
            e.printStackTrace();
        }


    }



}
