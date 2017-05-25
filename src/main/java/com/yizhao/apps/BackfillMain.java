package com.yizhao.apps;


import com.yizhao.apps.Connector.NetezzaConnector;
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
            String fastrackFileOutputPath = DEFAULT_FILE_PATH + table + "_fastrackFileOutputPath";

            if(partition == null){
                int i = 0;
                while(i < 10){
                    NetezzaConnector.dataToCsv(table, csvFileOutputPath, String.valueOf(i));
                    System.out.println("done with ekv raws to CSV file \n");
                    FastrackFileProcessor.execute(csvFileOutputPath, fastrackFileOutputPath + "_" + i + ".csv");
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
