package com.yizhao.apps;

import org.json.JSONObject;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Scanner;

/**
 * Created by yzhao on 6/5/17.
 */
public class BigQueryGenerator {
    public static void main(String[] args){
        generate("/Users/yzhao/Desktop/20170602160034nDWBzwnegroEJb0.20170602160034.1496444434946.Yi-Zhao-local-lax1.concat.csv", "/Users/yzhao/Desktop/big_query_generator.csv");
    }

    public static void generate(String fileInput, String fileOutput){
        StringBuilder query = new StringBuilder();
        query.append("select * from impact_prod.ekv_hotel_20161201 where event_id in (");
        // Location of file to read
        File file = new File(fileInput);
        File fout = new File(fileOutput);
        Scanner scanner = null;
        FileOutputStream fos = null;
        BufferedWriter bw = null;
        try {
            scanner = new Scanner(file);
            fos = new FileOutputStream(fout);
            bw = new BufferedWriter(new OutputStreamWriter(fos));
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                JSONObject obj = new JSONObject(line);
                String event_id = obj.get("event_id").toString();
                query.append(event_id);
                if(scanner.hasNextLine()) {
                    query.append(",");
                }
            }
            query.append(");");
            bw.write(query.toString());

        } catch (FileNotFoundException e) {
            e.printStackTrace();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            scanner.close();
            try {
                bw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
