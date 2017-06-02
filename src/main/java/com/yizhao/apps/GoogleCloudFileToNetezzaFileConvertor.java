package com.yizhao.apps;

import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.util.Scanner;


/**
 * {"event_id":1097534875260,"cookie_id":106438361728,"dp_id":2452,"vertical":"hotel","activity_group":"prospecting","activity_type":"search","event_ts":"2016-12-18 14:03:31","hotel_city":"tokyo","page":"ot","location_id":64848}
 */
public class GoogleCloudFileToNetezzaFileConvertor {
    public static void main(String[] args){
        process();
    }

    public static void process(){
        readDir("/opt/opinmind/var/google/ekvhotel/error");
    }


    /**
     * dirPath = "/path/to/files";
     *
     * @param dirPath
     */
    public static void readDir(String dirPath){
        File folder = new File(dirPath);
        File[] listOfFiles = folder.listFiles();

        for (int i = 0; i < listOfFiles.length; i++) {
            File file = listOfFiles[i];
            if (file.isFile() && file.getName().endsWith(".csv")) {
                readFile(file);
            }
        }
    }

    public static void readFile(File f){
        Scanner s = null;
        try {
            s = new Scanner(f);
            while (s.hasNextLine()) {
                String line = s.nextLine();
                parseJason(line);
            }
        }catch (Exception e){

        }finally {
            if (s != null) {
                s.close();
            }
        }
    }


    public static void parseJason(String jsonObject){
        JSONObject obj = new JSONObject(jsonObject);
       // JSONObject event_id = obj.getString("event_id");
        System.out.println(obj.get("event_id"));

    }
}
