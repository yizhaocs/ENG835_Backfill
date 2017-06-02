package com.yizhao.apps;

import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.util.Scanner;


/**
 * {
 * "event_id":1097534875260,
 * "cookie_id":106438361728,
 * "dp_id":2452,
 * "vertical":"hotel",
 * "activity_group":"prospecting",
 * "activity_type":"search",
 * "event_ts":"2016-12-18 14:03:31",
 * "hotel_city":"tokyo",
 * "page":"ot",
 * "location_id":64848
 * }
 *
 * hotel:
 *      event_id,cookie_id,dp_id,vertical,activity_group,activity_type,event_ts,checkin_date,checkout_date,trip_duration,hotel_name,hotel_brand,hotel_city,hotel_state,hotel_country,number_of_rooms,number_of_travelers,currency_type,avg_daily_rate,hotel_code,booked_date,page,user_id,location_id"
 * flight:
 *      event_id,cookie_id,dp_id,vertical,activity_group,activity_type,event_ts,departure_date,return_date,origin_airport,destination_airport,air_carrier,cabin_class,cabin_class_group,currency_type,number_of_travelers,trip_duration,booked_date,airfare,page,user_id,location_id"
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
        String line = null;
        try {
            s = new Scanner(f);
            while (s.hasNextLine()) {
                line = s.nextLine();
                parseJason(line);
            }
        }catch (Exception e){
            System.out.println("failed to parseJason:" + line);
        }finally {
            if (s != null) {
                s.close();
            }
        }
    }


    public static void parseJason(String line){
        JSONObject obj = new JSONObject(line);
        String event_id = obj.get("event_id").toString();
        String cookie_id = obj.get("cookie_id").toString();
        String dp_id = obj.get("dp_id").toString();
        String vertical = obj.get("vertical").toString();
        String activity_group = obj.get("activity_group").toString();
        String activity_type = obj.get("activity_type").toString();
        String event_ts = obj.get("event_ts").toString();
        String hotel_city = obj.get("hotel_city").toString();
        String page = obj.get("page").toString();
        String location_id = obj.get("location_id").toString();

        String result = event_id + "|" + cookie_id + "|";
    }
}
