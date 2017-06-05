package com.yizhao.apps;

import com.yizhao.apps.Util.DateUtil;
import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;


/**
 * google cloud format ekv hotel:
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
 * google cloud format ekv flight:
 * {
 * "event_id":10106649178263,
 * "cookie_id":209256950273,
 * "dp_id":1068,
 * "vertical":"flight",
 * "activity_group":"prospecting",
 * "activity_type":"search",
 * "event_ts":"2017-05-04 11:09:40",
 * "departure_date":"2017-07-05",
 * "return_date":"2017-07-11",
 * "origin_airport":"JFK",
 * "destination_airport":"AGP",
 * "air_carrier":"dl",
 * "cabin_class":"cabin:first/business",
 * "cabin_class_group":"business/first",
 * "number_of_travelers":1,
 * "trip_duration":6,
 * "user_id":"c1073ad1b55c2dc830b1d1f76cd2c848",
 * "location_id":47172
 * }
 *
 * netezza format hotel:
 *      event_id,cookie_id,dp_id,vertical,activity_group,activity_type,event_ts,checkin_date,checkout_date,trip_duration,hotel_name,hotel_brand,hotel_city,hotel_state,hotel_country,number_of_rooms,number_of_travelers,currency_type,avg_daily_rate,hotel_code,booked_date,page,user_id,location_id"
 *
 * netezza format flight:
 *      event_id,cookie_id,dp_id,vertical,activity_group,activity_type,event_ts,departure_date,return_date,origin_airport,destination_airport,air_carrier,cabin_class,cabin_class_group,currency_type,number_of_travelers,trip_duration,booked_date,airfare,page,user_id,location_id"
 */
public class GoogleCloudFileToNetezzaFileConvertor {
    public static void main(String[] args){
        String todayDate = DateUtil.getCurrentDate();
        process("/opt/opinmind/var/google/ekvhotel/error", "/Users/yzhao/Desktop/ekv_hotel_all_netezza-" + todayDate + "_hotel_001.csv", "ekvhotel");
        process("/opt/opinmind/var/google/ekvflight/error", "/Users/yzhao/Desktop/ekv_flight_all_netezza-" + todayDate + "_flight_001.csv", "ekvflight");
    }

    public static void process(String inputDirPath, String outputPath, String type){
        readDir(inputDirPath, outputPath, type);
    }


    /**
     * dirPath = "/path/to/files";
     *
     * @param inputDirPath
     */
    public static void readDir(String inputDirPath, String outputPath, String type){
        File folder = new File(inputDirPath);
        File[] listOfFiles = folder.listFiles();

        for (int i = 0; i < listOfFiles.length; i++) {
            File file = listOfFiles[i];
            if (file.isFile() && file.getName().endsWith(".csv")) {
                readFile(file, outputPath, type);
            }
        }
    }

    public static void readFile(File f, String outputPath, String type){
        FileWriter out = null;
        Scanner s = null;
        String line = null;
        try {
            out = new FileWriter(outputPath);
            s = new Scanner(f);
            while (s.hasNextLine()) {
                line = s.nextLine();
                String netezzaFormatLine = parseJasonAndConvertToNetezzaFormat(line, type);
                out.write(netezzaFormatLine);
                out.write("\n");
            }
        }catch (Exception e){
            System.out.println("failed to parseJason:" + line);
        }finally {
            if (s != null) {
                s.close();
            }
            if(out != null){
                try {
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public static String parseJasonAndConvertToNetezzaFormat(String line, String type){
        String result = null;
        String NULL = "NULL";
        if(type.equals("ekvhotel")) {
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



            // event_id,cookie_id,dp_id,vertical,activity_group,activity_type,event_ts,checkin_date,checkout_date,trip_duration,hotel_name,hotel_brand,hotel_city,hotel_state,hotel_country,number_of_rooms,number_of_travelers,currency_type,avg_daily_rate,hotel_code,booked_date,page,user_id,location_id"
            result =
                    event_id + "|" +
                            cookie_id + "|" +
                            dp_id + "|" +
                            vertical + "|" +
                            activity_group + "|" +
                            activity_type + "|" +
                            event_ts + "|" +
                            NULL + "|" + //checkin_date
                            NULL + "|" + //checkout_date
                            NULL + "|" + //trip_duration
                            NULL + "|" + //hotel_name
                            NULL + "|" + //hotel_brand
                            hotel_city + "|" + //
                            NULL + "|" + // hotel_state
                            NULL + "|" + // hotel_country
                            NULL + "|" + // number_of_rooms
                            NULL + "|" + // number_of_travelers
                            NULL + "|" + // currency_type
                            NULL + "|" + // avg_daily_rate
                            NULL + "|" + // hotel_code
                            NULL + "|" + // booked_date
                            page + "|" + //
                            NULL + "|" + // user_id
                            location_id;

        }else if(type.equals("ekvflight")){
            JSONObject obj = new JSONObject(line);
            String event_id = obj.get("event_id").toString();
            String cookie_id = obj.get("cookie_id").toString();
            String dp_id = obj.get("dp_id").toString();
            String vertical = obj.get("vertical").toString();
            String activity_group = obj.get("activity_group").toString();
            String activity_type = obj.get("activity_type").toString();
            String event_ts = obj.get("event_ts").toString();
            String departure_date = obj.get("departure_date").toString();
            String return_date = obj.get("return_date").toString();
            String origin_airport = obj.get("origin_airport").toString();
            String destination_airport = obj.get("destination_airport").toString();
            String air_carrier = obj.get("air_carrier").toString();
            String cabin_class = obj.get("cabin_class").toString();
            String cabin_class_group = obj.get("cabin_class_group").toString();
            String number_of_travelers = obj.get("number_of_travelers").toString();
            String trip_duration = obj.get("trip_duration").toString();
            String user_id = obj.get("user_id").toString();
            String location_id = obj.get("location_id").toString();



            // event_id,
            // cookie_id,
            // dp_id,
            // vertical,
            // activity_group,
            // activity_type,
            // event_ts,
            // departure_date,
            // return_date,
            // origin_airport,
            // destination_airport,
            // air_carrier,
            // cabin_class,
            // cabin_class_group,
            // currency_type,
            // number_of_travelers,
            // trip_duration,
            // booked_date,
            // airfare,
            // page,
            // user_id,
            // location_id"
            result =
                    event_id + "|" +
                            cookie_id + "|" +
                            dp_id + "|" +
                            vertical + "|" +
                            activity_group + "|" +
                            activity_type + "|" +
                            event_ts + "|" +
                            departure_date + "|" +
                            return_date + "|" +
                            origin_airport + "|" +
                            destination_airport + "|" +
                            air_carrier + "|" +
                            cabin_class + "|" +
                            cabin_class_group + "|" +
                            NULL + "|" + // currency_type
                            number_of_travelers + "|" + // number_of_rooms
                            trip_duration + "|" + // trip_duration
                            NULL + "|" + // booked_date
                            NULL + "|" + // airfare
                            NULL + "|" + // page
                            user_id + "|" +
                            location_id;
        }
        return result;
    }

}
