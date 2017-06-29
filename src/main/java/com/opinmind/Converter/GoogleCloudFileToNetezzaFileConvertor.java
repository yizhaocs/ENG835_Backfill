package com.opinmind.Converter;

import com.opinmind.Util.DateUtil;
import org.apache.log4j.Logger;
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
 * <p>
 * google cloud format ekv flight:
 * {
 * "event_id":1077305980653,
 * "cookie_id":103513169200,
 * "dp_id":1449,
 * "vertical":"flight",
 * "activity_group":"prospecting",
 * "activity_type":"search",
 * "event_ts":"2016-02-29 16:00:01",
 * "departure_date":"2016-05-11",
 * "return_date":"2016-05-19",
 * "origin_airport":"LAX",
 * "destination_airport":"KRL",
 * "number_of_travelers":1,
 * "trip_duration":8,"page":"asp",
 * "location_id":70802}
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
 * <p>
 * netezza format hotel:
 * event_id,cookie_id,dp_id,vertical,activity_group,activity_type,event_ts,checkin_date,checkout_date,trip_duration,hotel_name,hotel_brand,hotel_city,hotel_state,hotel_country,number_of_rooms,number_of_travelers,currency_type,avg_daily_rate,hotel_code,booked_date,page,user_id,location_id"
 * <p>
 * netezza format flight:
 * event_id,cookie_id,dp_id,vertical,activity_group,activity_type,event_ts,departure_date,return_date,origin_airport,destination_airport,air_carrier,cabin_class,cabin_class_group,currency_type,number_of_travelers,trip_duration,booked_date,airfare,page,user_id,location_id"
 */
public class GoogleCloudFileToNetezzaFileConvertor {
    private static final Logger log = Logger.getLogger(GoogleCloudFileToNetezzaFileConvertor.class);

    public void main(String[] args) {
        String todayDate = DateUtil.getCurrentDate("yyyyMMdd");
        process("/opt/opinmind/var/google/ekvhotel/concat", "/home/yzhao/ENG835/googleToNetezzaFiles/ekv_hotel_all_netezza-" + todayDate + "_hotel_001.csv", "hotel");
        process("/opt/opinmind/var/google/ekvflight/concat", "/home/yzhao/ENG835/googleToNetezzaFiles/ekv_flight_all_netezza-" + todayDate + "_flight_001.csv", "flight");
    }

    public void process(String inputDirPath, String outputPath, String type) {
        readDirAndWriteToOutput(inputDirPath, outputPath, type);
    }


    /**
     * dirPath = "/path/to/files";
     *
     * @param inputDirPath
     */
    public void readDirAndWriteToOutput(String inputDirPath, String outputPath, String type) {
        File folder = new File(inputDirPath);
        File[] listOfFiles = folder.listFiles();
        FileWriter out = null;
        try {
            out = new FileWriter(outputPath);
            log.info("GoogleCloudFileToNetezzaFileConvertor.readDirAndWriteToOutput.listOfFiles.length:" + listOfFiles.length);
            for (int i = 0; i < listOfFiles.length; i++) {
                File file = listOfFiles[i];
                log.info("begin of converting file:" + file.getName());
                if (file.isFile() && file.getName().endsWith(".csv")) {
                    int count = readFile(out, file, type);
                    log.info("end of converting file with total converted count:" + count);
                }
            }
        } catch (Exception e) {
            log.error("[GoogleCloudFileToNetezzaFileConvertor.readDirAndWriteToOutput]: ", e);
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public int readFile(FileWriter out, File f, String type) {
        int count = 0;

        Scanner s = null;
        String line = null;
        try {
            s = new Scanner(f);
            while (s.hasNextLine()) {
                line = s.nextLine();
                String netezzaFormatLine = parseJasonAndConvertToNetezzaFormat(line, type);
                out.write(netezzaFormatLine);
                out.write("\n");
                count++;
            }
        } catch (Exception e) {
            log.info("[GoogleCloudFileToNetezzaFileConvertor.readFile] failed to parseJason:" + line);
        } finally {
            if (s != null) {
                s.close();
            }
        }
        return count;
    }


    public String parseJasonAndConvertToNetezzaFormat(String line, String type) {
        String result = null;
        String NULL = "";
        String dw_modification_ts = DateUtil.getCurrentDate("yyyy-MM-dd HH:mm:ss");
        if (type.equals("hotel")) {
            JSONObject obj = new JSONObject(line);
            String event_id = obj.isNull("event_id") ? NULL : obj.get("event_id").toString();
            String cookie_id = obj.isNull("cookie_id") ? NULL : obj.get("cookie_id").toString();
            String dp_id = obj.isNull("dp_id") ? NULL : obj.get("dp_id").toString();
            String vertical = obj.isNull("vertical") ? NULL : obj.get("vertical").toString();
            String activity_group = obj.isNull("activity_group") ? NULL : obj.get("activity_group").toString();
            String activity_type = obj.isNull("activity_type") ? NULL : obj.get("activity_type").toString();
            String event_ts = obj.isNull("event_ts") ? NULL : obj.get("event_ts").toString();
            String checkin_date = obj.isNull("checkin_date") ? NULL : obj.get("checkin_date").toString();
            String checkout_date = obj.isNull("checkout_date") ? NULL : obj.get("checkout_date").toString();
            String trip_duration = obj.isNull("trip_duration") ? NULL : obj.get("trip_duration").toString();
            String hotel_name = obj.isNull("hotel_name") ? NULL : obj.get("hotel_name").toString();
            String hotel_brand = obj.isNull("hotel_brand") ? NULL : obj.get("hotel_brand").toString();
            String hotel_city = obj.isNull("hotel_city") ? NULL : obj.get("hotel_city").toString();
            String hotel_state = obj.isNull("hotel_state") ? NULL : obj.get("hotel_state").toString();
            String hotel_country = obj.isNull("hotel_country") ? NULL : obj.get("hotel_country").toString();
            String number_of_rooms = obj.isNull("number_of_rooms") ? NULL : obj.get("number_of_rooms").toString();
            String number_of_travelers = obj.isNull("number_of_travelers") ? NULL : obj.get("number_of_travelers").toString();
            String currency_type = obj.isNull("currency_type") ? NULL : obj.get("currency_type").toString();
            String avg_daily_rate = obj.isNull("avg_daily_rate") ? NULL : obj.get("avg_daily_rate").toString();
            String hotel_code = obj.isNull("hotel_code") ? NULL : obj.get("hotel_code").toString();
            String booked_date = obj.isNull("booked_date") ? NULL : obj.get("booked_date").toString();
            String page = obj.isNull("page") ? NULL : obj.get("page").toString();
            String user_id = obj.isNull("user_id") ? NULL : obj.get("user_id").toString();
            String location_id = obj.isNull("location_id") ? NULL : obj.get("location_id").toString();


            // event_id,
            // cookie_id,
            // dp_id,
            // vertical,
            // activity_group,
            // activity_type,
            // event_ts,
            // checkin_date,
            // checkout_date,
            // trip_duration,
            // hotel_name,
            // hotel_brand,
            // hotel_city,
            // hotel_state,
            // hotel_country,
            // number_of_rooms,
            // number_of_travelers,
            // currency_type,
            // avg_daily_rate,
            // hotel_code,
            // booked_date,
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
                            checkin_date + "|" + //checkin_date
                            checkout_date + "|" + //checkout_date
                            trip_duration + "|" + //trip_duration
                            hotel_name + "|" + //hotel_name
                            hotel_brand + "|" + //hotel_brand
                            hotel_city + "|" + //
                            hotel_state + "|" + // hotel_state
                            hotel_country + "|" + // hotel_country
                            number_of_rooms + "|" + // number_of_rooms
                            number_of_travelers + "|" + // number_of_travelers
                            currency_type + "|" + // currency_type
                            avg_daily_rate + "|" + // avg_daily_rate
                            hotel_code + "|" + // hotel_code
                            booked_date + "|" + // booked_date
                            page + "|" + //
                            user_id + "|" + // user_id
                            location_id + "|" +
                            dw_modification_ts;

        } else if (type.equals("flight")) {
            JSONObject obj = new JSONObject(line);
            String event_id = obj.isNull("event_id") ? NULL : obj.get("event_id").toString();
            String cookie_id = obj.isNull("cookie_id") ? NULL : obj.get("cookie_id").toString();
            String dp_id = obj.isNull("dp_id") ? NULL : obj.get("dp_id").toString();
            String vertical = obj.isNull("vertical") ? NULL : obj.get("vertical").toString();
            String activity_group = obj.isNull("activity_group") ? NULL : obj.get("activity_group").toString();
            String activity_type = obj.isNull("activity_type") ? NULL : obj.get("activity_type").toString();
            String event_ts = obj.isNull("event_ts") ? NULL : obj.get("event_ts").toString();
            String departure_date = obj.isNull("departure_date") ? NULL : obj.get("departure_date").toString();
            String return_date = obj.isNull("return_date") ? NULL : obj.get("return_date").toString();
            String origin_airport = obj.isNull("origin_airport") ? NULL : obj.get("origin_airport").toString();
            String destination_airport = obj.isNull("destination_airport") ? NULL : obj.get("destination_airport").toString();
            String air_carrier = obj.isNull("air_carrier") ? NULL : obj.get("air_carrier").toString();
            String cabin_class = obj.isNull("cabin_class") ? NULL : obj.get("cabin_class").toString();
            String cabin_class_group = obj.isNull("cabin_class_group") ? NULL : obj.get("cabin_class_group").toString();
            String currency_type = obj.isNull("currency_type") ? NULL : obj.get("currency_type").toString();
            String number_of_travelers = obj.isNull("number_of_travelers") ? NULL : obj.get("number_of_travelers").toString();
            String trip_duration = obj.isNull("trip_duration") ? NULL : obj.get("trip_duration").toString();
            String booked_date = obj.isNull("booked_date") ? NULL : obj.get("booked_date").toString();
            String airfare = obj.isNull("airfare") ? NULL : obj.get("airfare").toString();
            String page = obj.isNull("page") ? NULL : obj.get("page").toString();
            String user_id = obj.isNull("user_id") ? NULL : obj.get("user_id").toString();
            String location_id = obj.isNull("location_id") ? NULL : obj.get("location_id").toString();


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
                            currency_type + "|" +
                            number_of_travelers + "|" +
                            trip_duration + "|" +
                            booked_date + "|" +
                            airfare + "|" +
                            page + "|" +
                            user_id + "|" +
                            location_id + "|" +
                            dw_modification_ts;
        }
        return result;
    }

    public void init() {

    }


    public void destroy() {

    }
}
