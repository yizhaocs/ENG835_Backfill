package com.opinmind.Util.DateAndTimeUtils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Reference: http://www.mkyong.com/java/java-date-and-calendar-examples/
 */
public class DateCalendar {
    public static void main(String[] args) {
        System.out.println(getUnixTimeStamp()); // 1498585337
        System.out.println(convertDateToString()); // 27/6/2017
        System.out.println(convertStringToDate()); // Tue Aug 31 10:20:56 PDT 1982
        System.out.println(getCurrentDateTime()); // 2017/06/27 10:42:17
    }

    /**
     * 10 digits
     *
     * @return
     */
    public static String getUnixTimeStamp(){
        return String.valueOf(System.currentTimeMillis()/1000);

    }

    public static String convertDateToString() {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/M/yyyy");
        String date = sdf.format(new Date());
        return date; //15/10/2013
    }

    public static String convertStringToDate() {
        SimpleDateFormat sdf = new SimpleDateFormat("dd-M-yyyy hh:mm:ss");
        String dateInString = "31-08-1982 10:20:56";
        Date date = null;
        try {
            date = sdf.parse(dateInString);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return date.toString(); //Tue Aug 31 10:20:56 SGT 1982
    }

    public static String getCurrentDateTime() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        return dateFormat.format(date);
    }
}
