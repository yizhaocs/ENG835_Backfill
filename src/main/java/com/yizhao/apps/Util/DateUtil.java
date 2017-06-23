package com.yizhao.apps.Util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by yzhao on 5/30/17.
 */
public class DateUtil {
    public static Long dateToUnixTime(Date date) {
        if (date == null) {
            return null;
        }
        return date.getTime() / 1000;
    }

    public static String getCurrentDate(String format) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        Date date = new Date();
        return dateFormat.format(date).toString();
    }

    public static long getCurrentTimeInUnixTimestamp() {
        Date d = new Date();
        return d.getTime() / 1000;
    }
}
