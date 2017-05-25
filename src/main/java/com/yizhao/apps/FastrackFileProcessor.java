package com.yizhao.apps;

import org.apache.commons.lang3.text.StrTokenizer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;


/**
 * Merges EKV raws, then converts and writes to the fastrack file.
 *
 *
 * EKV raw format:
 *      event_id|key_id|value|cookie_id|dp_id|location_id|modification_ts
 *      3077146869351|16491|2120|305402241992|2120|1522861|2017-02-09 10:25:38
 *      3077146869351|17647|roomguru|305402241992|2120|1522861|2017-02-09 10:25:38
 *      3077146869351|17398|hm|305402241992|2120|1522861|2017-02-09 10:25:38
 *
 *
 * fastrack file format:
 *      ckvraw|timestamp(seconds)|cookie_id|key1=value1&key2=value2&...keyN=valueN|event_id|dp_id|dp_user_id|location_id|referer_url|domain|user_agent
 *      ckvraw|1486664738|305402241992|16491=2120&17647=roomguru&17398=hm|3077146869351|2120|null|1522861|null|null|null
 *
 * @author YI ZHAO
 */
public class FastrackFileProcessor {
    private static final StrTokenizer st = StrTokenizer.getCSVInstance();
    private static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    static {
        st.setDelimiterChar('|');
    }



    public static void execute(String inFilePath, String fastrackFileOutputPath) {
        Map<String, FastrackFileDao> eventIdToData = new HashMap<String, FastrackFileDao>();
        int rowCount = 0;
        String preEventId = null;
        String curEventId = null;
        FileWriter out = null;
        Scanner s = null;
        try {
            out = new FileWriter(fastrackFileOutputPath);
            s = new Scanner(new File(inFilePath));
            while (s.hasNextLine()) {
                String line = s.nextLine();
                String[] str = st.reset(line).getTokenArray();
                if (str.length != 7) {
                    System.out.println("str.length != 7 at the line:" + line);
                    continue;
                }
                String event_id = str[0];
                String key_id = str[1];
                String value = str[2];
                String cookie_id = str[3];
                String dp_id = str[4];
                String location_id = str[5];
                String modification_ts = str[6];
                // 2017-03-15 23:04:35

                Date date = null;
                try{
                    date = dateFormat.parse(modification_ts);
                }catch(Exception e){
                        // for some cases, the seconds[ss] is missing, so we take care inside the catch block
                        DateFormat dateFormatTmp = new SimpleDateFormat("yyyy-MM-dd hh:mm");
                    try{
                        date = dateFormatTmp.parse(modification_ts);
                    }catch(Exception e2){
                        System.out.println("error data format event_id:" + event_id + " ,modification_ts:" + modification_ts + "\n");
                        System.out.println("Exception in FastrackFileProcessor:" + "\n");
                        e2.printStackTrace();
                    }
                }
                long modification_ts_unixTime = date.getTime()/1000;
                // true if read the first row of the file
                if(preEventId == null && curEventId == null){
                    preEventId = event_id;
                    curEventId = event_id;
                }else{
                    curEventId = event_id;
                }

                String kvPair = key_id + "=" + value;
                if (eventIdToData.containsKey(event_id)) {
                    FastrackFileDao tmpFastrackFileDao = eventIdToData.get(event_id);
                    tmpFastrackFileDao.setKvPair(tmpFastrackFileDao.getKvPair() + "&" + kvPair);
                } else {
                    FastrackFileDao curFastrackFileDao = new FastrackFileDao(event_id, kvPair, cookie_id, dp_id, location_id, String.valueOf(modification_ts_unixTime));
                    eventIdToData.put(event_id, curFastrackFileDao);

                    // true if start with new event_id, so we cloging the old one in to fastrack file
                    if(preEventId.equals(curEventId) == false) {
                        FastrackFileDao preFastrackFileDao = eventIdToData.get(preEventId);
                        out.write(toCKVRAW(preFastrackFileDao));
                        out.write("\n");
                        eventIdToData.remove(preEventId); // delete the old data for not to get out of memory exception
                        rowCount++;
                    }
                }
                preEventId = curEventId;
            }

            // cloging the last row data
            FastrackFileDao preFastrackFileDao = eventIdToData.get(preEventId);
            out.write(toCKVRAW(preFastrackFileDao));
            rowCount++;
        } catch (FileNotFoundException e) {
            System.out.println("Exception in FastrackFileProcessor:" + "\n");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("Exception in FastrackFileProcessor:" + "\n");
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println("Exception in FastrackFileProcessor:" + "\n");
            e.printStackTrace();
        } finally {
            System.out.print("Total row generated for fastrack is:" + rowCount + "\n");
            if (s != null) {
                s.close();
            }

            try {
                if (out != null) {
                    out.close();
                }
            }catch (IOException e){
                System.out.println("Caught IOException: " + e.getMessage());
            }
        }
    }

    private static String toCKVRAW(FastrackFileDao mFastrackFileDao){
        return "ckvraw" + "|" + mFastrackFileDao.getModification_ts() + "|" + mFastrackFileDao.getCookie_id() + "|" + mFastrackFileDao.getKvPair() + "|" + mFastrackFileDao.getEvent_id() + "|" + mFastrackFileDao.getDp_id() +  "|" + "null" + "|" + mFastrackFileDao.getLocation_id() +  "|" + "null" +  "|" + "null" +  "|" + "null";
    }
}
