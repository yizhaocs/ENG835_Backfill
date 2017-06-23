package com.yizhao.apps.Generator;

import com.yizhao.apps.Model.FastrackFileDao;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 * Create the fastrack clog from FastrackFileDao Class
 *
 * @author YI ZHAO
 */
public class FastrackFileGenerator {
    private static final Logger log = Logger.getLogger(FastrackFileGenerator.class);

    /**
     * ckvraw|timestamp(seconds)|cookie_id|key1=value1&key2=value2&...keyN=valueN|event_id|dp_id|dp_user_id|location_id|referer_url|domain|user_agent
     *
     * @param eventIdToData
     * @return
     */
    @Deprecated
    public void execute(Map<String, FastrackFileDao> eventIdToData, String fastrackFileOutputPath) {
        FileWriter out = null;
        try {
            // When you're working with FileWriter you don't have to create the file first,
            // you can simply start writing to it.
            out = new FileWriter(fastrackFileOutputPath);
            for (String event_id : eventIdToData.keySet()) {
                FastrackFileDao mFastrackFileDao = eventIdToData.get(event_id);
                out.write(toCKVRAW(mFastrackFileDao));
                out.write("\n");
            }

        } catch (FileNotFoundException e) {
            log.error("Caught FileNotFoundException: " + e.getMessage());
        } catch (IOException e) {
            log.error("Caught IOException: " + e.getMessage());
        } catch (Exception e) {
            log.error("Caught Exception: " + e.getMessage());
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException e) {
                log.error("Caught IOException: " + e.getMessage());
            }
        }
    }

    private String toCKVRAW(FastrackFileDao mFastrackFileDao) {
        return "ckvraw" + "|" + mFastrackFileDao.getModification_ts() + "|" + mFastrackFileDao.getCookie_id() + "|" + mFastrackFileDao.getKvPair() + "|" + mFastrackFileDao.getEvent_id() + "|" + mFastrackFileDao.getDp_id() + "|" + "null" + "|" + mFastrackFileDao.getLocation_id() + "|" + "null" + "|" + "null" + "|" + "null";

    }
}
