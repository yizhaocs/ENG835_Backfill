package com.opinmind.ekvsummarybackfill.Processors;

import com.opinmind.ekvsummarybackfill.Converter.EkvrawToFastrackFileConvertor;
import com.opinmind.ekvsummarybackfill.Util.Constants;
import com.opinmind.ekvsummarybackfill.Util.DateAndTimeUtils.DateCalendar;
import com.opinmind.ekvsummarybackfill.Util.DateUtil;
import com.opinmind.ekvsummarybackfill.Util.FileUtils.FileDeleteUtil;
import org.apache.log4j.Logger;

import java.io.File;
import java.net.InetAddress;

/**
 * @author YI ZHAO
 */
public class EkvrawToFastrackProcessor {
    private final Logger log = Logger.getLogger(EkvrawToFastrackProcessor.class);
    private EkvrawToFastrackFileConvertor ekvrawToFastrackFileConvertor;
    public void execute(boolean deleteEKVRAW) throws Exception {
        /**
         * hostName has startwith properties:
         *  hdu.include.only.sources=localhost,dmining,modata,ps,ag,bidder,udcuweb,qa1-ps1,qa-yoweb1,qa2-ps1,qa2-yoweb1,qa4-ps1,qa4-yoweb1,qa-ag1,qa2-ag1,qa4-ag1,qa-bidder,qa2-bidder,qa4-bidder,qa-googlebidder,qa-googlebid,qa2-googlebid,qa4-googlebid,qa1-modata1,qa2-modata1,qa4-modata1
         */
        String CurrentHostName = InetAddress.getLocalHost().getHostName();
        String fileHostName = null;
        // hdu.include.only.sources in common.properties
        if (CurrentHostName.contains("qa") || CurrentHostName.contains("manager")) {
            fileHostName = "qa1-ps1-lax1";
        } else {
            fileHostName = "ps";
        }


        String currentDate = DateUtil.getCurrentDate("yyyyMMdd");
        String timeStamp = DateCalendar.getUnixTimeStamp();
        ekvrawToFastrackFileConvertor.execute(Constants.Files.EKVRAW_FILE_PATH, Constants.Files.fastrackFileOutputPath + currentDate + "-000000" + "." + fileHostName + "." + timeStamp + "000" + ".csv.force");
        log.info("done with CSV file to fastrack file\n");

        if(deleteEKVRAW) {
            File f = new File(Constants.Files.EKVRAW_FILE_PATH);
            if(f.exists()) {
                if (FileDeleteUtil.deleteFile(f) == 1) {
                    log.info(Constants.Files.EKVRAW_FILE_PATH + " has deleted");
                } else {
                    log.info(Constants.Files.EKVRAW_FILE_PATH + " has failed to delete");
                }
            }else{
                log.info(Constants.Files.EKVRAW_FILE_PATH + " does not exist");
            }
        }
    }

    public EkvrawToFastrackFileConvertor getEkvrawToFastrackFileConvertor() {
        return ekvrawToFastrackFileConvertor;
    }

    public void setEkvrawToFastrackFileConvertor(EkvrawToFastrackFileConvertor ekvrawToFastrackFileConvertor) {
        this.ekvrawToFastrackFileConvertor = ekvrawToFastrackFileConvertor;
    }

    public void init() {}

    public void destroy() {}
}
