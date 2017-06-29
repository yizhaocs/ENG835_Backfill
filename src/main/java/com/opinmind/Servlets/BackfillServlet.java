package com.opinmind.Servlets;

import com.opinmind.BackfillController;
import org.apache.log4j.Logger;
import org.springframework.web.HttpRequestHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;

/**
 * curl "http://localhost:8080/backfill/run?mode=backfill&option=d&table=ENG759_BACKFILL_PRICELINE&startDate=2016-04&endDate=2016-05"
 * curl "http://localhost:8080/backfill/run?mode=backfill&option=d&table=eng759_backfill_apac&startDate=2016-12&endDate=2017-01"
 * curl "http://localhost:8080/backfill/run?mode=dump_ekvraw&option=d&table=eng759_backfill_apac&startDate=2016-12&endDate=2017-03"
 * curl "http://localhost:8080/backfill/run?mode=convert&inputPath=/workplace/yzhao/googleFiles/apac/flightFiles/122016/&outPutPath=/workplace/yzhao/netezzaFiles/apac/flightFiles/122016&monthYear=122016&type=hotel&partition="
 * testing:
 * curl "http://localhost:8080/backfill/run?mode=backfill&option=d&table=ENG759_BACKFILL_Testing&startDate=2016-12&endDate=2017-02"
 *
 *
 * curl "http://localhost:8080/backfill/run?mode=backfill&option=d&table=ENG759_BACKFILL_Testing&startDate=2016-12&endDate=2017-02"


 CREATE TABLE ENG759_BACKFILL_Testing AS(
 SELECT EKV.EVENT_ID, EKV.KEY_ID, EKV.VALUE, EKV.COOKIE_ID, EKV.DP_ID, EKV.LOCATION_ID, EKV.MODIFICATION_TS
 FROM ENG759_BACKFILL_APAC AS EKV
 WHERE  MODIFICATION_TS = '2016-12-19'
 ORDER BY EKV.EVENT_ID
 LIMIT 2000);

 INSERT INTO ENG759_BACKFILL_Testing
 SELECT * FROM ENG759_BACKFILL_APAC
 WHERE  MODIFICATION_TS = '2017-01-19'
 ORDER BY EVENT_ID
 LIMIT 2000;

 INSERT INTO ENG759_BACKFILL_Testing
 SELECT * FROM ENG759_BACKFILL_APAC
 WHERE  MODIFICATION_TS = '2017-02-19'
 ORDER BY EVENT_ID
 LIMIT 2000;
 */
public class BackfillServlet implements HttpRequestHandler {
    private static final Logger log = Logger.getLogger(BackfillServlet.class);
    private BackfillController backfillController;

    public void handleRequest(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        String mode = req.getParameter("mode"); // convert is convert google cloud files to Netezza file, dump is dump the ekvraw and consolited them by event_id
        // for mode = backfill or dump
        String option = null; // r is partition by reminder, d is partition by date
        String table = null;
        String startDate = null;
        String endDate = null;
        String partition = null;
        // for mode = convert
        String inputPath = null;
        String outPutPath = null;
        String monthYear = null;
        String type = null; // hotel or flight

        log.info("[BackfillServlet.handleRequest] getParameterMap:" + "\n");
        for(String s: req.getParameterMap().keySet()){
            String[] value = req.getParameterMap().get(s);
            log.info(s + "=" + Arrays.toString(value) + "\n");
        }

        if (mode == null) {
            log.error("[BackfillServlet.handleRequest] mode is null " + "\n");
            return;
        }

        try {
            if (mode.equals("backfill") || mode.equals("dump_ekvraw")) {
                option = req.getParameter("option");
                table = req.getParameter("table");
                startDate = req.getParameter("startDate");
                endDate = req.getParameter("endDate");
                if (option.equals("r")) {
                    partition = req.getParameter("partition");
                }

                if (mode.equals("backfill") || mode.equals("dump_ekvraw")) {
                    log.info("[BackfillServlet.handleRequest] is going to execute runModeBackfillOrDumpEKVraw" + "\n");
                    backfillController.runModeBackfillOrDumpEKVraw(mode, option, table, startDate, endDate, partition);
                }
            } else if (mode.equals("convert")) {
                inputPath = req.getParameter("inputPath");
                outPutPath = req.getParameter("outPutPath");
                monthYear = req.getParameter("monthYear");
                type = req.getParameter("type");

                log.info("[BackfillServlet.handleRequest] is going to execute runModeConvert" + "\n");
                backfillController.runModeConvert(inputPath, outPutPath + "/ekv_" + type + "_all_netezza-" + monthYear + "_" + type + "_001.csv", type);
            }
        } catch (Exception e) {
            log.error("[BackfillServlet.handleRequest]: ", e);
        }
    }

    public void setBackfillController(BackfillController backfillController) {
        this.backfillController = backfillController;
    }

    public void init() throws ServletException {
        log.info("[BackfillServlet.init]");
    }

    public void destroy() throws ServletException {
        log.info("[BackfillServlet.destroy]");
    }
}
