package com.opinmind.Servlets;

import com.opinmind.BackfillController;
import com.opinmind.Util.Constants;
import org.apache.log4j.Logger;
import org.springframework.web.HttpRequestHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;

/**
 * mode=testing_backfill
 * curl "http://localhost:8080/backfill/run?mode=testing_backfill&option=d&table=eng759_backfill_apac&startDate=2016-12&endDate=2017-03"
 * curl "http://localhost:8080/backfill/run?mode=testing_backfill&option=d&table=ENG759_BACKFILL_PRICELINE&startDate=2016-04&endDate=2017-03"
 */
/**
 * mode=backfill
 * curl "http://localhost:8080/backfill/run?mode=backfill&option=d&table=eng759_backfill_apac&startDate=2016-12&endDate=2017-03"
 * curl "http://localhost:8080/backfill/run?mode=backfill&option=d&table=ENG759_BACKFILL_PRICELINE&startDate=2016-04&endDate=2017-03"
 */
/**
 * mode=dump_ekvraw
 * curl "http://localhost:8080/backfill/run?mode=dump_ekvraw&option=d&table=eng759_backfill_apac&startDate=2016-12&endDate=2017-03"
 */
/**
 * mode=ekvrawToFastrack
 * curl "http://localhost:8080/backfill/run?mode=ekvrawToFastrack&deleteEKVRAW=1"
 * curl "http://localhost:8080/backfill/run?mode=ekvrawToFastrack&deleteEKVRAW=0"
 */
/**
 * mode=convert
 * curl "http://localhost:8080/backfill/run?mode=convert&inputPath=/workplace/yzhao/googleFiles/apac/flightFiles/122016/&outPutPath=/workplace/yzhao/netezzaFiles/apac/flightFiles/122016&monthYear=122016&type=hotel&partition="
 */
/**
 * @author YI ZHAO
 */
public class BackfillServlet implements HttpRequestHandler {
    private static final Logger log = Logger.getLogger(BackfillServlet.class);
    private BackfillController backfillController;

    public void handleRequest(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        String mode = req.getParameter("mode"); // convert is convert google cloud files to Netezza file, dump is dump the ekvraw and consolited them by event_id
        // for mode = testing_backfill or backfill or dump_ekvraw or ekvrawToFastrack or convert
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

        log.info("[BackfillServlet.handleRequest] getParameterMap:");
        for(String s: req.getParameterMap().keySet()){
            String[] value = req.getParameterMap().get(s);
            log.info(s + "=" + Arrays.toString(value));
        }

        if (mode == null) {
            log.error("[BackfillServlet.handleRequest] mode is null ");
            return;
        }

        try {
            if (mode.equals(Constants.Mode.TESTING_BACKFILL) || mode.equals(Constants.Mode.BACKFILL) || mode.equals(Constants.Mode.DUMP_EKVRAW)) {
                option = req.getParameter("option");
                table = req.getParameter("table");
                startDate = req.getParameter("startDate");
                endDate = req.getParameter("endDate");
                if (option.equals("r")) {
                    partition = req.getParameter("partition");
                }

                log.info("[BackfillServlet.handleRequest] is going to execute runModeBackfillOrDumpEKVraw");
                backfillController.runModeBackfillOrDumpEKVraw(mode, option, table, startDate, endDate, partition);
            }else if (mode.equals(Constants.Mode.EKVRAW_TO_FASTRACK)) {
                String deleteEKVRAW = req.getParameter("deleteEKVRAW");
                if(deleteEKVRAW.equals("1")){
                    backfillController.runModeEkvrawToFastrack(true);
                }else{
                    backfillController.runModeEkvrawToFastrack(false);
                }
            } else if (mode.equals(Constants.Mode.CONVERT)) {
                inputPath = req.getParameter("inputPath");
                outPutPath = req.getParameter("outPutPath");
                monthYear = req.getParameter("monthYear");
                type = req.getParameter("type");

                log.info("[BackfillServlet.handleRequest] is going to execute runModeConvert");
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
