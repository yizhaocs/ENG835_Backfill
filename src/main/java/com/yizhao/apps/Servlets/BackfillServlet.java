package com.yizhao.apps.Servlets;

import com.yizhao.apps.BackfillController;
import org.apache.log4j.Logger;
import org.springframework.web.HttpRequestHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * curl http://localhost:8080/backfill/backfill?mode=backfill&option=d&table=eng759_backfill_apac&startDate=2016-12&endDate=2017-03
 * curl http://localhost:8080/backfill/backfill?mode=dump_ekvraw&option=d&table=eng759_backfill_apac&startDate=2016-12&endDate=2017-03
 * curl http://localhost:8080/backfill/backfill?mode=convert&inputPath=/workplace/yzhao/googleFiles/apac/flightFiles/122016/&outPutPath=/workplace/yzhao/netezzaFiles/apac/flightFiles/122016&monthYear=122016&type=hotel&partition=
 */
public class BackfillServlet implements HttpRequestHandler {
    private static final Logger log = Logger.getLogger(BackfillServlet.class);
    private BackfillController backfillController;

    public void init() throws ServletException {
        log.info("[BackfillServlet.init]");
    }

    public void destroy() throws ServletException {
        log.info("[BackfillServlet.destroy]");
    }


    public void handleRequest(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        String mode = req.getParameter("mode");
        // for mode = backfill or dump
        String option = null;
        String table = null;
        String startDate = null;
        String endDate = null;
        // for mode = dump
        String partition = null;
        // for mode = convert
        String inputPath = null;
        String outPutPath = null;
        String monthYear = null;
        String type = null;

        if (mode == null) {
            return;
        }

        try {
            if (mode.equals("backfill")) {
                log.info("[BackfillServlet.handleRequest] is going to execute runModeBackfill" + "\n");
                backfillController.runModeBackfill(option, table, startDate, endDate);
            }else if (mode.equals("dump_ekvraw")) {
                log.info("[BackfillServlet.handleRequest] is going to execute runModeDumpEKVraw" + "\n");
                backfillController.runModeDumpEKVraw(option, table, startDate, endDate, partition);
            }else if (mode.equals("convert")) {
                log.info("[BackfillServlet.handleRequest] is going to execute runModeConvert" + "\n");
                backfillController.runModeConvert(inputPath,outPutPath,monthYear,type);
            }
        } catch (Exception e) {
            log.error("[BackfillServlet.handleRequest]:" + "\n");
            e.printStackTrace();
        }

        System.out.println("lololol");
        System.out.println(req.getParameter("mode"));
        System.out.println(req.getParameter("inputPath"));
    }

    public BackfillController getBackfillController() {
        return backfillController;
    }

    public void setBackfillController(BackfillController backfillController) {
        this.backfillController = backfillController;
    }
}
