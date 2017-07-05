package com.opinmind.ekvsummarybackfill.Servlets;

import com.opinmind.ekvsummarybackfill.Processors.BackfillProcessor;
import com.opinmind.ekvsummarybackfill.Util.Constants;
import com.opinmind.ekvsummarybackfill.Util.EmailUtils.SendEmail;
import org.apache.log4j.Logger;
import org.springframework.web.HttpRequestHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;

/**
 * mode=testing_backfill
 * curl "http://localhost:8080/ekvsummarybackfill/backfill?mode=testing_backfill&option=d&table=ENG759_BACKFILL_PRICELINE&startDate=2016-04&endDate=2017-03"
 * curl "http://localhost:8080/ekvsummarybackfill/backfill?mode=testing_backfill&option=d&table=eng759_backfill_apac&startDate=2016-12&endDate=2017-03"
 *
 */
/**
 * mode=backfill
 * curl "http://localhost:8080/ekvsummarybackfill/backfill?mode=backfill&option=d&table=eng759_backfill_apac&startDate=2016-12&endDate=2017-03"
 * curl "http://localhost:8080/ekvsummarybackfill/backfill?mode=backfill&option=d&table=ENG759_BACKFILL_PRICELINE&startDate=2016-04&endDate=2017-03"
 */
/**
 * mode=dump_ekvraw
 * curl "http://localhost:8080/ekvsummarybackfill/backfill?mode=dump_ekvraw&option=d&table=eng759_backfill_apac&startDate=2016-12&endDate=2017-03"
 */


/**
 * @author YI ZHAO
 */
public class BackfillServlet implements HttpRequestHandler {
    private static final Logger log = Logger.getLogger(BackfillServlet.class);
    private BackfillProcessor backfillProcessor;
    private SendEmail sendEmail = new SendEmail();

    public void handleRequest(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        String mode = req.getParameter("mode"); // convert is convert google cloud files to Netezza file, dump is dump the ekvraw and consolited them by event_id
        // for mode = testing_backfill or backfill or dump_ekvraw or ekvrawToFastrack or convert
        String option = null; // r is partition by reminder, d is partition by date
        String table = null;
        String startDate = null;
        String endDate = null;
        String partition = null;


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

                log.info("[BackfillServlet.handleRequest] is going to execute backfillProcessor.execute");
                backfillProcessor.execute(mode, option, table, startDate, endDate, partition, sendEmail);
                sendEmail.send(table, null, null, null, true, false);
            }
        } catch (Exception e) {
            log.error("[BackfillServlet.handleRequest]: ", e);
        }

    }

    public void setBackfillProcessor(BackfillProcessor backfillProcessor) {
        this.backfillProcessor = backfillProcessor;
    }

    public void init() throws ServletException {
        log.info("[BackfillServlet.init]");
    }

    public void destroy() throws ServletException {
        log.info("[BackfillServlet.destroy]");
    }
}
