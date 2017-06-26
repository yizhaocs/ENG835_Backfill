package com.yizhao.apps.Servlets;

import com.yizhao.apps.BackfillController;
import org.apache.log4j.Logger;
import org.springframework.web.HttpRequestHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * curl http://localhost:8080/backfill/backfill?mode=123
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
        System.out.println("lololol");
        System.out.println(req.getParameter("mode"));
    }

    public BackfillController getBackfillController() {
        return backfillController;
    }

    public void setBackfillController(BackfillController backfillController) {
        this.backfillController = backfillController;
    }
}
