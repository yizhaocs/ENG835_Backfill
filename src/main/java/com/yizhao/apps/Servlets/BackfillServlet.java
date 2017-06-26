package com.yizhao.apps.Servlets;

import com.yizhao.apps.BackfillController;
import org.springframework.web.HttpRequestHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * curl http://localhost:8080/backfill
 */
public class BackfillServlet implements HttpRequestHandler {
    private BackfillController backfillController;

    public void init() throws ServletException {
        System.out.println("zhao yi");
    }

    public void destroy() throws ServletException {
    }

    public void handleRequest(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {


        System.out.println("lololol");
    }

    public BackfillController getBackfillController() {
        return backfillController;
    }

    public void setBackfillController(BackfillController backfillController) {
        this.backfillController = backfillController;
    }
}
