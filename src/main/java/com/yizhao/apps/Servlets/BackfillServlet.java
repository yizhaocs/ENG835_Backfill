package com.yizhao.apps.Servlets;

import org.springframework.web.HttpRequestHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by yzhao on 6/26/17.
 */
public class BackfillServlet implements HttpRequestHandler {
    public void init() throws ServletException {
    }

    public void destroy() throws ServletException {
    }

    public void handleRequest(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        System.out.println("lololol");
    }
}
