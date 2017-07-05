package com.opinmind.ekvsumbf.Servlets;

import com.opinmind.ekvsumbf.Processors.ConvertProcessor;
import org.apache.log4j.Logger;
import org.springframework.web.HttpRequestHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * mode=convert
 * curl "http://localhost:8080/ekvsummarybackfill/convert?inputPath=/workplace/yzhao/googleFiles/apac/flightFiles/122016/&outPutPath=/workplace/yzhao/netezzaFiles/apac/flightFiles/122016&monthYear=122016&type=hotel&partition="
 */
/**
 * @author YI ZHAO
 */
public class ConvertServlet implements HttpRequestHandler {
    private static final Logger log = Logger.getLogger(ConvertServlet.class);
    private ConvertProcessor convertProcessor;

    public void handleRequest(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        String inputPath = req.getParameter("inputPath");
        String outPutPath = req.getParameter("outPutPath");
        String monthYear = req.getParameter("monthYear");
        String type = req.getParameter("type");

        log.info("[ConvertServlet.handleRequest] is going to execute convertProcessor.execute");
        try {
            convertProcessor.execute(inputPath, outPutPath + "/ekv_" + type + "_all_netezza-" + monthYear + "_" + type + "_001.csv", type);
        }catch(Exception e){
            log.error("[ConvertServlet.handleRequest]: ", e);
        }
    }

    public ConvertProcessor getConvertProcessor() {
        return convertProcessor;
    }

    public void setConvertProcessor(ConvertProcessor convertProcessor) {
        this.convertProcessor = convertProcessor;
    }

    public void init() throws ServletException {
        log.info("[ConvertServlet.init]");
    }

    public void destroy() throws ServletException {
        log.info("[ConvertServlet.destroy]");
    }
}
