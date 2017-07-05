package com.opinmind.ekvsumbf.Servlets;

import com.opinmind.ekvsumbf.Processors.EkvrawToFastrackProcessor;
import org.apache.log4j.Logger;
import org.springframework.web.HttpRequestHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * mode=ekvrawToFastrack
 * curl "http://localhost:8080/ekvsummarybackfill/ekvrawToFastrack?deleteEKVRAW=1"
 * curl "http://localhost:8080/ekvsummarybackfill/ekvrawToFastrack?deleteEKVRAW=0"
 */
/**
 * @author YI ZHAO
 */
public class EkvrawToFastrackServlet implements HttpRequestHandler {
    private static final Logger log = Logger.getLogger(EkvrawToFastrackServlet.class);
    private EkvrawToFastrackProcessor ekvrawToFastrackProcessor;

    public void handleRequest(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        log.info("[EkvrawToFastrackServlet.handleRequest] is going to execute ekvrawToFastrackProcessor.execute");
        try {
            String deleteEKVRAW = req.getParameter("deleteEKVRAW");
            if(deleteEKVRAW.equals("1")){
                ekvrawToFastrackProcessor.execute(true);
            }else{
                ekvrawToFastrackProcessor.execute(false);
            }
        }catch(Exception e){
            log.error("[EkvrawToFastrackServlet.handleRequest]: ", e);
        }
    }

    public EkvrawToFastrackProcessor getEkvrawToFastrackProcessor() {
        return ekvrawToFastrackProcessor;
    }

    public void setEkvrawToFastrackProcessor(EkvrawToFastrackProcessor ekvrawToFastrackProcessor) {
        this.ekvrawToFastrackProcessor = ekvrawToFastrackProcessor;
    }

    public void init() throws ServletException {
        log.info("[EkvrawToFastrackServlet.init]");
    }

    public void destroy() throws ServletException {
        log.info("[EkvrawToFastrackServlet.destroy]");
    }
}
