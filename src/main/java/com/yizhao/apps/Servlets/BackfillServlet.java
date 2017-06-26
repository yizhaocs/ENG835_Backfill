package com.yizhao.apps.Servlets;

import com.yizhao.apps.BackfillController;
import com.yizhao.apps.Util.MathUtil;
import org.apache.log4j.Logger;
import org.springframework.web.HttpRequestHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.InetAddress;

/**
 * curl http://localhost:8080/backfill/backfill?mode=backfill&option=d&table=eng759_backfill_apac&startDate=2016-12&endDate=2017-03
 * curl http://localhost:8080/backfill/backfill?mode=dump&option=d&table=eng759_backfill_apac&startDate=2016-12&endDate=2017-03
 * curl http://localhost:8080/backfill/backfill?mode=convert&inputPath=/workplace/yzhao/googleFiles/apac/flightFiles/122016/&outPutPath=/workplace/yzhao/netezzaFiles/apac/flightFiles/122016&monthYear=122016&type=hotel&partition=
 */
public class BackfillServlet implements HttpRequestHandler {
    private static final Logger log = Logger.getLogger(BackfillServlet.class);
    private BackfillController backfillController;
    private static final String DEFAULT_FILE_PATH = "/home/yzhao/ENG835/";

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
        // for mode = convert
        String inputPath = null;
        String outPutPath = null;
        String monthYear = null;
        String type = null;
        String partition = null;

        if (mode == null) {
            return;
        }

        try {
            if (mode.equals("convert")) {
                if (inputPath == null) {
                    log.error("inputPath is null");
                    return;
                }

                if (outPutPath == null) {
                    log.error("outPutPath is null");
                    return;
                }

                if (monthYear == null) {
                    log.error("monthYear is null");
                    return;
                }

                if (type == null) {
                    log.error("type is null");
                    return;
                }

                backfillController.googleCloudFileToNetezzaFileConvertor.process(inputPath, outPutPath + "/ekv_" + type + "_all_netezza-" + monthYear + "_" + type + "_001.csv", type);
            } else if (mode.equals("dump")) {
                if (option == null) {
                    log.error("option is null");
                    return;
                }

                if (option.equals("d")) {
                    if (startDate == null) {
                        log.error("startDate is null");
                        return;
                    }
                } else if (option.equals("r")) {
                    if (partition == null) {
                        return;
                    }
                }

                if (table == null) {
                    log.error("table is null");
                    return;
                }

                String startYear = null;
                String startYearMonth = null;
                String endYear = null;
                String endYearMonth = null;
                if (option.equals("d")) {
                    if (startDate != null) {
                        String[] startYearDateStr = startDate.split("-");
                        startYear = startYearDateStr[0];
                        startYearMonth = startYearDateStr[1];
                    }

                    if (endDate != null) {
                        String[] endYearDateStr = endDate.split("-");
                        endYear = endYearDateStr[0];
                        endYearMonth = endYearDateStr[1];
                    }


                }


                String csvFileOutputPath = DEFAULT_FILE_PATH + table + "_csvFileOutputPath.csv";
                String fastrackFileOutputPath = DEFAULT_FILE_PATH;

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

                if (option.equals("d")) {
                    log.info("startYear:" + startYear);
                    log.info("startYearMonth:" + startYearMonth);
                    log.info("endYear:" + endYear);
                    log.info("endYearMonth:" + endYearMonth);


                    if (endDate != null) {
                        int count = 0;
                        String curYear = startYear;
                        String curYearMonth = startYearMonth;
                        while (!curYear.equals(endYear) || !curYearMonth.equals(endYearMonth)) {
                            backfillController.runBackfill(table, csvFileOutputPath, null, curYear, curYearMonth, fastrackFileOutputPath, fileHostName);

                            count++;
                            if (!curYear.equals(endYear) && !curYearMonth.equals("12")) {
                                curYearMonth = new String(MathUtil.plusOne(curYearMonth.toCharArray()));
                            } else if (!curYear.equals(endYear) && curYearMonth.equals("12")) {
                                curYear = new String(MathUtil.plusOne(curYear.toCharArray()));
                                curYearMonth = "01";
                            } else if (curYear.equals(endYear) && !curYearMonth.equals(endYearMonth)) {
                                curYearMonth = new String(MathUtil.plusOne(curYearMonth.toCharArray()));
                            } else {
                                log.info("curYear and curYearMonth are same as endYear and endYearMonth");
                            }
                        }

                        // run for the final month
                        backfillController.dumpEkvrawFromNetezza(table, csvFileOutputPath, partition, curYear, curYearMonth);
                    } else {
                        // only get one month
                        backfillController.dumpEkvrawFromNetezza(table, csvFileOutputPath, partition, startYear, startYearMonth);
                    }
                } else if (option.equals("r")) {
                    if (partition == null) {
                        int i = 0;
                        while (i < 10) {
                            backfillController.dumpEkvrawFromNetezza(table, csvFileOutputPath, String.valueOf(i), null, null);
                            i++;
                        }
                    } else {
                        backfillController.dumpEkvrawFromNetezza(table, csvFileOutputPath, partition, null, null);
                    }
                }
            } else if (mode.equals("backfill")) {
                if (option == null) {
                    log.error("option is null");
                    return;
                }

                if (option.equals("d")) {
                    if (startDate == null) {
                        log.error("startDate is null");
                        return;
                    }
                } else if (option.equals("r")) {
                    if (partition == null) {
                        return;
                    }
                }

                if (table == null) {
                    log.error("table is null");
                    return;
                }

                String startYear = null;
                String startYearMonth = null;
                String endYear = null;
                String endYearMonth = null;
                if (option.equals("d")) {
                    if (startDate != null) {
                        String[] startYearDateStr = startDate.split("-");
                        startYear = startYearDateStr[0];
                        startYearMonth = startYearDateStr[1];
                    }

                    if (endDate != null) {
                        String[] endYearDateStr = endDate.split("-");
                        endYear = endYearDateStr[0];
                        endYearMonth = endYearDateStr[1];
                    }
                }


                String csvFileOutputPath = DEFAULT_FILE_PATH + table + "_csvFileOutputPath.csv";
                String fastrackFileOutputPath = DEFAULT_FILE_PATH;


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

                if (option.equals("d")) {
                    log.info("startYear:" + startYear);
                    log.info("startYearMonth:" + startYearMonth);
                    log.info("endYear:" + endYear);
                    log.info("endYearMonth:" + endYearMonth);


                    if (endDate != null) {
                        int count = 0;
                        String curYear = startYear;
                        String curYearMonth = startYearMonth;
                        while (!curYear.equals(endYear) || !curYearMonth.equals(endYearMonth)) {
                            backfillController.runBackfill(table, csvFileOutputPath, null, curYear, curYearMonth, fastrackFileOutputPath, fileHostName);

                            count++;
                            if (!curYear.equals(endYear) && !curYearMonth.equals("12")) {
                                curYearMonth = new String(MathUtil.plusOne(curYearMonth.toCharArray()));
                            } else if (!curYear.equals(endYear) && curYearMonth.equals("12")) {
                                curYear = new String(MathUtil.plusOne(curYear.toCharArray()));
                                curYearMonth = "01";
                            } else if (curYear.equals(endYear) && !curYearMonth.equals(endYearMonth)) {
                                curYearMonth = new String(MathUtil.plusOne(curYearMonth.toCharArray()));
                            } else {
                                log.info("curYear and curYearMonth are same as endYear and endYearMonth");
                            }
                        }

                        // run for the final month
                        backfillController.runBackfill(table, csvFileOutputPath, null, curYear, curYearMonth, fastrackFileOutputPath, fileHostName);
                    } else {
                        // only get one month
                        backfillController.runBackfill(table, csvFileOutputPath, null, startYear, startYearMonth, fastrackFileOutputPath, fileHostName);
                    }
                } else if (option.equals("r")) {
                    if (partition == null) {
                        int i = 0;
                        while (i < 10) {
                            backfillController.runBackfill(table, csvFileOutputPath, String.valueOf(i), null, null, fastrackFileOutputPath, fileHostName);
                            i++;
                        }
                    } else {
                        backfillController.runBackfill(table, csvFileOutputPath, partition, null, null, fastrackFileOutputPath, fileHostName);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Exception in Main:" + "\n");
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
