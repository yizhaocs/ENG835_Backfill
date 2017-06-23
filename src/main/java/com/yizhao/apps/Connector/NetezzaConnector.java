package com.yizhao.apps.Connector;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author YI ZHAO
 */
public class NetezzaConnector {
    private static final Logger log = Logger.getLogger(NetezzaConnector.class);
    private final String NETEZZA_DB_DRIVER = "org.netezza.Driver";
    private final String DB_CONNECTION = "jdbc:netezza://nz-vip-nym1:5480/opinmind_dev";
    private final String DB_USER = "opinmind_dev_admin";
    private final String DB_PASSWORD = "29JWmn2e";

    public void dataToCsvPartitionByMod(String table, String csvFileOutputPath, String partition) throws SQLException {
        Connection dbConnection = null;
        Statement statement = null;

        String selectTableSQL = null;

        selectTableSQL = "create external table \'" + csvFileOutputPath + "\'" +
                "\n" +
                "using (delim '|' escapechar '\\' remoteSource 'JDBC')" +
                "\n" +
                "as select * from " + table +
                "\n" +
                "WHERE MOD(" + table + ".EVENT_ID, 10)=" + partition +
                "\n" +
                "ORDER BY " + table + ".EVENT_ID";

        try {
            dbConnection = getDBConnection();
            statement = dbConnection.createStatement();

            log.info("execute query: \n" + selectTableSQL);

            // execute select SQL stetement
            statement.execute(selectTableSQL);
        } catch (SQLException e) {
            log.error("Exception in NetezzaConnector.dataToCsvPartitionByMod:" + "\n");
            e.printStackTrace();

        } finally {
            if (statement != null) {
                statement.close();
            }

            if (dbConnection != null) {
                dbConnection.close();
            }
        }
    }

    public void dataToCsvPartitionByYearMonth(String table, String csvFileOutputPath, String year, String month) throws SQLException {
        Connection dbConnection = null;
        Statement statement = null;

        String selectTableSQL = null;

        selectTableSQL = "create external table \'" + csvFileOutputPath + "\'" +
                "\n" +
                "using (delim '|' escapechar '\\' remoteSource 'JDBC')" +
                "\n" +
                "as select * from " + table +
                "\n" +
                "WHERE " + table + ".MODIFICATION_TS >='" + year + "-" + month + "-01'" +
                "\n" +
                "AND " + table + ".MODIFICATION_TS <=last_day('" + year + "-" + month + "-01')" + // select last_day('2017-02-01')
                "\n" +
                "ORDER BY " + table + ".EVENT_ID";

        try {
            dbConnection = getDBConnection();
            statement = dbConnection.createStatement();

            log.info("execute query: \n" + selectTableSQL);

            // execute select SQL stetement
            statement.execute(selectTableSQL);
        } catch (SQLException e) {
            log.error("Exception in NetezzaConnector.dataToCsvPartitionByYearMonth:" + "\n");
            e.printStackTrace();

        } finally {
            if (statement != null) {
                statement.close();
            }

            if (dbConnection != null) {
                dbConnection.close();
            }
        }
    }

    public void generateBackFillTableForApac(String dpIds, String startDate, String endDate) throws SQLException {
        if (dpIds == null) {
            log.error("dpIds is missing in NetezzaConnector.generateBackFillTable");
            return;
        }

        if (startDate == null) {
            log.error("startDate is missing in NetezzaConnector.generateBackFillTable");
            return;
        }

        if (endDate == null) {
            log.error("endDate is missing in NetezzaConnector.generateBackFillTable");
            return;
        }

        Connection dbConnection = null;
        Statement statement = null;

        String query = "CREATE TABLE ENG759_BACKFILL_APAC AS(\n" +
                "SELECT EKV.EVENT_ID, EKV.KEY_ID, EKV.VALUE, EKV.COOKIE_ID, EKV.DP_ID, EKV.LOCATION_ID, EKV.MODIFICATION_TS\n" +
                "FROM OPINMIND_PROD..event_key_value AS EKV\n" +
                "LEFT JOIN OPINMIND_PROD..ekv_hotel AS EKV_H\n" +
                "ON EKV.EVENT_ID=EKV_H.EVENT_ID\n" +
                "LEFT JOIN OPINMIND_PROD..ekv_flight AS EKV_F\n" +
                "ON EKV.EVENT_ID=EKV_F.EVENT_ID\n" +
                "WHERE EKV.DP_ID in (" + dpIds + ")\n" +
                "AND EKV_H.EVENT_ID IS NULL\n" +
                "AND EKV_F.EVENT_ID IS NULL\n" +
                "AND EKV.dw_modification_ts >= '" + startDate + "'\n" +
                "AND EKV.dw_modification_ts <= '" + endDate + "'\n" +
                "ORDER BY EKV.EVENT_ID);";

        try {
            dbConnection = getDBConnection();
            statement = dbConnection.createStatement();

            log.info("execute query: \n" + query);

            // execute select SQL stetement
            statement.execute(query);
        } catch (SQLException e) {
            log.error("Exception in NetezzaConnector.generateBackFillTable:" + "\n");
            e.printStackTrace();

        } finally {
            if (statement != null) {
                statement.close();
            }

            if (dbConnection != null) {
                dbConnection.close();
            }
        }
    }

    public Connection getDBConnection() {
        Connection dbConnection = null;

        try {
            Class.forName(NETEZZA_DB_DRIVER);
        } catch (ClassNotFoundException e) {
            log.error("Exception in NetezzaConnector:" + "\n");
            e.printStackTrace();
        }

        try {
            dbConnection = DriverManager.getConnection(DB_CONNECTION, DB_USER,
                    DB_PASSWORD);
            return dbConnection;
        } catch (SQLException e) {
            log.error("Exception in NetezzaConnector:" + "\n");
            e.printStackTrace();
        }

        return dbConnection;

    }


    public void connectionTesting() {
        try {
            Class.forName(NETEZZA_DB_DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;
        }

        Connection connection = null;

        try {
            connection = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
//            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/marketplace","om", "N3wQA3ra.");

        } catch (SQLException e) {
            log.error("Exception in NetezzaConnector:" + "\n");
            e.printStackTrace();
            return;
        }

        if (connection != null) {
            log.info("You made it, take control your Netezza database now!");
        } else {
            log.info("Failed to make connection to Netezza database!");
        }
    }

    public void init() {

    }


    public void destroy() {

    }
}
