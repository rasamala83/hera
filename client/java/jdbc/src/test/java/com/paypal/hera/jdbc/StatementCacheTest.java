package com.paypal.hera.jdbc;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;


/**
 * @since: Mar 2025
 * @author: anagopal
 */
public class StatementCacheTest {
    
	private final static  Logger LOGGER = LoggerFactory.getLogger(StatementCacheTest.class);
	private static Connection  dirDBConn;
	private static  HeraConnection dbConn,dbConn2;

    static class User {
        int userId;
        String name;
        String email;
        String createdDate; // Format: YYYY-MM-DD

        User(int userId, String name, String email, String createdDate) {
            this.userId = userId;
            this.name = name;
            this.email = email;
            this.createdDate = createdDate;
        }
    }

    // Sample data
    private static final List<User> users = Collections.unmodifiableList(Arrays.asList(
            new User(1, "John Doe", "john@example.com", "2025-05-01"),
            new User(2, "Jane Smith", "jane@example.com", "2025-05-02"),
            new User(3, "Bob Johnson", "bob@example.com", "2025-05-03"),
            new User(4, "Bob Raj", "bobraj@example.com", "2025-05-02"),
            new User(5, "John Smith", "johnsmith@example.com", "2025-05-04")
    ));

	@BeforeClass
	public static void setUp() throws IOException, InterruptedException{
	LOGGER.debug("ClientInfoPoolnameTest :: setUp {}","starting oralce and mux");
        Util.startOracleContainer("oracle-xe-21","heraapp","heraappstg");
	Util.makeAndStartMuxOracleWorker(null);
    }

    @AfterClass
    public static void teardown() throws IOException, InterruptedException {	
	LOGGER.debug("ClientInfoPoolnameTest :: teardown {}","sttopping  oralce and mux");
	Runtime.getRuntime().exec("docker stop oracle-xe-21").waitFor();
	Runtime.getRuntime().exec("docker rm oracle-xe-21").waitFor();
	Runtime.getRuntime().exec("killall -ILL mux oracleworker").waitFor();
    }


   @Test
    public void testOCIStatementCache() throws IOException, SQLException {
	LOGGER.debug("testOCIStatementCache :: testOCIStatementCache {}","starting test");
        dirDBConn = getDBConnection();
        //Create table structure
        assert dirDBConn != null;
        createTestDataTable(dirDBConn);

        //check if oracle container is running
        Runtime.getRuntime().exec("docker inspect -f '{{.State.Running}}' oracle-xe-21");

        dbConn = Util.makeDbConn();

        //get session Id
        int sid =  getSessionId(dbConn);
        long initialHardParserCount = getHardParseCount(dbConn, sid);

        //Perform bulk insert
       long startTime = System.currentTimeMillis();
       //Populate the table
       final String insertSQL = "INSERT INTO user (user_id, name, email, created_date) VALUES (?, ?, ?, ?)";

       try (PreparedStatement ps = dbConn.prepareStatement(insertSQL)) {
           SimpleDateFormat dataFormatter = new SimpleDateFormat("yyyy-MM-dd");
           for (User user : users) {
               ps.setInt(1, user.userId);
               ps.setString(2, user.name);
               ps.setString(3, user.email);
               ps.setDate(4, new Date(dataFormatter.parse(user.createdDate).getTime()));
               ps.addBatch();
           }
           int[] updateCounts = ps.executeBatch();
           dbConn.commit();
       } catch (Exception ex) {
           dbConn.rollback();
           throw new SQLException("Batch Insertion failed: " + ex.getMessage(), ex);
       }
       long elapsedTime = System.currentTimeMillis() - startTime;
       LOGGER.info("Elapsed time while inserting data to table : {} using session Id: {}", elapsedTime, sid);
       long finalHardParses = getHardParseCount(dbConn, sid);
       int[] parseStats = getParseStats(dbConn, insertSQL);
       LOGGER.info("SQL parsing data as part of data inserts to the table, total exec: {} number of parsing: {}", parseStats[1], parseStats[0]);
       LOGGER.info("Hard parsing count for session Id: {} with executions: {} and count: {}", sid, parseStats[1], finalHardParses);
       assert parseStats[0] == 1;
       assert parseStats[1] == users.size();

       //Use different connection for select
       //get session Id
       dbConn2 = Util.makeDbConn();
       sid =  getSessionId(dbConn2);
       initialHardParserCount = getHardParseCount(dbConn2, sid);
       final String query = "SELECT * FROM user WHERE user_id = ?";
       Random rand = new Random();
       finalHardParses = getHardParseCount(dbConn2, sid);
       parseStats = getParseStats(dbConn2, insertSQL);
       LOGGER.info(" Before: Conn2: SQL parsing data as part of data inserts to the table, total exec: {} number of parsing: {}", parseStats[1], parseStats[0]);
       LOGGER.info("Before: Conn2: Hard parsing count for session Id: {} with executions: {} and count: {}", sid, parseStats[1], finalHardParses);
       //Executes selects
       for(int i = 0; i < 100; i++) {
          executeQuery(dbConn2, query, rand.nextInt(5) + 1);
       }
       finalHardParses = getHardParseCount(dbConn2, sid);
       parseStats = getParseStats(dbConn2, insertSQL);
       LOGGER.info("Before: SQL parsing data as part of data inserts to the table, total exec: {} number of parsing: {}", parseStats[1], parseStats[0]);
       LOGGER.info("Before: Hard parsing count for session Id: {} with executions: {} and count: {}", sid, parseStats[1], finalHardParses);
       assert parseStats[0] == 100;
       assert parseStats[1] <= 2;
    }

    private void executeQuery(final Connection conn, final String query, final int bindVal) throws SQLException {
        try(PreparedStatement psmt  = conn.prepareStatement(query)) {
            psmt.setInt(1, bindVal);
            try(ResultSet rs = psmt.executeQuery()) {
                rs.next();
            }
        }
    }
    private void createTestDataTable(final Connection dirDBConn) throws SQLException {
        //Create table with some data
        final String tableCreateDDL = "CREATE TABLE user (user_id NUMBER PRIMARY KEY, name VARCHAR2(50) NOT NULL, email VARCHAR2(100), created_date DATE DEFAULT SYSDATE)";
        ResultSet rs = null;
        Statement stmt = null;
        try {
             stmt = dirDBConn.prepareStatement(tableCreateDDL);
             rs = stmt.executeQuery(tableCreateDDL);
        } catch (SQLException e) {
            LOGGER.error("failed while populate data in table: {0}", e);
            throw e;
        }  finally {
            if (stmt != null) {
                stmt.close();
            }

            if (rs != null) {
                rs.close();
            }
        }
    }

    private Connection getDBConnection(){
        String jdbcUrl = "jdbc:oracle:thin:@localhost:1521/poolname";
        String username = "heraapp";
        String password = "heraappstg";
	
	LOGGER.debug("Connecting to database directly :: "+jdbcUrl);
        int maxRetries = 10;
        int retryCount = 0;
        boolean success = false;
        while (retryCount < maxRetries && !success) {
            try {
                Thread.sleep(1500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            try {
                Class.forName("oracle.jdbc.driver.OracleDriver");
                Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
                LOGGER.debug("Connected to Oracle database successfully!");
                success =true;
                return connection;
            } catch (SQLException e) {
                LOGGER.error("Error connecting to Oracle database:",new Exception(e));
            }catch (Exception exc){
                LOGGER.error("Error connecting to Oracle database: ", new Exception(exc));
            }
        }
        return null;
    }

    private static int getSessionId(Connection conn) throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement("SELECT sid FROM V$SESSION WHERE username = USER");
             ResultSet rs = pstmt.executeQuery()) {
            if (rs.next()) {
                return rs.getInt(1);
            }
            throw new SQLException("Unable to retrieve session ID.");
        }
    }

    private static int[] getParseStats(final Connection conn, final String sqlText) throws SQLException {
        final String query = "SELECT parse_calls, executions FROM V$SQL WHERE sql_text = ?";
        try(PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, sqlText);
            try(ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return new int[]{rs.getInt(1), rs.getInt(2)};
                }
                return new int[]{0, 0}; // No stats found
            } catch (SQLException e) {
                pstmt.close();
                throw new SQLException("Failed to fetch V$SQL data: "+e.getMessage(), e);
            }
        } catch (SQLException e) {
            throw new SQLException("Failed to fetch V$SQL data: "+e.getMessage(), e);
        }
    }

    private static long getHardParseCount(Connection conn, int sid) throws SQLException {
        String sql = "SELECT value FROM V$SESSTAT WHERE statistic# = " +
                "(SELECT statistic# FROM V$STATNAME WHERE name = 'parse count (hard)') AND sid = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, sid);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
                throw new SQLException("Unable to retrieve hard parse count.");
            }
        }
    }
}
