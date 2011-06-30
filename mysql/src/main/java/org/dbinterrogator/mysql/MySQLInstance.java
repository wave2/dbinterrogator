/**
 * Copyright (c) 2007-2011 Wave2 Limited. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of Wave2 Limited nor the names of its contributors may be
 * used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.dbinterrogator.mysql;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Map;
import java.util.Properties;

/**
 *
 * @author Alan Snelson
 */
public class MySQLInstance implements Instance {

    //Database properties
    private String hostname = null;
    private String schema = null;
    private Connection conn = null;
    private DatabaseMetaData databaseMetaData;
    private String databaseProductVersion = null;
    private int databaseProductMajorVersion = 0;
    private int databaseProductMinorVersion = 0;
    private String mysqlVersion = null;
    //Application properties
    private final Properties properties = new Properties();

    /**
     * Load application properties
     */
    private void loadProperties() {
        try {
            properties.load(getClass().getResourceAsStream("/application.properties"));
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }

    /**
     * Default constructor for MySQLInstance.
     */
    public MySQLInstance() {
        loadProperties();
    }

    /**
     * Create a new instance of MySQLInstance using default database.
     *
     * @param  hostname  MySQL Server Hostname
     * @param  username  MySQL Username
     * @param  password  MySQL Password
     */
    public MySQLInstance(String hostname, String username, String password) throws SQLException {
        loadProperties();
        try {
            connect(hostname, username, password, "mysql");
        } catch (SQLException se) {
            throw se;
        }
    }

    /**
     * Create a new instance of MySQLInstance using supplied database.
     *
     * @param  hostname  MySQL Server Hostname
     * @param  username  MySQL Username
     * @param  password  MySQL Password
     * @param  db        Default database
     */
    public MySQLInstance(String host, String username, String password, String db) throws SQLException {
        loadProperties();
        try {
            connect(host, username, password, db);
        } catch (SQLException se) {
            throw se;
        }
    }

    /**
     * Connect to MySQL server
     *
     * @param  hostname  MySQL Server Hostname
     * @param  username  MySQL Username
     * @param  password  MySQL Password
     * @param  db        Default database
     */
    @Override
    public void connect(String hostname, int port, String username, String password, String db) throws SQLException {
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            conn = DriverManager.getConnection("jdbc:mysql://" + hostname + ":" + port + "/" + db, username, password);
            databaseMetaData = conn.getMetaData();
            databaseProductVersion = databaseMetaData.getDatabaseProductVersion();
            databaseProductMajorVersion = databaseMetaData.getDatabaseMajorVersion();
            databaseProductMinorVersion = databaseMetaData.getDatabaseMinorVersion();
            this.hostname = hostname;
            schema = db;
        } catch (SQLException se) {
            throw se;
        } catch (Exception e) {
            System.err.println("Cannot connect to database server");
        }
    }

    /**
     * Connect to MySQL server
     *
     * @param  host      MySQL Server Hostname
     * @param  username  MySQL Username
     * @param  password  MySQL Password
     * @param  db        Default database
     */
    @Override
    public final void connect(String hostname, String username, String password, String db) throws SQLException {
        connect(hostname, 3306, username, password, db);
    }

    /**
     * Check if schema exists if not create it
     *
     * @param  schema Schema name
     */
    @Override
    public void createSchema(String schema) {
        //Ok lets see if the database exists - if not create it
        try {
            conn.setCatalog("INFORMATION_SCHEMA");
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SELECT COUNT(*) AS schema_exists FROM SCHEMATA WHERE SCHEMA_NAME='" + schema + "';");
            ResultSet rs = s.getResultSet();
            rs.next();
            if (rs.getInt("schema_exists") != 1) {
                Statement stmt = conn.createStatement();
                //Create Schema
                stmt.executeUpdate("CREATE DATABASE " + schema);
                stmt.close();
            }
        } catch (SQLException sqle) {
            System.err.println(sqle.getMessage());
        }
    }

    /**
     * Check if schema exist and if so drop it 
     *
     * @param  schema Schema name
     */
    @Override
    public void dropSchema(String schema) {
        //Ok lets see if the database exists - if so drop it
        try {
            conn.setCatalog("INFORMATION_SCHEMA");
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SELECT COUNT(*) AS schema_exists FROM SCHEMATA WHERE SCHEMA_NAME='" + schema + "';");
            ResultSet rs = s.getResultSet();
            rs.next();
            if (rs.getInt("schema_exists") == 1) {
                Statement stmt = conn.createStatement();
                //Create Schema
                stmt.executeUpdate("DROP DATABASE " + schema);
                stmt.close();
            }
        } catch (SQLException sqle) {
            System.err.println(sqle.getMessage());
        }
    }

    /**
     * Get create database script
     *
     * @param  database Database name
     * @return Create database script
     */
    @Override
    public String getCreateDatabase(String database) {
        String createDatabase = null;
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SHOW CREATE DATABASE `" + database + "`");
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                createDatabase = rs.getString("Create Database") + ";";
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        return createDatabase;
    }

    /**
     * Convenience method for objects created with schema - calls getCreateTable
     *
     * @param event Event to dump
     * @return Create event definition
     */
    @Override
    public String getCreateEvent(String event) {
        return getCreateEvent(schema, event);
    }

    /**
     * Get create event definition
     *
     * @param schema Schema name
     * @param event  Event name
     * @return Create event definition
     */
    @Override
    public String getCreateEvent(String schema, String event) {
        String createEvent = "--\n-- Event structure for event `" + event + "`\n--\n\n";
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SHOW CREATE EVENT `" + schema + "`.`" + event + "`");
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                createEvent += rs.getString("Create Event") + ";";
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        return createEvent;
    }

    /**
     * Convenience method for objects created with schema - calls getCreateRoutine
     *
     * @param routine Routine to dump
     * @return Create routine definition
     */
    @Override
    public String getCreateRoutine(String routine) {
        return getCreateRoutine(schema, routine);
    }

    /**
     * Get create routine definition
     *
     * @param schema Schema name
     * @param routine  Routine name
     * @return Create routine definition
     */
    @Override
    public String getCreateRoutine(String schema, String routine) {
        String createRoutine = "--\n-- Routine structure for routine `" + routine + "`\n--\n\n";
        try {
            PreparedStatement stmt = conn.prepareStatement(convertStreamToString(getClass().getResourceAsStream("getCreateRoutine.sql")),ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            stmt.setString(1, schema);
            stmt.setString(2, routine);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                createRoutine += rs.getString("ROUTINE_DEFINITION") + ";";
            }
            rs.close();
            stmt.close();
        } catch (SQLException sqle) {
            System.err.println(sqle.getMessage());
            createRoutine = "";
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
        }
        return createRoutine;
    }

    /**
     * Convenience method for objects created with schema - calls getCreateTable
     *
     * @param table Table to dump
     * @return Create table definition
     */
    @Override
    public String getCreateTable(String table) {
        return getCreateTable(schema, table);
    }

    /**
     * Get create table definition
     *
     * @param schema Schema name
     * @param table  Table name
     * @return Create table definition
     */
    @Override
    public String getCreateTable(String schema, String table) {
        String createTable = "--\n-- Table structure for table `" + table + "`\n--\n\n";
        try {
            Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            stmt.executeQuery("SHOW CREATE TABLE `" + schema + "`.`" + table + "`");
            ResultSet rs = stmt.getResultSet();
            while (rs.next()) {
                createTable += rs.getString("Create Table") + ";";
            }
            rs.close();
            stmt.close();
        } catch (SQLException sqle) {
            System.err.println(sqle.getMessage());
            createTable = "";
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
        }
        return createTable;
    }

    /**
     * Convenience method for objects created with schema - calls getCreateTrigger
     *
     * @param  trigger Trigger name
     * @return Create trigger definition
     */
    @Override
    public String getCreateTrigger(String trigger) {
        return getCreateTrigger(schema, trigger);
    }

    /**
     * Get create trigger definition
     *
     * @param schema Schema name
     * @param trigger  Trigger name
     * @return Create trigger definition
     */
    @Override
    public String getCreateTrigger(String schema, String trigger) {
        String createTrigger = "--\n-- Trigger structure for trigger `" + trigger + "`\n--\n\n";
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SHOW CREATE TRIGGER " + schema + "." + trigger);
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                createTrigger += rs.getString("SQL Original Statement") + ";";
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            createTrigger = "";
        }
        return createTrigger;
    }

    /**
     * Convenience method for objects created with schema - calls getCreateView
     *
     * @param  view View name
     * @return Create view definition
     */
    @Override
    public String getCreateView(String view) {
        return getCreateView(schema, view);
    }

    /**
     * Get create view definition
     *
     * @param schema Schema name
     * @param table  Table name
     * @return Create table definition
     */
    @Override
    public String getCreateView(String schema, String view) {
        String createView = "--\n-- View definition for view `" + view + "`\n--\n\n";
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SHOW CREATE VIEW `" + schema + "`.`" + view + "`");
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                createView += rs.getString("Create View") + ";";
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            createView = "";
        }
        return createView;
    }

    /**
     * Create script with insert statements: NOTE THIS IS INCOMPLETE AND ANY CONTRIBUTIONS WOULD BE WELCOME
     *
     * @param  out    BufferedWriter
     * @param  table  Table Name
     */
    public void dumpTable(BufferedWriter out, String table) {
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SELECT /*!40001 SQL_NO_CACHE */ * FROM `" + table + "`");
            ResultSet rs = s.getResultSet();
            ResultSetMetaData rsMetaData = rs.getMetaData();
            if (rs.last()) {
                out.write("\n\n--\n-- Dumping data for table `" + table + "`\n--\n\n");
                rs.beforeFirst();
            }
            int columnCount = rsMetaData.getColumnCount();
            String prefix = "INSERT INTO `" + table + "` (";
            for (int i = 1; i <= columnCount; i++) {
                if (i == columnCount) {
                    prefix += rsMetaData.getColumnName(i) + ") VALUES(";
                } else {
                    prefix += rsMetaData.getColumnName(i) + ",";
                }
            }
            String postfix = new String();
            String separator = ",";
            int count = 0;
            while (rs.next()) {
                postfix = "";
                for (int i = 1; i <= columnCount; i++) {
                    if (i == columnCount) {
                        separator = ");\n";
                    }
                    //[#1] Convert LongBlob data to hex string to avoid character encoding issues
                    if (rs.getMetaData().getColumnTypeName(i).equalsIgnoreCase("LONGBLOB")) {
                        try {
                            postfix += "UNHEX('" + byteArrayToHexString(rs.getBytes(i)) + "')" + separator;
                        } catch (Exception e) {
                            postfix += "NULL,";
                        }
                    } else {
                        try {
                            postfix += "'" + escapeString(rs.getBytes(i)).toString() + "'" + separator;
                        } catch (Exception e) {
                            postfix += "NULL,";
                        }
                    }

                }
                out.write(prefix + postfix);
                ++count;
            }
            rs.close();
            s.close();
        } catch (IOException e) {
            System.err.println(e.getMessage());
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
    }

    @Override
    public Map<String, String> getGlobalVariables() {
        Map<String, String> variables = new TreeMap();
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SHOW GLOBAL VARIABLES");
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                variables.put(rs.getString(1), rs.getString(2));
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        return variables;
    }

    /**
     * Execute SQL script
     *
     * @param  schema Schema name
     * @param  script SQL Script
     */
    @Override
    public String executeScript(String schema, BufferedReader script) {
        String result = "";
        String line;
        StringBuilder sb = new StringBuilder();
        try {
            while ((line = script.readLine()) != null) {
                //Strip standalone comments
                if (!(line.startsWith("--") || line.startsWith("/*") || line.length() == 0)) {
                    sb.append(line.trim()).append(" ");
                }
            }
            conn.setCatalog(schema);
            conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();
            for (String sqlstmt : sb.toString().replaceAll("(?i)(^|; )(ALTER|CREATE|DROP|INSERT|UPDATE|RENAME)", "$1ZZZZ$2").split("ZZZZ")) {
                if (!sqlstmt.equals("")) {
                    stmt.addBatch(sqlstmt);
                }
            }
            stmt.executeBatch();
            conn.commit();
            conn.setAutoCommit(true);
            stmt.close();
        } catch (IOException ioe) {
            System.err.println(ioe.getMessage());
        } catch (SQLException sqle) {
            if (conn != null) {
                try {
                    conn.rollback();
                    conn.setAutoCommit(true);
                    return sqle.getMessage();
                } catch (SQLException e) {
                    return e.getMessage();
                }

            }
        }
        return result;
    }

    /**
     * Get MySQLInstance version
     *
     * @return MySQLInstance version
     */
    @Override
    public String getVersion() {
        return properties.getProperty("application.version");
    }

    /**
     * Get a list of events
     *
     * @param schema Schema name
     * @return List of events
     */
    @Override
    public ArrayList<String> listEvents(String schema) {
        ArrayList<String> events = new ArrayList();
        try {
            //Version 5 upwards use information_schema
            if (databaseProductMajorVersion >= 5 && databaseProductMinorVersion >= 1) {
                Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
                s.executeQuery("SELECT EVENT_NAME FROM INFORMATION_SCHEMA.EVENTS WHERE EVENT_SCHEMA='" + schema + "'");
                ResultSet rs = s.getResultSet();
                while (rs.next()) {
                    events.add(rs.getString(1));
                }
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        return events;
    }

    /**
     * Get a list of grant tables
     *
     * @return List of grant tables
     */
    @Override
    public ArrayList<String> listGrantTables() {
        ArrayList<String> grantTables = new ArrayList();
        grantTables.add("user");
        grantTables.add("db");
        grantTables.add("tables_priv");
        grantTables.add("columns_priv");
        //The procs_priv table exists as of MySQL 5.0.3.
        if (databaseProductMajorVersion > 4) {
            grantTables.add("procs_priv");
        }
        return grantTables;
    }

    /**
     * Get a list of routines
     *
     * @param schema Schema name
     * @return List of routines
     */
    @Override
    public ArrayList<String> listRoutines(String schema) {
        ArrayList<String> routines = new ArrayList();
        //Triggers were included beginning with MySQL 5.0.2
        if (databaseProductMajorVersion < 5) {
            return routines;
        }
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA='" + schema + "'");
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                routines.add(rs.getString(1));
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return routines;
    }

    /**
     * Get a list of schemata
     *
     * @return List of schemata
     */
    @Override
    public ArrayList<String> listSchemata() {
        ArrayList<String> schemata = new ArrayList();
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SHOW DATABASES");
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                //Skip Information Schema
                if (!rs.getString("Database").equals("information_schema")) {
                    schemata.add(rs.getString("Database"));
                }
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return schemata;
    }

    /**
     * Get a list of tables
     *
     * @param schema Schema name
     * @return List of tables
     */
    @Override
    public ArrayList<String> listTables(String schema) {
        ArrayList<String> tables = new ArrayList();
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            //Version 5 upwards use information_schema
            if (databaseProductMajorVersion < 5) {
                s.executeQuery("SHOW TABLES FROM `" + schema + "`");
            } else {
                s.executeQuery("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '" + schema + "'");
            }
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                tables.add(rs.getString(1));
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return tables;
    }

    /**
     * Get a list of triggers
     *
     * @param schema Schema name
     * @return List of triggers
     */
    @Override
    public ArrayList<String> listTriggers(String schema) {
        ArrayList<String> triggers = new ArrayList();
        //Triggers were included beginning with MySQL 5.0.2
        if (databaseProductMajorVersion < 5) {
            return triggers;
        }
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SELECT TRIGGER_NAME FROM INFORMATION_SCHEMA.TRIGGERS WHERE TRIGGER_SCHEMA='" + schema + "'");
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                triggers.add(rs.getString(1));
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return triggers;
    }

    /**
     * get a list of views
     *
     * @param  schema Schema name
     * @return List of views
     */
    @Override
    public ArrayList<String> listViews(String schema) {
        ArrayList<String> views = new ArrayList();
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            //Version 5 upwards use information_schema
            if (databaseProductMajorVersion < 5) {
                //Views were introduced in version 5.0.1
                return views;
            } else {
                s.executeQuery("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'VIEW' AND TABLE_SCHEMA = '" + schema + "'");
            }
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                views.add(rs.getString(1));
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return views;
    }

    /**
     * Return current schema
     *
     * @return Currently set schema
     */
    @Override
    public String getSchema() {
        return schema;
    }

    /**
     * Set current schema for convenience methods
     *
     * @param  schema Schema name
     */
    @Override
    public void setSchema(String schema) {
        this.schema = schema;
        try {
            conn.setCatalog(schema);
        } catch (SQLException sqle) {
            System.err.println(sqle.getMessage());
        }
    }

    /**
     * Convert bytes to hex
     *
     * @param  bIn       String to be converted to hex passed in as byte array
     * @return bOut      MySQL compatible hex string
     */
    private static String byteArrayToHexString(byte[] bIn) {
        StringBuilder sb = new StringBuilder(bIn.length * 2);
        for (int i = 0; i < bIn.length; i++) {
            int v = bIn[i] & 0xff;
            if (v < 16) {
                sb.append('0');
            }
            sb.append(Integer.toHexString(v));
        }
        return sb.toString().toUpperCase();
    }

    /**
     * Escape string ready for insert via mysql client
     *
     * @param  bIn       String to be escaped passed in as byte array
     * @return bOut      MySQL compatible insert ready ByteArrayOutputStream
     */
    private ByteArrayOutputStream escapeString(byte[] bIn) {
        int numBytes = bIn.length;
        ByteArrayOutputStream bOut = new ByteArrayOutputStream(numBytes + 2);
        for (int i = 0; i < numBytes; ++i) {
            byte b = bIn[i];

            switch (b) {
                case 0: /* Must be escaped for 'mysql' */
                    bOut.write('\\');
                    bOut.write('0');
                    break;

                case '\n': /* Must be escaped for logs */
                    bOut.write('\\');
                    bOut.write('n');
                    break;

                case '\r':
                    bOut.write('\\');
                    bOut.write('r');
                    break;

                case '\\':
                    bOut.write('\\');
                    bOut.write('\\');

                    break;

                case '\'':
                    bOut.write('\\');
                    bOut.write('\'');

                    break;

                case '"': /* Better safe than sorry */
                    bOut.write('\\');
                    bOut.write('"');
                    break;

                case '\032': /* This gives problems on Win32 */
                    bOut.write('\\');
                    bOut.write('Z');
                    break;

                default:
                    bOut.write(b);
            }
        }
        return bOut;
    }

    /**
     * Return MySQLInstance header for output to file
     *
     * @return MySQLInstance header
     */
    private String getHeader() {
        //return Dump Header        
        return "-- BinaryStor MySQL Dump " + properties.getProperty("application.version") + "\n--\n-- Host: " + hostname + "    " + "Database: " + schema + "\n-- ------------------------------------------------------\n-- Server Version: " + databaseProductVersion + "\n--";
    }

    /**
     * Convert InputStream into String
     *
     * @param is InputStream
     * 
     * @return String
     */  
    private static String convertStreamToString(InputStream is) throws Exception {
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        StringBuilder sb = new StringBuilder();
        String line = null;
        while ((line = reader.readLine()) != null) {
            sb.append(line).append("\n");
        }
        is.close();
        return sb.toString();
    }

    /**
     * Close database connection
     *
     * @return Application status code
     */
    @Override
    public int cleanup() {
        try {
            conn.close();
        } catch (Exception e) { /* ignore close errors */ }
        return 1;
    }
}
