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
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.IOException;
import java.io.BufferedWriter;
import java.io.StringWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.io.IOUtils;
import org.kohsuke.args4j.*;

/**
 *
 * @author Alan Snelson
 */
public class MySQLInstance {

    //Command Line Arguments
    @Option(name = "--help")
    private boolean help;
    @Option(name = "-h", usage = "MySQL Server Hostname")
    private String hostname;
    @Option(name = "-u", usage = "MySQL Username")
    private String username;
    @Option(name = "-p", usage = "MySQL Password")
    private String password;
    @Option(name = "-v")
    private boolean verbose;
    // receives other command line parameters than options
    @Argument
    private List<String> arguments = new ArrayList<String>();
    //Database properties
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
        }
        catch (IOException e) {
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
        }
        catch (SQLException se) {
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
        }
        catch (SQLException se) {
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
            if (verbose) {
                System.out.println("Database connection established");
            }
        }
        catch (SQLException se) {
            throw se;
        }
        catch (Exception e) {
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
    public final void connect(String hostname, String username, String password, String db) throws SQLException {
        connect(hostname, 3306, username, password, db);
    }

    /**
     * Check if schema exists if not create it
     *
     * @param  schema Schema name
     */
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
        }
        catch (SQLException sqle) {
            System.err.println(sqle.getMessage());
        }
    }

    /**
     * Check if schema exist and if so drop it 
     *
     * @param  schema Schema name
     */
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
        }
        catch (SQLException sqle) {
            System.err.println(sqle.getMessage());
        }
    }

    public File dumpAllDatabases() {
        return null;
    }

    /**
     * Get create database script
     *
     * @param  database Database name
     * @return Create database script
     */
    public String dumpCreateDatabase(String database) {
        String createDatabase = null;
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SHOW CREATE DATABASE `" + database + "`");
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                createDatabase = rs.getString("Create Database") + ";";
            }
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        return createDatabase;
    }

    public File dumpDatabase(String database) {
        //TODO Need to dump database structure and data
        return null;
    }

    public File dumpAllTables(String database) {
        //TODO Need to dump all tables within specified schema
        return null;
    }

    /**
     * Convenience method for objects created with schema - calls dumpCreateTable
     *
     * @param event Event to dump
     * @return Create event definition
     */
    public String dumpCreateEvent(String event) {
        return dumpCreateEvent(schema, event);
    }

    /**
     * Get create event definition
     *
     * @param schema Schema name
     * @param event  Event name
     * @return Create event definition
     */
    public String dumpCreateEvent(String schema, String event) {
        String createEvent = "--\n-- Event structure for event `" + event + "`\n--\n\n";
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SHOW CREATE EVENT `" + schema + "`.`" + event + "`");
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                createEvent += rs.getString("Create Event") + ";";
            }
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        return createEvent;
    }

    /**
     * Convenience method for objects created with schema - calls dumpCreateRoutine
     *
     * @param routine Routine to dump
     * @return Create routine definition
     */
    public String dumpCreateRoutine(String routine) {
        return dumpCreateRoutine(schema, routine);
    }

    /**
     * Get create routine definition
     *
     * @param schema Schema name
     * @param routine  Routine name
     * @return Create routine definition
     */
    public String dumpCreateRoutine(String schema, String routine) {
        String createRoutine = "--\n-- Routine structure for routine `" + routine + "`\n--\n\n";
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SELECT ROUTINE_DEFINITION FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME='" + routine + "'");
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                createRoutine += rs.getString("ROUTINE_DEFINITION") + ";";
            }
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            createRoutine = "";
        }
        return createRoutine;
    }

    /**
     * Convenience method for objects created with schema - calls dumpCreateTable
     *
     * @param table Table to dump
     * @return Create table definition
     */
    public String dumpCreateTable(String table) {
        return dumpCreateTable(schema, table);
    }

    /**
     * Get create table definition
     *
     * @param schema Schema name
     * @param table  Table name
     * @return Create table definition
     */
    public String dumpCreateTable(String schema, String table) {
        String createTable = "--\n-- Table structure for table `" + table + "`\n--\n\n";
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SHOW CREATE TABLE `" + schema + "`.`" + table + "`");
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                createTable += rs.getString("Create Table") + ";";
            }
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            createTable = "";
        }
        return createTable;
    }

    /**
     * Convenience method for objects created with schema - calls dumpCreateTrigger
     *
     * @param  trigger Trigger name
     * @return Create trigger definition
     */
    public String dumpCreateTrigger(String trigger) {
        return dumpCreateTrigger(schema, trigger);
    }

    /**
     * Get create trigger definition
     *
     * @param schema Schema name
     * @param trigger  Trigger name
     * @return Create trigger definition
     */
    public String dumpCreateTrigger(String schema, String trigger) {
        String createTrigger = "--\n-- Trigger structure for trigger `" + trigger + "`\n--\n\n";
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SHOW CREATE TRIGGER " + schema + "." + trigger);
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                createTrigger += rs.getString("SQL Original Statement") + ";";
            }
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            createTrigger = "";
        }
        return createTrigger;
    }

    /**
     * Convenience method for objects created with schema - calls dumpCreateView
     *
     * @param  view View name
     * @return Create view definition
     */
    public String dumpCreateView(String view) {
        return dumpCreateView(schema, view);
    }

    /**
     * Get create view definition
     *
     * @param schema Schema name
     * @param table  Table name
     * @return Create table definition
     */
    public String dumpCreateView(String schema, String view) {
        String createView = "--\n-- View definition for view `" + view + "`\n--\n\n";
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SHOW CREATE VIEW `" + schema + "`.`" + view + "`");
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                createView += rs.getString("Create View") + ";";
            }
        }
        catch (SQLException e) {
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
                        }
                        catch (Exception e) {
                            postfix += "NULL,";
                        }
                    } else {
                        try {
                            postfix += "'" + escapeString(rs.getBytes(i)).toString() + "'" + separator;
                        }
                        catch (Exception e) {
                            postfix += "NULL,";
                        }
                    }

                }
                out.write(prefix + postfix);
                ++count;
            }
            rs.close();
            s.close();
        }
        catch (IOException e) {
            System.err.println(e.getMessage());
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
    }

    public Map<String, String> dumpGlobalVariables() {
        Map<String, String> variables = new TreeMap();
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SHOW GLOBAL VARIABLES");
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                variables.put(rs.getString(1), rs.getString(2));
            }
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        return variables;
    }

    public File dumpAllViews(String database) {
        return null;
    }

    public File dumpView(String view) {
        return null;
    }

    /**
     * Execute SQL script
     *
     * @param  schema Schema name
     * @param  script SQL Script
     */
    public String executeScript(String schema, BufferedReader script) {
        String result = "";
        String line;
        StringBuilder sb = new StringBuilder();
        try {
            while (( line = script.readLine() ) != null) {
                //Strip standalone comments
                if (!( line.startsWith("--") || line.startsWith("/*") || line.length() == 0 )) {
                    sb.append(line.trim() + " ");
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
        }
        catch (IOException ioe) {
            System.err.println(ioe.getMessage());
        }
        catch (SQLException sqle) {
            if (conn != null) {
                try {
                    conn.rollback();
                    conn.setAutoCommit(true);
                    return sqle.getMessage();
                }
                catch (SQLException e) {
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
    public String getVersion() {
        return properties.getProperty("application.version");
    }

    /**
     * Get a list of events
     *
     * @param schema Schema name
     * @return List of events
     */
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
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        return events;
    }

    /**
     * Get a list of grant tables
     *
     * @return List of grant tables
     */
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
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return routines;
    }

    /**
     * Get a list of schemata
     *
     * @return List of schemata
     */
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
        }
        catch (SQLException e) {
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
        }
        catch (SQLException e) {
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
        }
        catch (SQLException e) {
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
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return views;
    }

    /**
     * Return current schema
     *
     * @return Currently set schema
     */
    public String getSchema() {
        return schema;
    }

    /**
     * Set current schema for convenience methods
     *
     * @param  schema Schema name
     */
    public void setSchema(String schema) {
        this.schema = schema;
        try {
            conn.setCatalog(schema);
        }
        catch (SQLException sqle) {
            System.err.println(sqle.getMessage());
        }
    }

    /**
     * Convert bytes to hex
     *
     * @param  bIn       String to be converted to hex passed in as byte array
     * @return bOut      MySQL compatible hex string
     */
    public static String byteArrayToHexString(byte[] bIn) {
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
     * Main entry point for MySQLInstance when run from command line
     *
     * @param  args  Command line arguments
     */
    public static void main(String[] args) {
        new MySQLInstance().doMain(args);
    }

    /**
     * Parse command line arguments and run MySQLInstance
     *
     * @param  args  Command line arguments
     */
    public int doMain(String[] args) {

        String usage = "Usage: java -jar MySQLInstance.jar [OPTIONS] database [tables]\nOR     java -jar MySQLInstance.jar [OPTIONS] --databases [OPTIONS] DB1 [DB2 DB3...]\nOR     java -jar MySQLInstance.jar [OPTIONS] --all-databases [OPTIONS]\nFor more options, use java -jar MySQLInstance.jar --help";
        CmdLineParser parser = new CmdLineParser(this);

        // if you have a wider console, you could increase the value;
        // here 80 is also the default
        parser.setUsageWidth(80);

        try {
            // parse the arguments.
            parser.parseArgument(args);

            if (help) {
                throw new CmdLineException("Print Help");
            }

            // after parsing arguments, you should check
            // if enough arguments are given.
            if (arguments.isEmpty()) {
                throw new CmdLineException("No argument is given");
            }

        }
        catch (CmdLineException e) {
            if (e.getMessage().equalsIgnoreCase("Print Help")) {
                System.out.println("MySQLInstance.java Ver " + properties.getProperty("application.version") + "\nThis software comes with ABSOLUTELY NO WARRANTY. This is free software,\nand you are welcome to modify and redistribute it under the BSD license" + "\n\n" + usage);
                return 0;
            }
            // if there's a problem in the command line,
            // you'll get this exception. this will report
            // an error message.
            System.err.println(e.getMessage());
            // print usage.
            System.err.println(usage);
            return 1;
        }


        //Do we have a hostname? if not use localhost as default
        if (hostname == null) {
            hostname = "localhost";
        }
        //First argument here should be database
        schema = arguments.remove(0);

        try {
            //Create temporary file to hold SQL output.
            File temp = File.createTempFile(schema, ".sql");
            BufferedWriter out = new BufferedWriter(new FileWriter(temp));
            this.connect(hostname, username, password, schema);
            out.write(getHeader());
            for (String arg : arguments) {
                out.write(dumpCreateTable(arg));
                this.dumpTable(out, arg);
            }
            out.flush();
            out.close();
            this.cleanup();
            BufferedReader sqlFile = new BufferedReader(new FileReader(temp));
            String sqlLine = new String();
            while ((sqlLine = sqlFile.readLine()) != null) {
                System.out.println(sqlLine);
            }
            sqlFile.close();
            temp.delete();
        }
        catch (SQLException se) {
            System.err.println(se.getMessage());
            return 1;
        }
        catch (IOException ioe) {
            System.err.println(ioe.getMessage());
            return 1;
        }
        return 0;
    }

    /**
     * Convert InputStream to String
     *
     * @param  stream InputStream to convert
     * @return string representation of InputStream
     */
    private String streamToString(InputStream stream) {
        StringWriter writer = new StringWriter();
        try {
            IOUtils.copy(stream, writer);
        }
        catch (IOException ioe) {
            System.err.print(ioe.getMessage());
        }
        return writer.toString();
    }

    /**
     * Close database connection
     *
     * @return Application status code
     */
    public int cleanup() {
        try {
            conn.close();
            if (verbose) {
                System.out.println("Database connection terminated");
            }
        }
        catch (Exception e) { /* ignore close errors */ }
        return 1;
    }
}
