/**
 * Copyright (C) 2008-2012 Wave2 Limited. All rights reserved.
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
package org.dbinterrogator.mssql;

import java.io.IOException;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.BufferedWriter;
import java.io.StringWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.commons.io.IOUtils;
import org.kohsuke.args4j.*;

/**
 *
 * @author Alan Snelson
 */
public class MSSQLInstance {

    //Command Line Arguments
    @Option(name = "--help")
    private boolean help;
    @Option(name = "-h", usage = "MSSQL Server Hostname")
    private String hostname;
    @Option(name = "-u", usage = "MSSQL Username")
    private String username;
    @Option(name = "-p", usage = "MSSQL Password")
    private String password;
    @Option(name = "-v")
    private boolean verbose = true;
    // receives other command line parameters than options
    @Argument
    private List<String> arguments = new ArrayList<String>();
    private String schema = null;
    private Connection conn = null;
    private DatabaseMetaData databaseMetaData;
    private String databaseProductVersion = null;
    private int databaseProductMajorVersion = 0;
    private int databaseProductMinorVersion = 0;
    private String mssqlVersion = null;
    private Properties properties;

    public MSSQLInstance(){
        //Load properties
        properties = new Properties();
        try {
            properties.load(getClass().getResourceAsStream("/application.properties"));
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }
    
    /**
     * Connect to MSSQL server
     *
     * @param  host      MSSQL Server Hostname
     * @param  instance  MSSQL Instance Name
     * @param  port      MSSQL Port
     * @param  username  MSSQL Username
     * @param  password  MSSQL Password
     * @param  db        Default database
     * @throws SQLException
     */
    public void connect(String host, String instance, int port, String username, String password, String db) throws SQLException {
        String url = "";
        try {
            Class.forName("net.sourceforge.jtds.jdbc.Driver").newInstance();
            url = "jdbc:jtds:sqlserver://" + host;
            //Database supplied?
            if (db != null) {
                url += "/" + db;
            }
            //Port number supplied?
            if (port != 0) {
                url += ":" + port;
            }
            //Instance name supplied?
            if (instance != null) {
                url += ";instance=" + instance;
            }
            //Username supplied?
            if (username != null) {
                url += ";user=" + username;
                //Password required if username supplied
                if (password != null) {
                    url += ";password=" + password;
                }
            }

            conn = DriverManager.getConnection(url);
            databaseMetaData = conn.getMetaData();
            databaseProductVersion = databaseMetaData.getDatabaseProductVersion();
            databaseProductMajorVersion = databaseMetaData.getDatabaseMajorVersion();
            databaseProductMinorVersion = databaseMetaData.getDatabaseMinorVersion();
            hostname = host;
            schema = db;
            if (verbose) {
                System.out.println("Connection established to server " + host + " - Version: " + databaseProductVersion);
            }
        }
        catch (SQLException se) {
            if (verbose) {
                System.err.println(se.getMessage());
            }
            throw se;
        }
        catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    public String getVersion(){
        return properties.getProperty("application.version");
    }

    /**
     * Connect to MSSQL server
     *
     * @param  host      MSSQL Server Hostname
     * @param  username  MSSQL Username
     * @param  password  MSSQL Password
     * @param  db        Default database
     * @throws SQLException
     */
    public void connect(String host, String username, String password, String db) throws SQLException {
        connect(host, null, 0, username, password, db);
    }

    /**
     * Connect to MSSQL server
     *
     * @param  host      MSSQL Server Hostname
     * @param  username  MSSQL Username
     * @param  password  MSSQL Password
     * @throws SQLException
     */
    public void connect(String host, String username, String password) throws SQLException {
        connect(host, null, 0, username, password, null);
    }

    /**
     * Main entry point for MSSQLInstance when run from command line
     *
     * @param  args  Command line arguments
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        new MSSQLInstance().doMain(args);
    }

    /**
     * Parse command line arguments and run MSSQLInstance
     *
     * @param  args  Command line arguments
     * @throws IOException
     */
    public void doMain(String[] args) throws IOException {
        String usage = "Usage: java -jar MSSQLInstance.jar [OPTIONS] database [tables]\nOR     java -jar MSSQLInstance.jar [OPTIONS] --databases [OPTIONS] DB1 [DB2 DB3...]\nOR     java -jar MSSQLInstance.jar [OPTIONS] --all-databases [OPTIONS]\nFor more options, use java -jar MSSQLInstance.jar --help";
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
                System.err.println("MSSQLInstance.java Ver " + getVersion() + "\nThis software comes with ABSOLUTELY NO WARRANTY. This is free software,\nand you are welcome to modify and redistribute it under the BSD license" + "\n\n" + usage);
                return;
            }
            // if there's a problem in the command line,
            // you'll get this exception. this will report
            // an error message.
            System.err.println(e.getMessage());
            // print usage.
            System.err.println(usage);
            return;
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
            System.out.println(this.dumpCreateTable("ReportServer", "dbo", "Catalog"));
            //out.write(getHeader());
            for (String arg : arguments) {
                System.out.println(arg);
                //out.write(dumpCreateTable(arg));
                //this.dumpTable(out,arg);
            }
            out.flush();
            out.close();
            this.cleanup();
        }
        catch (SQLException se) {
            System.err.println(se.getMessage());
        }
    }

    /**
     * List Databases
     *
     * @return List of databases
     */
    public ArrayList<String> listDatabases() {
        ArrayList<String> databases = new ArrayList();
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SELECT name FROM sys.databases");
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                databases.add(rs.getString(1));
            }
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        if (verbose) {
            System.out.println(databases);
        }
        return databases;
    }

    /**
     * List Tables
     *
     * @param  db  Database Name
     * @return List of tables for selected database
     */
    public ArrayList<String> listTables(String db) {
        ArrayList<String> tables = new ArrayList();
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SELECT name FROM [" + db + "].sys.tables");
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                tables.add(rs.getString(1));
            }
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        if (verbose) {
            System.out.println(tables);
        }
        return tables;
    }

    /**
     * List Views
     *
     * @param  db  Database Name
     * @return List of tables for selected database
     */
    public ArrayList<String> listViews(String db) {
        ArrayList<String> views = new ArrayList();
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SELECT name FROM [" + db + "].sys.views");
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                views.add(rs.getString(1));
            }
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        if (verbose) {
            System.out.println(views);
        }
        return views;
    }

    public String dumpCreateTable(String database, String schema, String table) {
        String createTable = "--\n-- Table structure for table `" + table + "`\n--\n\n";
        try {
            conn.setCatalog(database);
            PreparedStatement s = conn.prepareStatement(streamToString(getClass().getResourceAsStream("dumpCreateTable.sql")));
            //Schema
            s.setString(2, schema);
            //Table
            s.setString(1, table);
            ResultSet rs = s.executeQuery();
            while (rs.next()) {
                createTable += rs.getString("TextFileGroup") + ";";
            }
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            createTable = "";
        }
        return createTable;
    }

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
     *
     * @return
     */
    public int cleanup() {
        try {
            conn.close();
            if (verbose) {
                System.out.println("Database connection terminated");
            }
        }
        catch (Exception e) { /* ignore close errors */ }
        return 0;
    }
}
