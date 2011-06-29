/**
 * Copyright (c) 2008-2011 Wave2 Limited. All rights reserved.
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
package org.dbinterrogator.postgresql;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.File;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import org.kohsuke.args4j.*;

/**
 *
 * @author Alan Snelson
 */
public class PostgreSQLInstance {

    //Command Line Arguments
    @Option(name = "--help")
    private boolean help;
    @Option(name = "-h", usage = "PostgreSQL Server Hostname")
    private String hostname;
    @Option(name = "-p", usage = "PostgreSQL Server Port Number")
    private int port = 5432;
    @Option(name = "-U", usage = "PostgreSQL Username")
    private String username;
    @Option(name = "-W", usage = "PostgreSQL Password")
    private String password;
    @Option(name = "-v")
    private String database;
    //TODO remove prior to release
    private boolean verbose = true;
    // receives other command line parameters than options
    @Argument
    private List<String> arguments = new ArrayList<String>();
    private static String version = "0.1";
    private String schema = "public";
    private Connection conn = null;
    private DatabaseMetaData databaseMetaData;
    private String databaseProductVersion = null;
    private int databaseProductMajorVersion = 0;
    private int databaseProductMinorVersion = 0;
    private String posgresqlVersion = null;
    //Privilege codes
    private static final HashMap<String, String> pCodes = new HashMap<String, String>() {
        {
            put("a", "INSERT");
            put("c", "CONNECT");
            put("C", "CREATE");
            put("d", "DELETE");
            put("D", "TRUNCATE");
            put("r", "SELECT");
            put("t", "TRIGGER");
            put("T", "TEMPORARY");
            put("U", "USAGE");
            put("w", "UPDATE");
            put("x", "REFERENCES");
            put("X", "EXECUTE");
        }
    };

    /**
     * Default contructor for PostgreSQLInstance.
     */
    public PostgreSQLInstance() {
    }

    /**
     * Create a new instance of PostgreSQLInstance using default database (postgres).
     *
     * @param  host      PostgreSQL Server Hostname
     * @param  username  PostgreSQL Username
     * @param  password  PostgreSQL Password
     * @throws SQLException
     */
    public PostgreSQLInstance(String host, String username, String password) throws SQLException {
        try {
            connect(host, username, password, "postgres");
            this.hostname = host;
            this.username = username;
            this.password = password;
            if (System.getenv("PGDATABASE") != null){
                this.database = System.getenv("PGDATABASE");
            } else {
                this.database = "postgres";
            }
        } catch (SQLException se) {
            throw se;
        }

    }

    /**
     * Create a new instance of PostgreSQLInstance using supplied database.
     *
     * @param  host      PostgreSQL Server Hostname
     * @param  username  PostgreSQL Username
     * @param  password  PostgreSQL Password
     * @param  db        Default database
     * @throws SQLException
     */
    public PostgreSQLInstance(String host, String username, String password, String db) throws SQLException {
        try {
            connect(host, username, password, db);
            this.hostname = host;
            this.username = username;
            this.password = password;
            this.database = db;
        } catch (SQLException se) {
            throw se;
        }
    }

    /**
     * Connect to PostgreSQL server
     *
     * @param  host      PostgreSQL Server Hostname
     * @param port
     * @param  username  PostgreSQL Username
     * @param  password  PostgreSQL Password
     * @param  db        Default database
     * @throws SQLException
     */
    public void connect(String host, int port, String username, String password, String db) throws SQLException {
        try {
            Class.forName("org.postgresql.Driver").newInstance();
            conn = DriverManager.getConnection("jdbc:postgresql://" + host + ":" + port + "/" + db, username, password);
            databaseMetaData = conn.getMetaData();
            databaseProductVersion = databaseMetaData.getDatabaseProductVersion();
            databaseProductMajorVersion = databaseMetaData.getDatabaseMajorVersion();
            databaseProductMinorVersion = databaseMetaData.getDatabaseMinorVersion();
            this.hostname = host;
            this.port = port;
            this.username = username;
            this.password = password;
            this.database = db;
            if (verbose) {
                System.out.println("PostgreSQL " + databaseProductMajorVersion + "." + databaseProductMinorVersion + " Database connection established");
            }
        } catch (SQLException se) {
            throw se;
        } catch (Exception e) {
            System.err.println("Cannot connect to database server");
        }
    }

    /**
     * Connect to PostgreSQL server using default port
     *
     * @param  host      PostgreSQL Server Hostname
     * @param  username  PostgreSQL Username
     * @param  password  PostgreSQL Password
     * @param  db        Default database
     * @throws SQLException
     */
    public void connect(String host, String username, String password, String db) throws SQLException {
        connect(host, 5432, username, password, db);
    }

    /**
     * Parse permissions string and generate GRANT statements
     *
     * @param  acl      PostgreSQL acl string
     * @param  name     PostgreSQL name of object for grant statements
     * @param  type     PostgreSQL type of object for grant statements
     * @return
     */
    public static String parseACL(String acl, String name, String type) {
        String aclCommands = "";
        String[] acls = acl.substring(1, acl.length() - 1).split(",");
        HashMap<String, ArrayList<String>> withGrant = new HashMap();
        HashMap<String, ArrayList<String>> withoutGrant = new HashMap();
        //Function names require braces
        if (type.equals("FUNCTION")) {
            name = "\"" + name + "\"()";
        } else {
            name = "\"" + name + "\"";
        }
        for (String priv : acls) {
            String user = priv.split("=")[0];
            String privs = priv.split("=")[1].split("/")[0];
            //Public Role?
            if (user.equals("")) {
                user = "public";
            }
            //Schema privileges GRANT ALL?
            if (type.equals("DATABASE") && privs.contains("C") && privs.contains("T") && privs.contains("c")) {
                if (privs.contains("*")) {
                    aclCommands += "GRANT ALL ON DATABASE " + name + " TO " + user + " WITH GRANT OPTION;\n";
                } else {
                    aclCommands += "GRANT ALL ON DATABASE " + name + " TO " + user + ";\n";
                }
            } else if (type.equals("SCHEMA") && privs.contains("U") && privs.contains("C")) {
                if (privs.contains("*")) {
                    aclCommands += "GRANT ALL ON SCHEMA " + name + " TO " + user + " WITH GRANT OPTION;\n";
                } else {
                    aclCommands += "GRANT ALL ON SCHEMA " + name + " TO " + user + ";\n";
                }
            } else if (type.equals("SEQUENCE") && privs.contains("r") && privs.contains("w") && privs.contains("U")) {
                if (privs.contains("*")) {
                    aclCommands += "GRANT ALL ON SEQUENCE " + name + " TO " + user + " WITH GRANT OPTION;\n";
                } else {
                    aclCommands += "GRANT ALL ON SEQUENCE " + name + " TO " + user + ";\n";
                }
            } else if (type.equals("TABLE") && privs.contains("a") && privs.contains("r") && privs.contains("w") && privs.contains("d") && privs.contains("x") && privs.contains("t")) {
                if (privs.contains("*")) {
                    aclCommands += "GRANT ALL ON TABLE " + name + " TO " + user + " WITH GRANT OPTION;\n";
                } else {
                    aclCommands += "GRANT ALL ON TABLE " + name + " TO " + user + ";\n";
                }
            } else {
                //Determine privileges and check if GRANT OPTION set.
                for (char c : privs.toCharArray()) {
                    if (pCodes.containsKey(Character.toString(c))) {
                        //Is this the last character?
                        if (privs.indexOf(c) != privs.length() - 1) {
                            //Is the next character a GRANT
                            if ('*' == privs.charAt(privs.indexOf(c) + 1)) {
                                if (withGrant.containsKey(user)) {
                                    ArrayList userPrivs = withGrant.get(user);
                                    userPrivs.add(pCodes.get(Character.toString(c)));
                                    withGrant.put(user, userPrivs);
                                } else {
                                    ArrayList userPrivs = new ArrayList();
                                    userPrivs.add(pCodes.get(Character.toString(c)));
                                    withGrant.put(user, userPrivs);
                                }
                            } else {
                                if (withoutGrant.containsKey(user)) {
                                    ArrayList userPrivs = withoutGrant.get(user);
                                    userPrivs.add(pCodes.get(Character.toString(c)));
                                    withoutGrant.put(user, userPrivs);
                                } else {
                                    ArrayList userPrivs = new ArrayList();
                                    userPrivs.add(pCodes.get(Character.toString(c)));
                                    withoutGrant.put(user, userPrivs);
                                }
                            }
                        } else {
                            if (withoutGrant.containsKey(user)) {
                                ArrayList userPrivs = withoutGrant.get(user);
                                userPrivs.add(pCodes.get(Character.toString(c)));
                                withoutGrant.put(user, userPrivs);
                            } else {
                                ArrayList userPrivs = new ArrayList();
                                userPrivs.add(pCodes.get(Character.toString(c)));
                                withoutGrant.put(user, userPrivs);
                            }
                        }
                    }
                }
            }
        }
        //create GRANT statements without GRANT OPTION
        for (String user : withoutGrant.keySet()) {
            aclCommands += "GRANT ";
            for (String priv : withoutGrant.get(user)) {
                if (withoutGrant.get(user).get(withoutGrant.get(user).size() - 1).equals(priv)) {
                    aclCommands += priv;
                } else {
                    aclCommands += priv + ", ";
                }
            }
            aclCommands += " ON " + type + " " + name + " TO \"" + user + "\";\n";
        }
        //create GRANT statements with GRANT OPTION
        for (String user : withGrant.keySet()) {
            aclCommands += "GRANT ";
            for (String priv : withGrant.get(user)) {
                if (withGrant.get(user).get(withGrant.get(user).size() - 1).equals(priv)) {
                    aclCommands += priv;
                } else {
                    aclCommands += priv + ", ";
                }
            }
            aclCommands += " ON " + type + " " + name + " TO \"" + user + "\" WITH GRANT OPTION;\n";
        }
        return aclCommands;
    }

    /**
     * Obtain server parameters
     *
     * @return parameters
     */
    public Map<String, String> dumpServerParameters() {
        Map<String, String> parameters = new TreeMap();
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SELECT name, setting, vartype FROM pg_catalog.pg_settings");
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                if (rs.getString("vartype").equals("string")) {
                    parameters.put(rs.getString("name"), "'" + rs.getString("setting") + "'");
                }
                parameters.put(rs.getString("name"), rs.getString("setting"));
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        if (verbose) {
            System.out.println(parameters.toString());
        }
        return parameters;
    }

    /**
     * Returns create statement for supplied role
     *
     * @param  role     PostgreSQL role name
     * @return
     */
    public String dumpCreateRole(String role) {
        String createRole = null;
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SELECT pg_catalog.pg_authid.oid, rolname, rolcanlogin, rolpassword, rolsuper, rolinherit, rolcreatedb, " +
                    "rolcreaterole, rolconnlimit, rolvaliduntil, rolcatupdate, description FROM pg_catalog.pg_authid " +
                    "LEFT OUTER JOIN pg_catalog.pg_shdescription " +
                    "ON pg_catalog.pg_authid.oid = pg_catalog.pg_shdescription.objoid WHERE rolname = '" + role + "';");
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                createRole = "CREATE ROLE " + role;
                if (rs.getBoolean("rolcanlogin") == true) {
                    createRole += " LOGIN";
                }
                if (rs.getString("rolpassword") != null) {
                    createRole += " ENCRYPTED PASSWORD '" + rs.getString("rolpassword") + "'";
                }
                //Superuser?
                if (rs.getBoolean("rolsuper") == true) {
                    createRole += " SUPERUSER";
                } else {
                    createRole += " NOSUPERUSER";
                }
                //Inherit?
                if (rs.getBoolean("rolinherit") == true) {
                    createRole += " INHERIT";
                } else {
                    createRole += " NOINHERIT";
                }
                //CreateDB?
                if (rs.getBoolean("rolcreatedb") == true) {
                    createRole += " CREATEDB";
                } else {
                    createRole += " NOCREATEDB";
                }
                //CreateRole?
                if (rs.getBoolean("rolcreaterole") == true) {
                    createRole += " CREATEROLE";
                } else {
                    createRole += " NOCREATEROLE";
                }
                //Connection Limit?
                if (rs.getInt("rolconnlimit") != -1) {
                    createRole += " CONNECTION LIMIT " + rs.getString("rolconnlimit");
                }
                //Valid Until?
                if (rs.getString("rolvaliduntil") != null && !rs.getString("rolvaliduntil").equals("infinity")) {
                    createRole += " VALID UNTIL '" + rs.getString("rolvaliduntil") + "'";
                }
                createRole += ";\n";
                //Update Catalog Direct?
                if (rs.getBoolean("rolsuper") == true) {
                    if (rs.getBoolean("rolcatupdate") == false) {
                        createRole += "UPDATE pg_catalog.pg_authid SET rolcatupdate=false WHERE rolname='" + role + "';\n";
                    }
                }
                //Role Membership?
                Statement rolestmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
                rolestmt.executeQuery("SELECT rolname FROM pg_auth_members JOIN pg_catalog.pg_authid ON pg_catalog.pg_auth_members.roleid = pg_catalog.pg_authid.oid WHERE member = " + rs.getString("oid"));
                ResultSet rolers = rolestmt.getResultSet();
                while (rolers.next()) {
                    createRole += "GRANT \"" + rolers.getString("rolname") + "\" TO " + role + ";\n";
                }
                if (rs.getString("description") != null) {
                    createRole += "COMMENT ON ROLE " + role + " IS '" + rs.getString("description") + "';\n";
                }
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        if (verbose) {
            System.out.println(createRole);
        }
        return createRole;
    }

    /**
     * Returns create statement for supplied database
     *
     * @param  database     PostgreSQL database name
     * @return createDatabase
     */
    public String dumpCreateDatabase(String database) {
        String createDatabase = null;
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            //PostgreSQL version 8.2+
            if (databaseProductMajorVersion == 8 & databaseProductMinorVersion >= 2) {
                s.executeQuery("SELECT tableoid, oid, (SELECT rolname FROM pg_catalog.pg_roles " +
                        "WHERE oid = datdba) AS dba, pg_encoding_to_char(encoding) AS encoding, " +
                        "NULL AS datcollate, NULL AS datctype, datfrozenxid, " +
                        "(SELECT spcname FROM pg_tablespace t WHERE t.oid = dattablespace) AS tablespace, " +
                        "datconnlimit, shobj_description(oid, 'pg_database') AS description, datacl FROM pg_database " +
                        "WHERE datname = '" + database + "';");
            }
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                createDatabase = "CREATE DATABASE " + database + " WITH OWNER = " + rs.getString("dba");
                if (rs.getString("encoding") != null) {
                    createDatabase += " ENCODING = '" + rs.getString("encoding") + "'";
                }
                if (rs.getString("datcollate") != null) {
                    createDatabase += " LC_COLLATE = '" + rs.getString("datcollate") + "'";
                }
                if (rs.getString("datctype") != null) {
                    createDatabase += " LC_CTYPE = '" + rs.getString("datctype") + "'";
                }
                if (!rs.getString("tablespace").equals("pg_default")) {
                    createDatabase += " TABLESPACE = " + rs.getString("tablespace");
                }
                createDatabase += " CONNECTION LIMIT = " + rs.getString("datconnlimit") + ";\n";
                if (rs.getString("datacl") != null) {
                    createDatabase += parseACL(rs.getString("datacl"), database, "DATABASE");
                }
                if (rs.getString("description") != null) {
                    createDatabase += "COMMENT ON DATABASE " + database + " IS '" + rs.getString("description") + "';\n";
                }
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        if (verbose) {
            System.out.println(createDatabase);
        }
        return createDatabase;
    }

    /**
     * Returns create statement for supplied schema
     *
     * @param  schema     PostgreSQL role name
     * @return createSchema
     */
    public String dumpCreateSchema(String schema) {
        String createSchema = null;
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SELECT nspacl, description, rolname FROM pg_catalog.pg_namespace LEFT OUTER JOIN pg_catalog.pg_description ON pg_catalog.pg_namespace.oid=pg_catalog.pg_description.objoid JOIN pg_catalog.pg_roles ON pg_catalog.pg_roles.oid=pg_catalog.pg_namespace.nspowner WHERE nspname = '" + schema + "'");
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                createSchema = "CREATE SCHEMA \"" + schema + "\" AUTHORIZATION " + rs.getString("rolname") + ";\n";
                if (rs.getString("nspacl") != null) {
                    createSchema += parseACL(rs.getString("nspacl"), schema, "SCHEMA");
                }
                if (rs.getString("description") != null) {
                    createSchema += "COMMENT ON SCHEMA \"" + schema + "\" IS '" + rs.getString("description") + "';\n";
                }
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        if (verbose) {
            System.out.println(createSchema);
        }
        return createSchema;
    }

    /**
     * Returns create statement for supplied domain
     *
     * @param  schema     PostgreSQL schema
     * @param  domain     PostgreSQL domain name
     * @return createDomain
     */
    public String dumpCreateDomain(String schema, String domain) {
        String createDomain = null;
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SELECT (SELECT rolname FROM pg_catalog.pg_roles WHERE oid = typowner) as typowner, typnotnull, pg_catalog.format_type(typbasetype, typtypmod) AS typdefn, " +
                    "pg_catalog.pg_get_expr(typdefaultbin, 'pg_catalog.pg_type'::pg_catalog.regclass) AS typdefaultbin, typdefault, description " +
                    "FROM pg_catalog.pg_type LEFT OUTER JOIN pg_catalog.pg_description ON pg_catalog.pg_description.objoid = pg_catalog.pg_type.oid " +
                    "WHERE pg_catalog.pg_type.oid = (SELECT oid FROM pg_catalog.pg_type WHERE typname='" + domain + "' AND typnamespace = (SELECT oid from pg_catalog.pg_namespace WHERE nspname = '" + schema + "'))::pg_catalog.oid");
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                createDomain = "CREATE DOMAIN \"" + domain + "\" AS " + rs.getString("typdefn");
                //Default?
                if (rs.getString("typdefaultbin") != null) {
                    createDomain += " DEFAULT " + rs.getString("typdefaultbin");
                }
                if (rs.getBoolean("typnotnull") == true) {
                    createDomain += " NOT NULL";
                }
                createDomain += ";\n";
                createDomain += "ALTER DOMAIN \"" + domain + "\" OWNER TO " + rs.getString("typowner") + ";\n";
                //Comment?
                if (rs.getString("description") != null) {
                    createDomain += "COMMENT ON DOMAIN \"" + domain + "\" IS '" + rs.getString("description") + "';\n";
                }
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        if (verbose) {
            System.out.println(createDomain);
        }
        return createDomain;
    }

    /**
     * Returns create statement for supplied sequence
     *
     * @param  schema       PostgreSQL schema
     * @param  sequence     PostgreSQL domain name
     * @return createSequence
     */
    public String dumpCreateSequence(String schema, String sequence) {
        String createSequence = null;
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SELECT increment_by, min_value, max_value, last_value, cache_value, " +
                    "is_cycled, relacl, (SELECT rolname FROM pg_catalog.pg_roles WHERE oid = relowner) as relowner, " +
                    "description FROM pg_catalog.pg_class LEFT OUTER JOIN pg_catalog.pg_description " +
                    "ON pg_catalog.pg_description.objoid = pg_catalog.pg_class.oid " +
                    "JOIN \"" + sequence + "\" ON pg_catalog.pg_class.relname=sequence_name " +
                    "WHERE relkind = 'S' AND relname = '" + sequence + "'");
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                createSequence = "CREATE SEQUENCE \"" + sequence + "\" ";
                createSequence += "INCREMENT " + rs.getString("increment_by") + " ";
                createSequence += "MINVALUE " + rs.getString("min_value") + " ";
                createSequence += "MAXVALUE " + rs.getString("max_value") + " ";
                createSequence += "START " + rs.getString("last_value") + " ";
                createSequence += "CACHE " + rs.getString("cache_value");
                if (rs.getBoolean("is_cycled") == true) {
                    createSequence += " CYCLE";
                }
                createSequence += ";\n";
                createSequence += "ALTER TABLE \"" + sequence + "\" OWNER TO " + rs.getString("relowner") + ";\n";
                //Privileges?
                if (rs.getString("relacl") != null) {
                    createSequence += parseACL(rs.getString("relacl"), schema, "SEQUENCE");
                }
                //Comment?
                if (rs.getString("description") != null) {
                    createSequence += "COMMENT ON SEQUENCE \"" + sequence + "\" IS '" + rs.getString("description") + "';\n";
                }
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        if (verbose) {
            System.out.println(createSequence);
        }
        return createSequence;
    }

    /**
     *
     * @return
     */
    public File dumpAllDatabases() {
        return null;
    }

    /**
     *
     * @param database
     * @return
     */
    public File dumpDatabase(String database) {
        return null;
    }

    /**
     *
     * @param database
     * @return
     */
    public File dumpAllTables(String database) {
        return null;
    }

    /**
     * Returns create statement for supplied table using property schema
     *
     * @param  table        PostgreSQL domain name
     * @return
     */
    public String dumpCreateTable(String table) {
        return dumpCreateTable(schema, table);
    }

    /**
     * Returns create statement for supplied table
     *
     * @param  schema       PostgreSQL schema
     * @param  table        PostgreSQL domain name
     * @return createTable
     */
    public String dumpCreateTable(String schema, String table) {
        String createTable = null;
        String comments = "";
        HashMap<Integer, String> columnNames = new HashMap();
        try {
            Statement tablestmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            tablestmt.executeQuery("SELECT pg_catalog.pg_class.oid, relname, (SELECT rolname FROM pg_catalog.pg_roles WHERE oid = relowner) AS rolname, " +
                    "relnatts, relhasoids, relchecks, reltriggers, " +
                    "relhaspkey, relacl, description, spcname FROM pg_catalog.pg_class " +
                    "LEFT OUTER JOIN pg_catalog.pg_tablespace ON pg_catalog.pg_class.reltablespace = pg_catalog.pg_tablespace.oid " +
                    "LEFT OUTER JOIN pg_catalog.pg_description ON pg_catalog.pg_description.objoid = pg_catalog.pg_class.oid " +
                    "WHERE relname = '" + table + "' AND relnamespace = (SELECT oid from pg_namespace " +
                    "WHERE nspname = '" + schema + "')::pg_catalog.oid AND relkind = 'r' AND objsubid = 0");
            ResultSet tablers = tablestmt.getResultSet();
            while (tablers.next()) {
                createTable = "CREATE TABLE \"" + table + "\"\n(\n";
                //Process Columns
                Statement columnstmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
                columnstmt.executeQuery("SELECT a.attnum, a.attname, a.atttypmod, a.attstattarget, a.attstorage, " +
                        "t.typstorage, a.attnotnull, a.atthasdef, a.attisdropped, a.attlen, a.attalign, " +
                        "a.attislocal, pg_catalog.format_type(t.oid,a.atttypmod) AS atttypname, defs.adsrc, description " +
                        "FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_type t " +
                        "ON a.atttypid = t.oid LEFT OUTER JOIN pg_catalog.pg_attrdef defs " +
                        "ON defs.adrelid = a.attrelid AND defs.adnum = a.attnum " +
                        "LEFT OUTER JOIN pg_catalog.pg_description ON pg_catalog.pg_description.objoid = " +
                        "(SELECT oid FROM pg_class WHERE relname = '" + table + "' AND relnamespace = " +
                        "(SELECT oid from pg_namespace WHERE nspname = '" + schema + "'))::pg_catalog.oid " +
                        "AND pg_catalog.pg_description.objsubid = a.attnum WHERE a.attrelid = " +
                        "(SELECT oid FROM pg_class WHERE relname = '" + table + "' AND " +
                        "relnamespace = (SELECT oid from pg_namespace " +
                        "WHERE nspname = '" + schema + "'))" + "::pg_catalog.oid " +
                        "AND a.attnum > 0::pg_catalog.int2 AND attisdropped = FALSE ORDER BY a.attrelid, a.attnum");
                ResultSet columnrs = columnstmt.getResultSet();
                while (columnrs.next()) {
                    columnNames.put(columnrs.getInt("attnum"), columnrs.getString("attname"));
                    createTable += "  \"" + columnrs.getString("attname") + "\" " + columnrs.getString("atttypname");
                    //NOT NULL?
                    if (columnrs.getBoolean("attnotnull") == true) {
                        createTable += " NOT NULL";
                    }
                    //Default?
                    if (columnrs.getBoolean("atthasdef") == true) {
                        createTable += " DEFAULT " + columnrs.getString("adsrc");
                    }
                    if (!columnrs.isLast() || tablers.getInt("relchecks") > 0) {
                        createTable += ",";
                    }
                    //Comment?
                    if (columnrs.getString("description") != null) {
                        createTable += " -- " + columnrs.getString("description") + "\n";
                        comments += "COMMENT ON COLUMN \"" + table + "\".\"" + columnrs.getString("attname") + "\" IS '" + columnrs.getString("description") + "';\n";
                    } else {
                        createTable += "\n";
                    }
                }
                //Constraints?
                Statement constraintstmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
                constraintstmt.executeQuery("SELECT conname, contype, conkey, consrc, spcname FROM pg_catalog.pg_constraint " +
                        "LEFT OUTER JOIN pg_catalog.pg_class ON pg_catalog.pg_constraint.conname = pg_catalog.pg_class.relname " +
                        "LEFT OUTER JOIN pg_catalog.pg_tablespace ON  pg_catalog.pg_class.reltablespace = pg_catalog.pg_tablespace.oid " +
                        "WHERE connamespace = (SELECT oid from pg_namespace " +
                        "WHERE nspname = 'public')::pg_catalog.oid AND conrelid = " +
                        tablers.getString("oid"));
                ResultSet constraintrs = constraintstmt.getResultSet();
                while (constraintrs.next()) {
                    createTable += "  CONSTRAINT \"" + constraintrs.getString("conname") + "\"";
                    //Primary Key?
                    if (constraintrs.getString("contype").equals("p")) {
                        createTable += " PRIMARY KEY (";
                        Integer[] columns = (Integer[]) constraintrs.getArray("conkey").getArray();
                        for (int i = 0; i < columns.length; i++) {
                            createTable += "\"" + columnNames.get(columns[i]) + "\"";
                            if (i < columns.length - 1) {
                                createTable += ",";
                            } else {
                                createTable += ")";
                            }
                        }
                        if (constraintrs.getString("spcname") != null) {
                            createTable += " USING INDEX TABLESPACE \"" + constraintrs.getString("spcname") + "\"";
                        }
                    //Unique Constraint?
                    } else if (constraintrs.getString("contype").equals("u")) {
                        createTable += " UNIQUE (";
                        Integer[] columns = (Integer[]) constraintrs.getArray("conkey").getArray();
                        for (int i = 0; i < columns.length; i++) {
                            createTable += "\"" + columnNames.get(columns[i]) + "\"";
                            if (i < columns.length - 1) {
                                createTable += ",";
                            } else {
                                createTable += ")";
                            }
                        }
                        if (constraintrs.getString("spcname") != null) {
                            createTable += " USING INDEX TABLESPACE \"" + constraintrs.getString("spcname") + "\"";
                        }
                    //Check Constraints?
                    } else if (constraintrs.getString("contype").equals("c")) {
                        createTable += " CHECK " + constraintrs.getString("consrc");
                    }
                    if (!constraintrs.isLast()) {
                        createTable += ",\n";
                    } else {
                        createTable += "\n";
                    }
                }

                createTable += ")\n";
                //Table has OIDs?
                if (tablers.getBoolean("relhasoids") == true) {
                    createTable += "WITH ( OIDS=TRUE )";
                } else {
                    createTable += "WITH ( OIDS=TRUE )";
                }
                //Non default tablespace?
                if (tablers.getString("spcname") != null) {
                    createTable += " TABLESPACE \"" + tablers.getString("spcname") + "\"";
                }
                createTable += ";\n";
                //Table Owner
                createTable += "ALTER TABLE \"" + table + "\" OWNER TO " + tablers.getString("rolname") + ";\n";
                //Privileges?
                if (tablers.getString("relacl") != null) {
                    createTable += parseACL(tablers.getString("relacl"), table, "TABLE");
                }
                //Table Comment?
                if (tablers.getString("description") != null) {
                    createTable += "COMMENT ON TABLE \"" + table + "\" IS '" + tablers.getString("description") + "';\n";
                }
                //Comments?
                if (!comments.equals("")) {
                    createTable += comments;
                }
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        if (verbose) {
            System.out.println(createTable);
        }
        return createTable;
    }

    /**
     * Returns create statement for supplied function
     *
     * @param  schema    PostgreSQL Schema
     * @param  function  PostgreSQL Function
     * @return createFunction
     */
    public String dumpCreateFunction(String schema, String function) {
        String createFunction = null;
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SELECT (SELECT rolname FROM pg_catalog.pg_roles WHERE oid = proowner) as owner, pg_catalog.format_type(prorettype::pg_catalog.oid,NULL) as prorettype, proallargtypes, proargmodes, proargnames, prosrc, lanname, provolatile, proisstrict, " +
                    "prosecdef, procost, proretset, prorows, proacl, description FROM pg_catalog.pg_proc " +
                    "LEFT OUTER JOIN pg_catalog.pg_description ON pg_catalog.pg_proc.oid=pg_catalog.pg_description.objoid " +
                    "JOIN pg_language ON pg_catalog.pg_proc.prolang=pg_catalog.pg_language.oid " +
                    "WHERE pronamespace = (SELECT oid from pg_namespace WHERE nspname = '" + schema + "')::pg_catalog.oid " +
                    "AND proname = '" + function + "'");
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                createFunction = "CREATE OR REPLACE FUNCTION \"" + function + "\"(";
                //"SELECT pg_catalog.format_type('%u'::pg_catalog.oid, NULL)",
                if (rs.getArray("proargnames") != null) {
                    Statement t = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
                    String[] argnames = (String[]) rs.getArray("proargnames").getArray();
                    Long[] argtypes = (Long[]) rs.getArray("proallargtypes").getArray();
                    String[] argmodes = (String[]) rs.getArray("proargmodes").getArray();
                    for (int i = 0; i < argnames.length; i++) {
                        //IN or OUT or INOUT
                        if (argmodes[i].equals("i")) {
                            createFunction += "IN \"" + argnames[i] + "\" ";
                        } else if (argmodes[i].equals("o")) {
                            createFunction += "OUT \"" + argnames[i] + "\" ";
                        }
                        t.executeQuery("SELECT pg_catalog.format_type('" + argtypes[i] + "'::pg_catalog.oid, NULL) as typename");
                        ResultSet trs = t.getResultSet();
                        while (trs.next()) {
                            createFunction += trs.getString("typename");
                            if (i != argnames.length - 1) {
                                createFunction += ", ";
                            }
                        }
                    }
                }
                createFunction += ") RETURNS ";
                if (rs.getBoolean("proretset")) {
                    createFunction += "SETOF ";
                }
                createFunction += rs.getString("prorettype") + " AS\n";
                //Single line or multi-line?
                if (rs.getString("prosrc").contains("\n")) {
                    createFunction += "$$" + rs.getString("prosrc") + "$$\n";
                } else {
                    createFunction += "'" + rs.getString("prosrc") + "'\n";
                }
                createFunction += "LANGUAGE '" + rs.getString("lanname") + "' ";
                if (rs.getString("provolatile").equals("s")) {
                    createFunction += "STABLE ";
                }
                if (rs.getString("provolatile").equals("v")) {
                    createFunction += "VOLATILE ";
                }
                if (rs.getString("provolatile").equals("i")) {
                    createFunction += "IMMUTABLE ";
                }
                if (rs.getBoolean("proisstrict")) {
                    createFunction += "STRICT ";
                }
                if (rs.getBoolean("prosecdef")) {
                    createFunction += "SECURITY DEFINER ";
                }
                if (rs.getBoolean("proretset")) {
                    createFunction += "COST " + rs.getString("procost") + " ROWS " + rs.getString("prorows") + ";\n";
                } else {
                    createFunction += "COST " + rs.getString("procost") + ";\n";
                }
                createFunction += "ALTER FUNCTION \"" + function + "\"() OWNER TO " + rs.getString("owner") + ";\n";
                if (rs.getString("proacl") != null) {
                    createFunction += parseACL(rs.getString("proacl"), function, "FUNCTION");
                }
                if (rs.getString("description") != null) {
                    createFunction += "COMMENT ON FUNCTION \"" + function + "\"() IS '" + rs.getString("description") + "';\n";
                }
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        if (verbose) {
            System.out.println(createFunction);
        }
        return createFunction;
    }

    /**
     * Returns create statement for supplied trigger
     *
     * @param schema
     * @param trigger
     * @return createTrigger
     */
    public String dumpCreateTrigger(String schema, String table, String trigger) {
        String createTrigger = null;
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SELECT tgname, tgfoid::pg_catalog.regproc AS tgfname, tgtype, " +
                    "tgnargs, tgargs, tgenabled, tgisconstraint, tgconstrname, tgdeferrable, " +
                    "tgconstrrelid, tginitdeferred, t.tableoid, t.oid, " +
                    "tgconstrrelid::pg_catalog.regclass AS tgconstrrelname, description " +
                    "FROM pg_catalog.pg_trigger t " +
                    "LEFT OUTER JOIN pg_catalog.pg_description ON t.oid=pg_catalog.pg_description.objoid " +
                    "WHERE tgrelid = (SELECT oid FROM pg_class WHERE relname = '" + table + "' " +
                    "AND relnamespace = (SELECT oid from pg_namespace " +
                    "WHERE nspname = '" + schema + "'))::pg_catalog.oid AND tgconstraint = 0 " +
                    "AND tgname = '" + trigger + "'");
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                createTrigger = "CREATE TRIGGER \"" + trigger + "\"\n";
                // Trigger type
                int findx = 0;
                if (((rs.getInt("tgtype")) & (1 << 1)) != 0) {
                    createTrigger += "  BEFORE";
                } else {
                    createTrigger += "  AFTER";
                }
                if (((rs.getInt("tgtype")) & (1 << 2)) != 0) {
                    createTrigger += " INSERT";
                    findx++;
                }
                if (((rs.getInt("tgtype")) & (1 << 3)) != 0) {
                    if (findx > 0) {
                        createTrigger += " OR DELETE";
                    } else {
                        createTrigger += " DELETE";
                    }
                    findx++;
                }
                if (((rs.getInt("tgtype")) & (1 << 4)) != 0) {
                    if (findx > 0) {
                        createTrigger += " OR UPDATE";
                    } else {
                        createTrigger += " UPDATE";
                    }
                }
                if (((rs.getInt("tgtype")) & (1 << 5)) != 0) {
                    if (findx > 0) {
                        createTrigger += " OR TRUNCATE";
                    } else {
                        createTrigger += " TRUNCATE";
                    }
                }
                createTrigger += "\n  ON \"" + table + "\"\n";
                if (((rs.getInt("tgtype")) & (1 << 1)) != 0) {
                    createTrigger += "  FOR EACH ROW\n";
                } else {
                    createTrigger += "  FOR EACH STATEMENT\n";
                }
                createTrigger += "  EXECUTE PROCEDURE " + rs.getString("tgfname") + "";
                //Do we have function arguments?
                if (rs.getInt("tgnargs") > 0){
                    createTrigger += "(" + rs.getString("tgargs").substring(0, rs.getString("tgargs").length()-4).replace("\000", ",") + ");";
                } else {
                    createTrigger += "();";
                }
                if (rs.getString("description") != null) {
                    createTrigger += "\nCOMMENT ON TRIGGER \"" + trigger + "\" ON \"" + table + "\" IS '" + rs.getString("description") + "';";
                }

            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            createTrigger = null;
        }
        if (verbose) {
            System.out.println(createTrigger);
        }
        return createTrigger;
    }

    /**
     *
     * @param out
     * @param table
     */
    public void dumpTable(BufferedWriter out, String table) {
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SELECT /*!40001 SQL_NO_CACHE */ * FROM " + table);
            ResultSet rs = s.getResultSet();
            ResultSetMetaData rsMetaData = rs.getMetaData();
            if (rs.last()) {
                out.write("--\n-- Dumping data for table `" + table + "`\n--\n\n");
                rs.first();
            }
            int columnCount = rsMetaData.getColumnCount();
            String prefix = new String("INSERT INTO " + table + " (");
            for (int i = 1; i <= columnCount; i++) {
                if (i == columnCount) {
                    prefix += rsMetaData.getColumnName(i) + ") VALUES(";
                } else {
                    prefix += rsMetaData.getColumnName(i) + ",";
                }
            }
            String postfix = new String();
            int count = 0;
            while (rs.next()) {
                postfix = "";
                for (int i = 1; i <= columnCount; i++) {
                    if (i == columnCount) {
                        //System.err.println(rs.getMetaData().getColumnClassName(i));
                        postfix += "'" + rs.getString(i) + "');\n";
                    } else {
                        //System.err.println(rs.getMetaData().getColumnTypeName(i));
                        if (rs.getMetaData().getColumnTypeName(i).equalsIgnoreCase("LONGBLOB")) {
                            try {
                                postfix += "'" + escapeString(rs.getBytes(i)).toString() + "',";
                            } catch (Exception e) {
                                postfix += "NULL,";
                            }
                        } else {
                            try {
                                postfix += "'" + escapeString(rs.getBytes(i)).toString() + "',";
                            } catch (Exception e) {
                                postfix += "NULL,";
                            }
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

    /**
     *
     * @param database
     * @return
     */
    public File dumpAllViews(String database) {
        return null;
    }

    /**
     *
     * @param view
     * @return
     */
    public String dumpCreateView(String view) {
        return null;
    }

    /**
     *
     * @param view
     * @return
     */
    public File dumpView(String view) {
        return null;
    }

    /**
     * Return a list of databases managed by this instance
     *
     * @return databases
     */
    public ArrayList<String> listDatabases() {
        ArrayList<String> databases = new ArrayList();
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SELECT * FROM pg_database WHERE datistemplate=false;");
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                databases.add(rs.getString("datname"));
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return databases;
    }

    /**
     * Return a list of schemas within the connected database
     *
     * @return schemas
     */
    public ArrayList<String> listSchemas() {
        ArrayList<String> schemas = new ArrayList();
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SELECT nspname FROM pg_catalog.pg_namespace WHERE " +
                    "SUBSTRING(nspname FROM 1 FOR 3) != 'pg_' AND " +
                    "nspname != 'information_schema'");
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                schemas.add(rs.getString("nspname"));
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        if (verbose) {
            System.out.println(schemas.toString());
        }
        return schemas;
    }

    /**
     * Return a list of user defined functions within the supplied schema
     *
     * @param schema
     * @return functions
     */
    public ArrayList<String> listFunctions(String schema) {
        ArrayList<String> functions = new ArrayList();
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SELECT tableoid, oid, proname, prolang, pronargs, proargtypes, prorettype, " +
                    "proacl, pronamespace FROM pg_proc WHERE NOT proisagg AND " +
                    "pronamespace != (SELECT oid FROM pg_namespace WHERE nspname = 'pg_catalog') AND " +
                    "pronamespace = (SELECT oid from pg_namespace WHERE nspname = '" + schema + "')::pg_catalog.oid");
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                functions.add(rs.getString("proname"));
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        if (verbose) {
            System.out.println(functions.toString());
        }
        return functions;
    }

    /**
     * Returns a list of triggers associated with the supplied schema / table
     *
     * @param table
     * @param schema
     * @return triggers
     */
    public ArrayList<String> listTriggers(String table, String schema) {
        ArrayList<String> triggers = new ArrayList();
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SELECT tgname, tgfoid::pg_catalog.regproc AS tgfname, tgtype, tgnargs, tgargs, tgenabled, " +
                    "tgisconstraint, tgconstrname, tgdeferrable, tgconstrrelid, tginitdeferred, tableoid, oid, " +
                    "tgconstrrelid::pg_catalog.regclass AS tgconstrrelname FROM pg_catalog.pg_trigger t " +
                    "WHERE tgrelid = (SELECT oid FROM pg_class WHERE relname = '" + table + "' AND relnamespace = (SELECT oid from pg_namespace " +
                    "WHERE nspname = '" + schema + "'))::pg_catalog.oid::pg_catalog.oid AND tgconstraint = 0");
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                triggers.add(rs.getString(1));
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        if (verbose) {
            System.out.println(triggers.toString());
        }
        return triggers;
    }

    /**
     * Returns a list of tables within the supplied schema
     *
     * @param schema
     * @return tables
     */
    public ArrayList<String> listTables(String schema) {
        ArrayList<String> tables = new ArrayList();
        try {
            Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            s.executeQuery("SELECT relname FROM pg_class WHERE relkind = 'r' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '" + schema + "')::pg_catalog.oid");
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                tables.add(rs.getString(1));
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        if (verbose) {
            System.out.println(tables.toString());
        }
        return tables;
    }

     /**
     *
     * @return
     */
    public String getDatabase() {
        return database;
    }

    /**
     * Switch Database - requires existing econnection.
     *
     * @param schema
     */
    public void setDatabase(String database) {
        try{
            conn.close();
            connect(this.hostname, this.port, this.username, this.password, database);
            this.database = database;
        } catch (SQLException se) {
            System.err.println(se.getMessage());
        }
    }

    /**
     * Return current Schema
     *
     * @return schema
     */
    public String getSchema() {
        return schema;
    }

    /**
     * Set schema name
     *
     * @param schema
     */
    public void setSchema(String schema) {
        this.schema = schema;
    }

    /**
     * Escape string ready for insert via pgsql client
     *
     * @param  bIn       String to be escaped passed in as byte array
     * @return bOut      PostgreSQL compatible insert ready ByteArrayOutputStream
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

    private String getHeader() {
        //return Dump Header
        return "-- BinaryStor PostgreSQL Dump " + version + "\n--\n-- Host: " + hostname + "    " + "Database: " + schema + "\n-- ------------------------------------------------------\n-- Server Version: " + databaseProductVersion + "\n--";
    }

    /**
     * Main entry point for PostgreSQLInstance when run from command line
     *
     * @param  args  Command line arguments
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        new PostgreSQLInstance().doMain(args);
    }

    /**
     * Parse command line arguments and run PostgreSQLInstance
     *
     * @param  args  Command line arguments
     * @throws IOException
     */
    public void doMain(String[] args) throws IOException {

        String usage = "Usage: java -jar PostgreSQLInstance.jar [OPTIONS] database [tables]\nOR     java -jar PostgreSQLInstance.jar [OPTIONS] --databases [OPTIONS] DB1 [DB2 DB3...]\nOR     java -jar PostgreSQLInstance.jar [OPTIONS] --all-databases [OPTIONS]\nFor more options, use java -jar PostgreSQLInstance.jar --help";
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

        } catch (CmdLineException e) {
            if (e.getMessage().equalsIgnoreCase("Print Help")) {
                System.err.println("PostgreSQLInstance.java Ver " + version + "\nThis software comes with ABSOLUTELY NO WARRANTY. This is free software,\nand you are welcome to modify and redistribute it under the BSD license" + "\n\n" + usage);
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
            out.write(getHeader());
            for (String arg : arguments) {
                System.out.println(arg);
                out.write(dumpCreateTable(arg));
                this.dumpTable(out, arg);
            }
            out.flush();
            out.close();
            this.cleanup();
        } catch (SQLException se) {
            System.err.println(se.getMessage());
        }
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
        } catch (Exception e) { /* ignore close errors */ }
        return 1;
    }
}
