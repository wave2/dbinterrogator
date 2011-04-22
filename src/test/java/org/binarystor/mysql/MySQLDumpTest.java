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
package org.binarystor.mysql;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.sql.SQLException;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.matchers.JUnitMatchers;
import static org.junit.Assert.*;

/**
 *
 * @author Alan Snelson
 */
public class MySQLDumpTest {

    private final static Properties appProperties = new Properties();
    private final static Properties testProperties = new Properties();
    private static String hostname;
    private static int port;
    private static String username;
    private static String password;
    private static String schema;

    public MySQLDumpTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        try {
            appProperties.load(MySQLDump.class.getResourceAsStream("/application.properties"));
            testProperties.load(MySQLDumpTest.class.getResourceAsStream("/test.properties"));
            hostname = testProperties.getProperty("test.hostname");
            port = Integer.parseInt(testProperties.getProperty("test.port"));
            username = testProperties.getProperty("test.username");
            password = testProperties.getProperty("test.password");
            schema = testProperties.getProperty("test.schema");
            //Create test database
            MySQLDump instance = new MySQLDump(hostname, username, password);
            instance.createSchema(schema);
            instance.setSchema(schema);
            BufferedReader scriptContents = new BufferedReader(new InputStreamReader(MySQLDump.class.getResourceAsStream("createTestDatabaseObjects.sql"), "UTF-8"));
            instance.executeScript(schema, scriptContents);
            instance.cleanup();
        }
        catch (IOException e) {
            System.err.println(e.getMessage());
        }
        catch (SQLException sqle){
            System.err.println(sqle.getMessage());
        }
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
            //Drop test database
            MySQLDump instance = new MySQLDump(hostname, username, password);
            instance.dropSchema(schema);
            instance.cleanup();
    }
    
    

    @Before
    public void setUp() {

    }

    @After
    public void tearDown() {
    }

    /**
     * Test connection using hostname, port, username, password and database
     */
    @Test
    public void testConnect_5args() throws Exception {
        System.out.println("connect");
        MySQLDump instance = new MySQLDump();
        instance.connect(hostname, port, username, password, schema);
        instance.cleanup();
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
     * Test connection using hostname, username, password and db
     */
    @Test
    public void testConnect_4args() throws Exception {
        System.out.println("connect");
        MySQLDump instance = new MySQLDump();
        instance.connect(hostname, username, password, schema);
        instance.cleanup();
    }

    /**
     * Test of dumpCreateDatabase method, of class MySQLDump.
     */
    @Test
    public void testDumpCreateDatabase() {
        System.out.println("dumpCreateDatabase");
        String database = "mysql";
        try {
            MySQLDump instance = new MySQLDump(hostname, username, password);
            String expResult = "CREATE DATABASE `mysql`";
            String result = instance.dumpCreateDatabase(database);
            assertThat(result, JUnitMatchers.containsString(expResult));
        }
        catch (SQLException se) {
            System.err.println(se.getMessage());
        }
    }


    /**
     * Test of dumpCreateEvent method, of class MySQLDump.
     */
    @Test
    public void testDumpCreateEvent_String_String() {
        System.out.println("dumpCreateEvent");
        String event = "switch_gender";
        try {
            MySQLDump instance = new MySQLDump(hostname, username, password);
            String expResult = "gender = 'F'";
            String result = instance.dumpCreateEvent(schema, event);
            assertThat(result, JUnitMatchers.containsString(expResult));
        }
        catch (SQLException se) {
            System.err.println(se.getMessage());
        }
    }

    /**
     * Test of dumpCreateTable method, of class MySQLDump.
     */
    @Test
    public void testDumpCreateTable_String() {
        System.out.println("dumpCreateTable");
        String table = "employees";
        try {
            MySQLDump instance = new MySQLDump(hostname, username, password, schema);
            String expResult = "582ca3f7cbaf4edcc1b445f8ea90b503";
            String result = instance.dumpCreateTable(table);
            assertThat(result, JUnitMatchers.containsString(expResult));
        }
        catch (SQLException se) {
            System.err.println(se.getMessage());
        }
    }

    /**
     * Test of dumpCreateTable method, of class MySQLDump.
     */
    @Test
    public void testDumpCreateTable_String_String() {
        System.out.println("dumpCreateTable");
        String table = "employees";
        try {
            MySQLDump instance = new MySQLDump(hostname, username, password);
            String expResult = "582ca3f7cbaf4edcc1b445f8ea90b503";
            String result = instance.dumpCreateTable(schema, table);
            assertThat(result, JUnitMatchers.containsString(expResult));
        }
        catch (SQLException se) {
            System.err.println(se.getMessage());
        }
    }

    /**
     * Test of dumpCreateView method, of class MySQLDump.
     */
    @Test
    public void testDumpCreateView_String() {
        System.out.println("dumpCreateView");
        String view = "employees_view";
        try {
            MySQLDump instance = new MySQLDump(hostname, username, password, schema);
            String expResult = "VIEW `employees_view` AS";
            String result = instance.dumpCreateView(view);
            assertThat(result, JUnitMatchers.containsString(expResult));
        }
        catch (SQLException se) {
            System.err.println(se.getMessage());
        }
    }

    /**
     * Test of dumpCreateView method, of class MySQLDump.
     */
    @Test
    public void testDumpCreateView_String_String() {
        System.out.println("dumpCreateView");
        String view = "employees_view";
        try {
            MySQLDump instance = new MySQLDump(hostname, username, password);
            String expResult = "`employees_view` AS";
            String result = instance.dumpCreateView(schema, view);
            assertThat(result, JUnitMatchers.containsString(expResult));
        }
        catch (SQLException se) {
            System.err.println(se.getMessage());
        }
    }

    /**
     * Test of dumpCreateEvent method, of class MySQLDump.
     */
    @Test
    public void testDumpCreateEvent_String() {
        System.out.println("dumpCreateEvent");
        String event = "switch_gender";
        try {
            MySQLDump instance = new MySQLDump(hostname, username, password, schema);
            String expResult = "gender = 'F'";
            String result = instance.dumpCreateEvent(event);
            assertThat(result, JUnitMatchers.containsString(expResult));
        }
        catch (SQLException se) {
            System.err.println(se.getMessage());
        }
    }

    /**
     * Test of dumpCreateRoutine method, of class MySQLDump.
     */
    @Test
    public void testDumpCreateRoutine_String_String() {
        System.out.println("dumpCreateRoutine");
        String routine = "emp_dept_id";
        try {
            MySQLDump instance = new MySQLDump(hostname, username, password);
            String expResult = "emp_no = employee_id";
            String result = instance.dumpCreateRoutine(schema, routine);
            assertThat(result, JUnitMatchers.containsString(expResult));
        }
        catch (SQLException se) {
            System.err.println(se.getMessage());
        }
    }

    /**
     * Test of dumpCreateRoutine method, of class MySQLDump.
     */
    @Test
    public void testDumpCreateRoutine_String() {
        System.out.println("dumpCreateRoutine");
        String routine = "emp_dept_id";
        try {
            MySQLDump instance = new MySQLDump(hostname, username, password, schema);
            String expResult = "emp_no = employee_id";
            String result = instance.dumpCreateRoutine(routine);
            assertThat(result, JUnitMatchers.containsString(expResult));
        }
        catch (SQLException se) {
            System.err.println(se.getMessage());
        }
    }

    /**
     * Test of dumpCreateTrigger method, of class MySQLDump.
     */
    @Test
    public void testDumpCreateTrigger_String_String() {
        System.out.println("dumpCreateTrigger");
        String trigger = "set_hire_date";
        try {
            MySQLDump instance = new MySQLDump(hostname, username, password);
            String expResult = "1976-03-19";
            String result = instance.dumpCreateTrigger(schema, trigger);
            assertThat(result, JUnitMatchers.containsString(expResult));
        }
        catch (SQLException se) {
            System.err.println(se.getMessage());
        }
    }

    /**
     * Test of dumpCreateTrigger method, of class MySQLDump.
     */
    @Test
    public void testDumpCreateTrigger_String() {
        System.out.println("dumpCreateTrigger");
        String trigger = "set_hire_date";
        try {
            MySQLDump instance = new MySQLDump(hostname, username, password, schema);
            String expResult = "1976-03-19";
            String result = instance.dumpCreateTrigger(trigger);
            assertThat(result, JUnitMatchers.containsString(expResult));
        }
        catch (SQLException se) {
            System.err.println(se.getMessage());
        }
    }

    /**
     * Test of dumpGlobalVariables method, of class MySQLDump.
     */
    @Test
    public void testDumpGlobalVariables() {
        System.out.println("dumpGlobalVariables");
        try {
            MySQLDump instance = new MySQLDump(hostname, username, password);
            String expResult = "datadir";
            Map<String, String> result = instance.dumpGlobalVariables();
            assert ( result.containsKey(expResult) );
        }
        catch (SQLException se) {
            System.err.println(se.getMessage());
        }
    }

    /**
     * Test of getVersion method, of class MySQLDump.
     */
    @Test
    public void testGetVersion() {
        System.out.println("getVersion");
        MySQLDump instance = new MySQLDump();
        String expResult = appProperties.getProperty("application.version");
        String result = instance.getVersion();
        assertEquals(expResult, result);
    }

    /**
     * Test of listSchemata method, of class MySQLDump.
     */
    @Test
    public void testListSchemata() {
        System.out.println("listSchemata");
        try {
            MySQLDump instance = new MySQLDump(hostname, username, password, schema);
            String expResult = schema;
            ArrayList<String> result = instance.listSchemata();
            assert ( result.contains(expResult) );
        }
        catch (SQLException se) {
            System.err.println(se.getMessage());
        }
    }

    /**
     * Test of listRoutines method, of class MySQLDump.
     */
    @Test
    public void testListRoutines() {
        System.out.println("listRoutines");
        try {
            MySQLDump instance = new MySQLDump(hostname, username, password, schema);
            String expResult = "emp_dept_id";
            ArrayList<String> result = instance.listRoutines(schema);
            assert ( result.contains(expResult) );
        }
        catch (SQLException se) {
            System.err.println(se.getMessage());
        }
    }

    /**
     * Test of listTriggers method, of class MySQLDump.
     */
    @Test
    public void testListTriggers() {
        System.out.println("listTriggers");
        try {
            MySQLDump instance = new MySQLDump(hostname, username, password, schema);
            String expResult = "set_hire_date";
            ArrayList<String> result = instance.listTriggers(schema);
            assert ( result.contains(expResult) );
        }
        catch (SQLException se) {
            System.err.println(se.getMessage());
        }
    }

    /**
     * Test of listGrantTables method, of class MySQLDump.
     */
    @Test
    public void testListGrantTables() {
        System.out.println("listGrantTables");
        try {
            MySQLDump instance = new MySQLDump(hostname, username, password);
            String expResult = "tables_priv";
            ArrayList<String> result = instance.listGrantTables();
            assert ( result.contains(expResult) );
        }
        catch (SQLException se) {
            System.err.println(se.getMessage());
        }
    }

    /**
     * Test of listTables method, of class MySQLDump.
     */
    @Test
    public void testListTables() {
        System.out.println("listTables");
        try {
            MySQLDump instance = new MySQLDump(hostname, username, password, schema);
            String expResult = "employees";
            ArrayList<String> result = instance.listTables(schema);
            assert ( result.contains(expResult) );
        }
        catch (SQLException se) {
            System.err.println(se.getMessage());
        }
    }

    /**
     * Test of listViews method, of class MySQLDump.
     */
    @Test
    public void testListViews() {
        System.out.println("listViews");
        try {
            MySQLDump instance = new MySQLDump(hostname, username, password, schema);
            String expResult = "employees_view";
            ArrayList<String> result = instance.listViews(schema);
            assert ( result.contains(expResult) );
        }
        catch (SQLException se) {
            System.err.println(se.getMessage());
        }
    }

    /**
     * Test of listEvents method, of class MySQLDump.
     */
    @Test
    public void testListEvents() {
        System.out.println("listEvents");
        try {
            MySQLDump instance = new MySQLDump(hostname, username, password);
            String expResult = "switch_gender";
            ArrayList<String> result = instance.listEvents(schema);
            assert ( result.contains(expResult) );
        }
        catch (SQLException se) {
            System.err.println(se.getMessage());
        }
    }

    /**
     * Test of getSchema method, of class MySQLDump.
     */
    @Test
    public void testGetSchema() {
        System.out.println("getSchema");
        try {
            MySQLDump instance = new MySQLDump(hostname, username, password, schema);
            String expResult = schema;
            String result = instance.getSchema();
            assertEquals(expResult, result);
        }
        catch (SQLException se) {
            System.err.println(se.getMessage());
        }
    }

    /**
     * Test of setSchema method, of class MySQLDump.
     */
    @Test
    public void testSetSchema() {
        System.out.println("setSchema");
        try {
            MySQLDump instance = new MySQLDump(hostname, username, password);
            String expResult = schema;
            instance.setSchema(schema);
            assertEquals(expResult, instance.getSchema());
        }
        catch (SQLException se) {
            System.err.println(se.getMessage());
        }
    }

    /**
     * Test of doMain method, of class MySQLDump.
     */
    @Test
    public void testDoMain() throws Exception {
        System.out.println("doMain");
        String[] args = {};
        MySQLDump instance = new MySQLDump();
        int result = instance.doMain(args);
        assertEquals(1,result);
    }

    /**
     * Test of cleanup method, of class MySQLDump.
     */
    @Test
    public void testCleanup() {
        System.out.println("cleanup");
        MySQLDump instance = new MySQLDump();
        int expResult = 1;
        int result = instance.cleanup();
        assertEquals(expResult, result);
    }
}
