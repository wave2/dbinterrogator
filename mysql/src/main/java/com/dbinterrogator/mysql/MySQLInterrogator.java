/*
 * Copyright (c) 2018 Wave2 Limited
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.dbinterrogator.mysql;

import com.dbinterrogator.service.InterrogatorService;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;

public class MySQLInterrogator implements InterrogatorService {
    private Connection conn = null;

    public MySQLInterrogator() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (Exception ex) {
            // handle the error
        }
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

    @Override
    public void Connect(String hostname, String username, String password) {
        try {
            conn = DriverManager.getConnection("jdbc:mysql://" + hostname + "/information_schema?" + "user=" + username + "&password=" + password + "&useSSL=false");
        } catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }
    }

    @Override
    public void Disconnect() {

    }

    @Override
    public ArrayList<String> getUsers() {
        return null;
    }

    private ArrayList<String> singleColumn(String sqlFile){
        ArrayList<String> results = new ArrayList<String>();
        try {
            Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ResultSet rs = stmt.executeQuery(convertStreamToString(getClass().getClassLoader().getResourceAsStream(sqlFile + ".sql")));
            while (rs.next()) {
                results.add(rs.getString(1));
            }
            rs.close();
            stmt.close();
        } catch (SQLException sqle) {
            System.err.println(sqle.getMessage());
        } catch (Exception ex) {
            ex.printStackTrace();
            System.err.println(ex.getMessage());
        }
        return results;
    }

    @Override
    public ArrayList<String> getSchemata() {
        return singleColumn("getSchemata");
    }

    @Override
    public ArrayList<String> getDatabases(String Schema) {
        return null;
    }

    @Override
    public ArrayList<String> getTables(String schema) {
        ArrayList<String> tables = new ArrayList<String>();
        try {
            PreparedStatement stmt = conn.prepareStatement(convertStreamToString(getClass().getClassLoader().getResourceAsStream("getTables.sql")),ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            stmt.setString(1, schema);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                tables.add(rs.getString("TABLE_NAME"));
            }
            rs.close();
            stmt.close();
        } catch (SQLException sqle) {
            System.err.println(sqle.getMessage());
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
        }
        return tables;
    }

    @Override
    public String getCreateTable(String database, String table) {
        return null;
    }

    @Override
    public ArrayList<String> getIdentityColumns(String schema, String table) {
        ArrayList<String> columns = new ArrayList<String>();
        try {
            PreparedStatement stmt = conn.prepareStatement(convertStreamToString(getClass().getClassLoader().getResourceAsStream("getPrimaryKeyColumns.sql")),ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            stmt.setString(1, schema);
            stmt.setString(2, table);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                columns.add(rs.getString("COLUMN_NAME"));
            }
            rs.close();
            stmt.close();
        } catch (SQLException sqle) {
            System.err.println(sqle.getMessage());
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
        }
        return columns;
    }

    @Override
    public String getMaxValue(String column, String schema, String table) {
        String maxValue = "";
        try {
            Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ResultSet rs = stmt.executeQuery(String.format(convertStreamToString(getClass().getClassLoader().getResourceAsStream("getMaxValue.sql")),column,schema,table));
            while (rs.next()) {
                maxValue = rs.getString("MAX_VALUE");
            }
            rs.close();
            stmt.close();
        } catch (SQLException sqle) {
            System.err.println(sqle.getMessage());
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
        }
        return maxValue;
    }
}