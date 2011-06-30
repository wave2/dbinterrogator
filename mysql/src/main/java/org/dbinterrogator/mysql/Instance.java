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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;

/**
 *
 * @author Alan Snelson
 */
public interface Instance {
    
    public void connect(String hostname, int port, String username, String password, String db) throws SQLException;
    public void connect(String hostname, String username, String password, String db) throws SQLException;
    public void createSchema(String schema);
    public void dropSchema(String schema);
    public String getCreateDatabase(String database);
    public String getCreateEvent(String event);
    public String getCreateEvent(String schema, String event);
    public String getCreateRoutine(String routine);
    public String getCreateRoutine(String schema, String routine);
    public String getCreateTable(String table);
    public String getCreateTable(String schema, String table);
    public String getCreateTrigger(String trigger);
    public String getCreateTrigger(String schema, String trigger);
    public String getCreateView(String view);
    public String getCreateView(String schema, String view);
    public Map<String, String> getGlobalVariables();
    public String getVersion();
    public String executeScript(String schema, BufferedReader script);
    public ArrayList<String> listEvents(String schema);
    public ArrayList<String> listGrantTables();
    public ArrayList<String> listRoutines(String schema);
    public ArrayList<String> listSchemata();
    public ArrayList<String> listTables(String schema);
    public ArrayList<String> listTriggers(String schema);
    public ArrayList<String> listViews(String schema);
    public String getSchema();
    public void setSchema(String schema);
    public int cleanup();
    
}
