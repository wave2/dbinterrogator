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

package com.dbinterrogator.service;

import java.util.ServiceLoader;
import java.util.ArrayList;

public interface InterrogatorService {

    //Connection Related
    public void Connect(String hostname, String username, String password);
    public void Disconnect();

    //Security Related
    public ArrayList<String>  getUsers();

    //Structure Related
    public ArrayList<String>  getSchemata();
    public ArrayList<String>  getDatabases(String schema);
    public ArrayList<String>  getTables(String database);
    public String             getCreateTable(String database, String table);
    public ArrayList<String>  getIdentityColumns(String schema, String table);

    //Data related
    public String getMaxValue(String column, String schema, String table);

    public static InterrogatorService newInstance(String databaseType){
        ServiceLoader<InterrogatorService> service=ServiceLoader.load(InterrogatorService.class);
        for(InterrogatorService interrogator:service){
            if (interrogator.getClass().getName() == databaseType) {
                return interrogator;
            }
        }
        return null;

    }
}