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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dbinterrogator.mysql.MySQLInterrogator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

class MySQLInterrogatorTest {

    private static MySQLInterrogator interrogator = new MySQLInterrogator();
    private static String hostname;
    private static String username;
    private static String password;

    @BeforeAll
    static void initAll() {
        hostname = System.getProperty("mysql.hostname");
        username = System.getProperty("mysql.username");
        password = System.getProperty("mysql.password");
        interrogator.Connect(hostname,username,password);
    }

    @Test
    void getSchemataTest() {
        ArrayList<String> schemata = interrogator.getSchemata();
        assertEquals("information_schema", schemata.get(0));
        System.out.println(schemata);
    }

    @Test
    void getTablesTest() {
        ArrayList<String> tables = interrogator.getTables("information_schema");
        assertEquals("CHARACTER_SETS", tables.get(0));
        System.out.println(tables);
    }
}
