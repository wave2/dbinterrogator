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

package com.dbinterrogator.cli;

import com.dbinterrogator.service.InterrogatorService;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Unmatched;
import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Callable;

public class Main implements Callable<Void> {
    //Logger
    private static final Logger logger = LoggerFactory.getLogger(Main.class.getName());

    //Application properties
    private final Properties properties = new Properties();

    @Option(names = { "-classpath"}, hidden = true, description = "Hack to ignore classpath option")
    private String classpath;

    @Option(names = { "-c", "--config" }, required = true, description = "Configuration file")
    private Path config;

    @Option(names = { "-d", "--database" }, required = true, description = "Database Type")
    private String database;

    @Option(names = { "-i", "--instance" }, required = true, description = "Database Instance")
    private String instance;

    @Option(names = { "-h", "--help" }, usageHelp = true, description = "Displays this help message and quits.")
    private boolean helpRequested = false;

    @Parameters(arity = "1..*", paramLabel = "Commands", description = "Interrogation commands")
    private String[] commands;

    @Unmatched
    private String[] unmatchedArgs;


    @Override
    public Void call() throws Exception {
        if (config.toFile().canRead()) {
            System.out.println(config.toAbsolutePath());
            properties.load(new FileInputStream(config.toFile()));
        } else {
            logger.error("Unable to read config file: " + config.toAbsolutePath());
            System.exit(1);
        }
        //MySQL
        if (database.equals("mysql")) {
            logger.info("Connecting to MySQL on " + properties.getProperty("mysql." + instance + ".hostname"));
            InterrogatorService interrogator = InterrogatorService.newInstance("com.dbinterrogator.mysql.MySQLInterrogator");
            interrogator.Connect(properties.getProperty("mysql." + instance + ".hostname"), properties.getProperty("mysql." + instance + ".username"), properties.getProperty("mysql." + instance + ".password"));
            for (String command: commands) {
                if (command.toLowerCase().equals("getschemata")) {
                    logger.info("getSchemata");
                    logger.info(interrogator.getSchemata().toString());
                }
            }
            System.out.println(interrogator.getTables("sakila").toString());
            System.out.println(interrogator.getIdentityColumns("sakila", "actor").toString());
            System.out.println(interrogator.getMaxValue("actor_id", "sakila", "actor"));
        }
        return null;
    }


    public static void main(String[] args) throws Exception  {
        //Turn off noise from picocli
        System.setProperty("picocli.trace","OFF");
        CommandLine.call(new Main(), System.out, args);
    }

    /**
     * Load application properties
     */
    private void loadProperties() {
        try {
            properties.load(getClass().getResourceAsStream("/application.properties"));
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }



}