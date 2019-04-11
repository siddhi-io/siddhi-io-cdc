/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.extension.siddhi.io.cdc.source;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.apache.log4j.Logger;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TestCaseOfCDCSourceValidation {

    private static final Logger log = Logger.getLogger(TestCaseOfCDCSourceValidation.class);
    private AtomicInteger eventCount = new AtomicInteger(0);
    private AtomicBoolean eventArrived = new AtomicBoolean(false);
    private int waitTime = 50;
    private int timeout = 10000;
    private String username = "username";
    private String password = "password";
    private String databaseURL = "jdbc:mysql://localhost:3306/SimpleDB";
    private String tableName = "login";

    @BeforeMethod
    public void init() {
        eventCount.set(0);
        eventArrived.set(false);
    }

    /**
     * Test case to validate operation given by the user.
     */
    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void cdcOperationValidation() throws InterruptedException {
        log.info("------------------------------------------------------------------------------------------------");
        log.info("CDC TestCase: Validating operation parameter.");
        log.info("------------------------------------------------------------------------------------------------");

        SiddhiManager siddhiManager = new SiddhiManager();

        String invalidOperation = "otherOperation";

        //stream definition with invalid operation.
        String inStreamDefinition = "" +
                "@app:name('cdcTesting')" +
                "@source(type = 'cdc' , url = '" + databaseURL + "',  username = '" + username + "'," +
                " password = '" + password + "', table.name = '" + tableName + "', " +
                " operation = '" + invalidOperation + "'," +
                " @map(type='keyvalue'))" +
                "define stream istm (id string, name string);";
        String query = ("@info(name = 'query1') " +
                "from istm " +
                "select *  " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    log.info("Received event: " + event);
                }
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(500, 2, new AtomicInteger(1), 10000);
        siddhiAppRuntime.shutdown();
    }

    /**
     * Test case to validate url.
     */
    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void cdcUrlValidation() throws InterruptedException {
        log.info("------------------------------------------------------------------------------------------------");
        log.info("CDC TestCase: Validating url");
        log.info("------------------------------------------------------------------------------------------------");

        SiddhiManager siddhiManager = new SiddhiManager();

        String wrongURL = "jdbc:mysql://0.0.0.0.0:3306/SimpleDB";

        //stream definition with invalid operation.
        String inStreamDefinition = "" +
                "@app:name('cdcTesting')" +
                "@source(type = 'cdc' , url = '" + wrongURL + "',  username = '" + username + "'," +
                " password = '" + password + "', table.name = '" + tableName + "', " +
                " operation = 'insert'," +
                " @map(type='keyvalue'))" +
                "define stream istm (id string, name string);";
        String query = ("@info(name = 'query1') " +
                "from istm " +
                "select *  " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    log.info("Received event: " + event);
                }
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        siddhiAppRuntime.shutdown();
    }

    /**
     * Test case to validate connector.properties.
     */
    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void cdcConnectorPropertiesValidation() throws InterruptedException {
        log.info("------------------------------------------------------------------------------------------------");
        log.info("CDC TestCase: Validating connector.properties.");
        log.info("------------------------------------------------------------------------------------------------");

        SiddhiManager siddhiManager = new SiddhiManager();

        String invalidConnectorProperties = "username=";

        //stream definition with invalid operation.
        String inStreamDefinition = "" +
                "@app:name('cdcTesting')" +
                "@source(type = 'cdc' , url = '" + databaseURL + "',  username = '" + username + "'," +
                " password = '" + password + "', table.name = '" + tableName + "', " +
                " operation = 'insert', connector.properties='" + invalidConnectorProperties + "'," +
                " @map(type='keyvalue'))" +
                "define stream istm (id string, name string);";
        String query = ("@info(name = 'query1') " +
                "from istm " +
                "select *  " +
                "insert into outputStream;");


        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    log.info("Received event: " + event);
                }
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        siddhiAppRuntime.shutdown();
    }

    /**
     * Test case to check for the missing mandatory parameter: url.
     */
    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void checkForParameterURL() throws InterruptedException {
        log.info("------------------------------------------------------------------------------------------------");
        log.info("CDC TestCase: Checking for the missing mandatory parameter: url.");
        log.info("------------------------------------------------------------------------------------------------");

        SiddhiAppRuntime siddhiAppRuntime;
        String streamDefinition;
        SiddhiManager siddhiManager = new SiddhiManager();
        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    log.info("Received event: " + event);
                }
            }
        };
        String query = ("@info(name = 'query1') " +
                "from istm " +
                "select *  " +
                "insert into outputStream;");

        //stream definition with missing parameter: url
        streamDefinition = "@app:name('cdcTesting')" +
                "@source(type = 'cdc' ,  username = '" + username + "'," +
                " password = '" + password + "', table.name = '" + tableName + "', " +
                " operation = 'insert'," +
                " @map(type='keyvalue'))" +
                "define stream istm (id string, name string);";

        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streamDefinition + query);
        siddhiAppRuntime.addCallback("query1", queryCallback);
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        siddhiAppRuntime.shutdown();
    }

    /**
     * Test case to check for the missing mandatory parameter: username.
     */
    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void checkForParameterUsername() throws InterruptedException {
        log.info("------------------------------------------------------------------------------------------------");
        log.info("CDC TestCase: Checking for the missing mandatory parameter: username.");
        log.info("------------------------------------------------------------------------------------------------");

        SiddhiAppRuntime siddhiAppRuntime;
        String streamDefinition;
        SiddhiManager siddhiManager = new SiddhiManager();
        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    log.info("Received event: " + event);
                }
            }
        };
        String query = ("@info(name = 'query1') " +
                "from istm " +
                "select *  " +
                "insert into outputStream;");

        //stream definition with missing parameter: username
        streamDefinition = "@app:name('cdcTesting')" +
                "@source(type = 'cdc' , url = '" + databaseURL + "'," +
                " password = '" + password + "', table.name = '" + tableName + "', " +
                " operation = 'insert'," +
                " @map(type='keyvalue'))" +
                "define stream istm (id string, name string);";

        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streamDefinition + query);
        siddhiAppRuntime.addCallback("query1", queryCallback);
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        siddhiAppRuntime.shutdown();
    }

    /**
     * Test case to check for the missing mandatory parameter: password.
     */
    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void checkForParameterPassword() throws InterruptedException {
        log.info("------------------------------------------------------------------------------------------------");
        log.info("CDC TestCase: Checking for the missing mandatory parameter: password.");
        log.info("------------------------------------------------------------------------------------------------");

        SiddhiAppRuntime siddhiAppRuntime;
        String streamDefinition;
        SiddhiManager siddhiManager = new SiddhiManager();
        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    log.info("Received event: " + event);
                }
            }
        };
        String query = ("@info(name = 'query1') " +
                "from istm " +
                "select *  " +
                "insert into outputStream;");

        //stream definition with missing parameter: password
        streamDefinition = "@app:name('cdcTesting')" +
                "@source(type = 'cdc' , url = '" + databaseURL + "',  username = '" + username + "'," +
                " table.name = '" + tableName + "', " +
                " operation = 'insert'," +
                " @map(type='keyvalue'))" +
                "define stream istm (id string, name string);";

        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streamDefinition + query);
        siddhiAppRuntime.addCallback("query1", queryCallback);
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        siddhiAppRuntime.shutdown();
    }

    /**
     * Test case to check for the missing mandatory parameter: operation.
     */
    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void checkForParameterOperation() throws InterruptedException {
        log.info("------------------------------------------------------------------------------------------------");
        log.info("CDC TestCase: Checking for the missing mandatory parameter: operation.");
        log.info("------------------------------------------------------------------------------------------------");

        SiddhiAppRuntime siddhiAppRuntime;
        String streamDefinition;
        SiddhiManager siddhiManager = new SiddhiManager();
        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    log.info("Received event: " + event);
                }
            }
        };
        String query = ("@info(name = 'query1') " +
                "from istm " +
                "select *  " +
                "insert into outputStream;");

        //stream definition with missing parameter: operation
        streamDefinition = "@app:name('cdcTesting')" +
                "@source(type = 'cdc' , url = '" + databaseURL + "',  username = '" + username + "'," +
                " password = '" + password + "', table.name = '" + tableName + "', " +
                " @map(type='keyvalue'))" +
                "define stream istm (id string, name string);";

        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streamDefinition + query);
        siddhiAppRuntime.addCallback("query1", queryCallback);
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        siddhiAppRuntime.shutdown();
    }

    /**
     * Test case to check for the missing mandatory parameter: table.name.
     */
    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void checkForParameterTableName() throws InterruptedException {
        log.info("------------------------------------------------------------------------------------------------");
        log.info("CDC TestCase: Checking for the missing mandatory parameter: table.name.");
        log.info("------------------------------------------------------------------------------------------------");

        SiddhiAppRuntime siddhiAppRuntime;
        String streamDefinition;
        SiddhiManager siddhiManager = new SiddhiManager();
        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    log.info("Received event: " + event);
                }
            }
        };
        String query = ("@info(name = 'query1') " +
                "from istm " +
                "select *  " +
                "insert into outputStream;");

        //stream definition with missing parameter: table.name
        streamDefinition = "@app:name('cdcTesting')" +
                "@source(type = 'cdc' , url = '" + databaseURL + "',  username = '" + username + "'," +
                " password = '" + password + "'," +
                " operation = 'insert'," +
                " @map(type='keyvalue'))" +
                "define stream istm (id string, name string);";

        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streamDefinition + query);
        siddhiAppRuntime.addCallback("query1", queryCallback);
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        siddhiAppRuntime.shutdown();
    }

    /**
     * Test case to validate mode.
     */
    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void cdcModeValidation() throws InterruptedException {
        log.info("------------------------------------------------------------------------------------------------");
        log.info("CDC TestCase: Validate parameter: mode");
        log.info("------------------------------------------------------------------------------------------------");

        String wrongMode = "otherMode";

        SiddhiAppRuntime siddhiAppRuntime;
        String streamDefinition;
        SiddhiManager siddhiManager = new SiddhiManager();
        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    log.info("Received event: " + event);
                }
            }
        };
        String query = ("@info(name = 'query1') " +
                "from istm " +
                "select *  " +
                "insert into outputStream;");

        //stream definition with missing parameter: table.name
        streamDefinition = "@app:name('cdcTesting')" +
                "@source(type = 'cdc', mode = '" + wrongMode + "'," +
                " url = '" + databaseURL + "'," +
                " username = '" + username + "'," +
                " password = '" + password + "'," +
                " table.name = '" + tableName + "', " +
                " operation = 'insert', " +
                " @map(type='keyvalue'))" +
                "define stream istm (id string, name string);";

        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streamDefinition + query);
        siddhiAppRuntime.addCallback("query1", queryCallback);
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        siddhiAppRuntime.shutdown();
    }
}
