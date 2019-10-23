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

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.CannotRestoreSiddhiAppStateException;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.SiddhiTestHelper;
import org.wso2.siddhi.core.util.persistence.InMemoryPersistenceStore;
import org.wso2.siddhi.core.util.persistence.PersistenceStore;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TestCaseOfCDCPollingMode {
    private static final Logger log = Logger.getLogger(TestCaseOfCDCPollingMode.class);
    private Event currentEvent;
    private AtomicInteger eventCount = new AtomicInteger(0);
    private AtomicBoolean eventArrived = new AtomicBoolean(false);
    private int waitTime = 5000;
    private int timeout = 50000;
    private String username;
    private String password;
    private String jdbcDriverName;
    private String databaseURL;
    private String pollingColumn = "id";

    @BeforeClass
    public void initializeConnectionParams() {
        String databaseType = System.getenv("DATABASE_TYPE");
        String port = System.getenv("PORT");
        String host = System.getenv("DOCKER_HOST_IP");
        username = System.getenv("DATABASE_USER");
        password = System.getenv("DATABASE_PASSWORD");

        switch (databaseType) {
            case "MySQL":
                databaseURL = "jdbc:mysql://" + host + ":" + port + "/SimpleDB?useSSL=false";
                jdbcDriverName = "com.mysql.jdbc.Driver";
                break;
            case "ORACLE":
                String sid = System.getenv("SID");
                databaseURL = "jdbc:oracle:thin:@" + host + ":" + port + ":" + sid;
                jdbcDriverName = "oracle.jdbc.driver.OracleDriver";
                break;
            case "POSTGRES":
                String postgresDB = System.getenv("POSTGRES_DB");
                databaseURL = "jdbc:postgresql://" + host + ":" + port + "/" + postgresDB;
                jdbcDriverName = "org.postgresql.Driver";
                break;
            case "H2":
                databaseURL = "jdbc:h2:./target/testdb";
                jdbcDriverName = "org.h2.Driver";
                break;
            case "MSSQL":
                databaseURL = "jdbc:sqlserver://" + host + ":" + port + ";";
                jdbcDriverName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
                break;
            default:
                //If the environmental variables are not set, run tests against H2.
                databaseURL = "jdbc:h2:./target/testdb";
                jdbcDriverName = "org.h2.Driver";
                break;
        }
    }

    @BeforeMethod
    public void init() {
        eventCount.set(0);
        eventArrived.set(false);
        currentEvent = new Event();
    }

    /**
     * Test case to Capture Insert operation from a MySQL table using polling mode.
     */
    @Test
    public void testCDCPollingMode() throws InterruptedException {
        log.info("------------------------------------------------------------------------------------------------");
        log.info("CDC TestCase: Capturing change data with polling mode.");
        log.info("------------------------------------------------------------------------------------------------");

        String pollingTableName = "loginTable1";
        SiddhiManager siddhiManager = new SiddhiManager();

        int pollingInterval = 1;
        String cdcinStreamDefinition = "@source(type = 'cdc', mode='polling'," +
                " polling.column='" + pollingColumn + "'," +
                " jdbc.driver.name='" + jdbcDriverName + "'," +
                " url = '" + databaseURL + "'," +
                " username = '" + username + "'," +
                " password = '" + password + "'," +
                " table.name = '" + pollingTableName + "', polling.interval = '" + pollingInterval + "'," +
                " @map(type='keyvalue'))" +
                "define stream istm (id string, name string);\n";

        String rdbmsStoreDefinition = "define stream insertionStream (id string, name string);" +
                "@Store(type='rdbms', jdbc.url='" + databaseURL + "'," +
                " username='" + username + "', password='" + password + "' ," +
                " jdbc.driver.name='" + jdbcDriverName + "')" +
                " define table loginTable1 (id string, name string);";

        String rdbmsQuery = "@info(name='query2') " +
                "from insertionStream " +
                "insert into loginTable1;";

        QueryCallback rdbmsQueryCallback = new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    log.info("insert done: " + event);
                }
            }
        };

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cdcinStreamDefinition +
                rdbmsStoreDefinition + rdbmsQuery);
        siddhiAppRuntime.addCallback("query2", rdbmsQueryCallback);

        StreamCallback insertionStreamCallback = new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    currentEvent = event;
                    eventCount.getAndIncrement();
                    log.info(eventCount + ". " + event);
                    eventArrived.set(true);
                }
            }
        };

        siddhiAppRuntime.addCallback("istm", insertionStreamCallback);
        siddhiAppRuntime.start();

        //wait till cdc-poller initialize.
        Thread.sleep(5000);

        //Do an insert and wait for cdc app to capture.
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("insertionStream");
        Object[] insertingObject = new Object[]{"e002", "testEmployer"};
        inputHandler.send(insertingObject);

        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);

        //Assert event arrival.
        Assert.assertTrue(eventArrived.get());

        //Assert event data.
        Assert.assertEquals(insertingObject, currentEvent.getData());

        siddhiAppRuntime.shutdown();
        siddhiManager.shutdown();
    }

    @Test(dependsOnMethods = {"testCDCPollingMode"})
    public void testOutOfOrderRecords() throws InterruptedException {
        log.info("------------------------------------------------------------------------------------------------");
        log.info("CDC TestCase: Test missed/out-of-order events in polling mode.");
        log.info("------------------------------------------------------------------------------------------------");

        SiddhiManager siddhiManager = new SiddhiManager();

        int pollingInterval = 1;
        String cdcinStreamDefinition = "@source(type = 'cdc', " +
                "mode='polling', " +
                "polling.column='" + pollingColumn + "', " +
                "jdbc.driver.name='" + jdbcDriverName + "', " +
                "url = '" + databaseURL + "', " +
                "username = '" + username + "', " +
                "password = '" + password + "', " +
                "table.name = 'students', " +
                "polling.interval = '" + pollingInterval + "', " +
                "operation = 'insert', " +
                "wait.on.missed.record = 'true'," +
                "missed.record.waiting.timeout = '10'," +
                "@map(type='keyvalue'), " +
                "@attributes(id = 'id', name = 'name'))" +
                "define stream outputStream (id int, name string);\n";

        String rdbmsStoreDefinition = "define stream inputStream (id int, name string);" +
                "@Store(type='rdbms', " +
                "jdbc.url='" + databaseURL + "', " +
                "username='" + username + "', " +
                "password='" + password + "' , " +
                "jdbc.driver.name='" + jdbcDriverName + "')" +
                "define table students (id int, name string);";

        String rdbmsQuery = "@info(name='query2') " +
                "from inputStream " +
                "insert into students;";

        QueryCallback rdbmsQueryCallback = new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    log.info("insert done: " + event);
                }
            }
        };

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cdcinStreamDefinition +
                rdbmsStoreDefinition + rdbmsQuery);
        siddhiAppRuntime.addCallback("query2", rdbmsQueryCallback);

        StreamCallback outputStreamCallback = new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    eventCount.getAndIncrement();
                    log.info(eventCount + ". " + event);
                }
            }
        };

        siddhiAppRuntime.addCallback("outputStream", outputStreamCallback);
        siddhiAppRuntime.start();

        // Wait till CDC poller initializes.
        Thread.sleep(5000);

        // Do inserts and wait CDC app to capture the events.
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        Object[] ann = new Object[]{1, "Ann"};
        Object[] bob = new Object[]{2, "Bob"};
        Object[] charles = new Object[]{3, "Charles"};
        Object[] david = new Object[]{4, "David"};

        inputHandler.send(ann);
        inputHandler.send(bob);
        inputHandler.send(david);
        Thread.sleep(1000);
        inputHandler.send(charles);

        SiddhiTestHelper.waitForEvents(waitTime, 4, eventCount, timeout);

        // Assert received event count.
        Assert.assertEquals(eventCount.get(), 4);

        siddhiAppRuntime.shutdown();
        siddhiManager.shutdown();
    }

    /**
     * Test case to test state persistence of polling mode.
     */
    @Test(dependsOnMethods = {"testOutOfOrderRecords"})
    public void testCDCPollingModeStatePersistence() throws InterruptedException {
        log.info("------------------------------------------------------------------------------------------------");
        log.info("CDC TestCase: Testing state persistence of the polling mode.");
        log.info("------------------------------------------------------------------------------------------------");

        String pollingTableName = "loginTable2";
        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        int pollingInterval = 1;
        String cdcinStreamDefinition = "@App:name('testing_siddhi_app')" +
                "\n@source(type = 'cdc', mode='polling'," +
                " polling.column='" + pollingColumn + "'," +
                " jdbc.driver.name='" + jdbcDriverName + "'," +
                " url = '" + databaseURL + "'," +
                " username = '" + username + "'," +
                " password = '" + password + "'," +
                " table.name = '" + pollingTableName + "', polling.interval = '" + pollingInterval + "'," +
                " @map(type='keyvalue'))" +
                "define stream istm (id string, name string);";

        String rdbmsStoreDefinition = "\ndefine stream insertionStream (id string, name string);" +
                "\n@Store(type='rdbms', jdbc.url='" + databaseURL + "'," +
                " username='" + username + "', password='" + password + "' ," +
                " jdbc.driver.name='" + jdbcDriverName + "')" +
                "\ndefine table loginTable2 (id string, name string);";

        String rdbmsQuery = "@info(name='query2') " +
                "from insertionStream " +
                "insert into loginTable2;";

        QueryCallback rdbmsQueryCallback = new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    log.info("insert done: " + event);
                }
            }
        };

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cdcinStreamDefinition
                + rdbmsStoreDefinition + rdbmsQuery);
        siddhiAppRuntime.addCallback("query2", rdbmsQueryCallback);

        StreamCallback insertionStreamCallback = new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    currentEvent = event;
                    eventCount.getAndIncrement();
                    log.info(eventCount + ". " + event);
                    eventArrived.set(true);
                }
            }
        };

        siddhiAppRuntime.addCallback("istm", insertionStreamCallback);
        siddhiAppRuntime.start();

        //wait till cdc-poller initialize.
        Thread.sleep(5000);

        //Do an insert and wait for cdc app to capture.
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("insertionStream");
        Object[] insertingObject = new Object[]{"e003", "testEmployer"};
        inputHandler.send(insertingObject);

        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);

        //Assert event arrival.
        Assert.assertTrue(eventArrived.get());

        //Assert event data.
        Assert.assertEquals(insertingObject, currentEvent.getData());

        //persisting
        Thread.sleep(5000);
        siddhiAppRuntime.persist();

        //stopping siddhi app
        Thread.sleep(500);
        siddhiAppRuntime.shutdown();

        //insert a row while the cdc source is down.
        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(rdbmsStoreDefinition + rdbmsQuery);
        siddhiAppRuntime.addCallback("query2", rdbmsQueryCallback);
        siddhiAppRuntime.start();
        inputHandler = siddhiAppRuntime.getInputHandler("insertionStream");
        insertingObject = new Object[]{"e004", "new_employer"};
        inputHandler.send(insertingObject);
        Thread.sleep(5000);
        siddhiAppRuntime.shutdown();

        //start CDC siddhi app
        init();
        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cdcinStreamDefinition);
        siddhiAppRuntime.addCallback("istm", insertionStreamCallback);
        siddhiAppRuntime.start();
        Thread.sleep(5000);

        //loading
        try {
            siddhiAppRuntime.restoreLastRevision();
        } catch (CannotRestoreSiddhiAppStateException e) {
            Assert.fail("Restoring of Siddhi app " + siddhiAppRuntime.getName() + " failed", e);
        }

        //wait till cdc-poller initialize.
        Thread.sleep(5000);

        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);

        //Assert event arrival.
        Assert.assertTrue(eventArrived.get());

        //Assert event data.
        Assert.assertEquals(insertingObject, currentEvent.getData());

        siddhiAppRuntime.shutdown();
        siddhiManager.shutdown();
    }
}
