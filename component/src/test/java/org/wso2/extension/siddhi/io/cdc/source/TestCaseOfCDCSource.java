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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.CannotRestoreSiddhiAppStateException;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.SiddhiTestHelper;
import org.wso2.siddhi.core.util.config.InMemoryConfigManager;
import org.wso2.siddhi.core.util.persistence.InMemoryPersistenceStore;
import org.wso2.siddhi.core.util.persistence.PersistenceStore;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TestCaseOfCDCSource {

    private static final Logger log = Logger.getLogger(TestCaseOfCDCSource.class);
    private AtomicInteger eventCount = new AtomicInteger(0);
    private AtomicBoolean eventArrived = new AtomicBoolean(false);
    private int waitTime = 50;
    private int timeout = 10000;
    private String username = "root";
    private String password = "1234";
    private String jdbcDriverName = "com.mysql.jdbc.Driver";
    private String databaseURL = "jdbc:mysql://localhost:3306/SimpleDB";
    private String tableName = "login";

    @BeforeMethod
    public void init() {
        eventCount.set(0);
        eventArrived.set(false);
    }

    /**
     * Test case to Capture Insert operations from a MySQL table.
     * Offset data persistence is enabled.
     */
    @Test
    public void testInsertCDC() throws InterruptedException {
        log.info("------------------------------------------------------------------------------------------------");
        log.info("CDC TestCase-1: Capturing Insert change data from MySQL.");
        log.info("------------------------------------------------------------------------------------------------");

        PersistenceStore persistenceStore = new InMemoryPersistenceStore();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String cdcinStreamDefinition = "@app:name('cdcTesting')" +
                "@source(type = 'cdc'," +
                " url = '" + databaseURL + "'," +
                " username = '" + username + "'," +
                " password = '" + password + "'," +
                " table.name = '" + tableName + "', " +
                " operation = 'insert', " +
                " @map(type='keyvalue'))" +
                "define stream istm (id string, name string);";

        String cdcquery = ("@info(name = 'query1') " +
                "from istm#log() " +
                "select *  " +
                "insert into outputStream;");


        SiddhiAppRuntime cdcAppRuntime = siddhiManager.createSiddhiAppRuntime(cdcinStreamDefinition +
                cdcquery);

        siddhiManager.setConfigManager(new InMemoryConfigManager());

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    eventCount.getAndIncrement();
                    log.info(eventCount + ". " + event);
                    eventArrived.set(true);
                }
            }
        };

        cdcAppRuntime.addCallback("query1", queryCallback);
        cdcAppRuntime.start();

        SiddhiTestHelper.waitForEvents(waitTime, 11, eventCount, timeout);

        //persisting
        cdcAppRuntime.persist();

        //restarting siddhi app
        cdcAppRuntime.shutdown();
        eventArrived.set(false);
        eventCount.set(0);

        cdcAppRuntime = siddhiManager.createSiddhiAppRuntime(cdcinStreamDefinition + cdcquery);
        cdcAppRuntime.addCallback("query1", queryCallback);
        cdcAppRuntime.start();

        //loading
        try {
            cdcAppRuntime.restoreLastRevision();
        } catch (CannotRestoreSiddhiAppStateException e) {
            Assert.fail("Restoring of Siddhi app " + cdcAppRuntime.getName() + " failed");
        }

        log.info("Siddhi app restarted. Waiting for events...");

        //starting RDBMS store.

        String rdbmsStoreDefinition = "define stream insertionStream (id string, name string);" +
                "@Store(type='rdbms', jdbc.url='" + databaseURL + "'," +
                " username='" + username + "', password='" + password + "' ," +
                " jdbc.driver.name='" + jdbcDriverName + "')" +
                "define table login (id string, name string);";

        String rdbmsQuery = "@info(name='query2') " +
                "from insertionStream " +
                "insert into login;";

        QueryCallback queryCallback2 = new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    log.info("insert done: " + event);
                }
            }
        };
        SiddhiAppRuntime rdbmsAppRuntime = siddhiManager.createSiddhiAppRuntime(rdbmsStoreDefinition + rdbmsQuery);
        rdbmsAppRuntime.addCallback("query2", queryCallback2);
        rdbmsAppRuntime.start();

        //Do an insert and wait for cdc app to capture.
        InputHandler rdbmsInputHandler = rdbmsAppRuntime.getInputHandler("insertionStream");
        Object[] insertingObject = new Object[]{"e077", "testEmployer"};
        rdbmsInputHandler.send(insertingObject);
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        siddhiManager.shutdown();
    }

    /**
     * Test case to Capture Delete operations from a MySQL table.
     */
    @Test
    public void testDeleteCDC() throws InterruptedException {
        log.info("------------------------------------------------------------------------------------------------");
        log.info("CDC TestCase-2: Capturing Delete change data from MySQL.");
        log.info("------------------------------------------------------------------------------------------------");

        PersistenceStore persistenceStore = new InMemoryPersistenceStore();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String cdcinStreamDefinition = "@app:name('cdcTesting')" +
                "@source(type = 'cdc'," +
                " url = '" + databaseURL + "'," +
                " username = '" + username + "'," +
                " password = '" + password + "'," +
                " table.name = '" + tableName + "', " +
                " operation = 'delete', " +
                " @map(type='keyvalue'))" +
                "define stream istm (before_id string, before_name string);";

        String cdcquery = ("@info(name = 'query1') " +
                "from istm#log() " +
                "select *  " +
                "insert into outputStream;");


        SiddhiAppRuntime cdcAppRuntime = siddhiManager.createSiddhiAppRuntime(cdcinStreamDefinition +
                cdcquery);

        siddhiManager.setConfigManager(new InMemoryConfigManager());

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    eventCount.getAndIncrement();
                    log.info(eventCount + ". " + event);
                    eventArrived.set(true);
                }
            }
        };

        cdcAppRuntime.addCallback("query1", queryCallback);
        cdcAppRuntime.start();

        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);

        //persisting
        cdcAppRuntime.persist();

        //restarting siddhi app
        cdcAppRuntime.shutdown();
        eventArrived.set(false);
        eventCount.set(0);

        cdcAppRuntime = siddhiManager.createSiddhiAppRuntime(cdcinStreamDefinition + cdcquery);
        cdcAppRuntime.addCallback("query1", queryCallback);
        cdcAppRuntime.start();

        //loading
        try {
            cdcAppRuntime.restoreLastRevision();
        } catch (CannotRestoreSiddhiAppStateException e) {
            Assert.fail("Restoring of Siddhi app " + cdcAppRuntime.getName() + " failed");
        }

        log.info("Siddhi app restarted. Waiting for events...");

        //starting RDBMS store.
        String rdbmsStoreDefinition = "define stream DeletionStream (id string);" +
                "@Store(type='rdbms', jdbc.url='" + databaseURL + "'," +
                " username='" + username + "', password='" + password + "' ," +
                " jdbc.driver.name='" + jdbcDriverName + "')" +
                "define table login (id string, name string);";

        String rdbmsQuery = "@info(name='query2') " +
                "from DeletionStream " +
                "delete login on login.id==id;";

        QueryCallback queryCallback2 = new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    log.info("delete done: " + event);
                }
            }
        };
        SiddhiAppRuntime rdbmsAppRuntime = siddhiManager.createSiddhiAppRuntime(rdbmsStoreDefinition + rdbmsQuery);
        rdbmsAppRuntime.addCallback("query2", queryCallback2);
        rdbmsAppRuntime.start();

        //Do an insert and wait for cdc app to capture.
        InputHandler rdbmsInputHandler = rdbmsAppRuntime.getInputHandler("DeletionStream");
        Object[] insertingObject = new Object[]{"e077"};
        rdbmsInputHandler.send(insertingObject);
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
    }

    /**
     * Test case to Capture Update operations from a MySQL table.
     */
    @Test
    public void testUpdateCDC() throws InterruptedException {
        log.info("------------------------------------------------------------------------------------------------");
        log.info("CDC TestCase-3: Capturing Update change data from MySQL.");
        log.info("------------------------------------------------------------------------------------------------");

        PersistenceStore persistenceStore = new InMemoryPersistenceStore();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String cdcinStreamDefinition = "@app:name('cdcTesting')" +
                "@source(type = 'cdc'," +
                " url = '" + databaseURL + "'," +
                " username = '" + username + "'," +
                " password = '" + password + "'," +
                " table.name = '" + tableName + "', " +
                " operation = 'update', " +
                " @map(type='keyvalue'))" +
                "define stream istm (before_id string, id string, before_name string, name string);";

        String cdcquery = ("@info(name = 'query1') " +
                "from istm#log() " +
                "select *  " +
                "insert into outputStream;");


        SiddhiAppRuntime cdcAppRuntime = siddhiManager.createSiddhiAppRuntime(cdcinStreamDefinition +
                cdcquery);

        siddhiManager.setConfigManager(new InMemoryConfigManager());

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    eventCount.getAndIncrement();
                    log.info(eventCount + ". " + event);
                    eventArrived.set(true);
                }
            }
        };

        cdcAppRuntime.addCallback("query1", queryCallback);
        cdcAppRuntime.start();

        SiddhiTestHelper.waitForEvents(waitTime, 11, eventCount, timeout);

        //persisting
        cdcAppRuntime.persist();

        //restarting siddhi app
        cdcAppRuntime.shutdown();
        eventArrived.set(false);
        eventCount.set(0);

        cdcAppRuntime = siddhiManager.createSiddhiAppRuntime(cdcinStreamDefinition + cdcquery);
        cdcAppRuntime.addCallback("query1", queryCallback);
        cdcAppRuntime.start();

        //loading
        try {
            cdcAppRuntime.restoreLastRevision();
        } catch (CannotRestoreSiddhiAppStateException e) {
            Assert.fail("Restoring of Siddhi app " + cdcAppRuntime.getName() + " failed");
        }

        log.info("Siddhi app restarted. Waiting for events...");

        //starting RDBMS store.

        String rdbmsStoreDefinition = "define stream UpdateStream (id string, name string);" +
                "@Store(type='rdbms', jdbc.url='" + databaseURL + "'," +
                " username='" + username + "', password='" + password + "' ," +
                " jdbc.driver.name='" + jdbcDriverName + "')" +
                "define table login (id string, name string);";

        String rdbmsQuery = "@info(name='query2') " +
                "from UpdateStream " +
                "update login on login.id==id;";

        QueryCallback queryCallback2 = new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    log.info("update done: " + event);
                }
            }
        };
        SiddhiAppRuntime rdbmsAppRuntime = siddhiManager.createSiddhiAppRuntime(rdbmsStoreDefinition + rdbmsQuery);
        rdbmsAppRuntime.addCallback("query2", queryCallback2);
        rdbmsAppRuntime.start();

        //Do an insert and wait for cdc app to capture.
        InputHandler rdbmsInputHandler = rdbmsAppRuntime.getInputHandler("UpdateStream");
        Object[] insertingObject = new Object[]{"e077", "newEmpName"};
        rdbmsInputHandler.send(insertingObject);
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        siddhiManager.shutdown();
    }

    /**
     * Test case to validate operation given by the user.
     */
    @Test
    public void cdcOperationValidation() throws InterruptedException {
        log.info("------------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();

        String invalidOperation = "otherOperation";

        //stream definition with invalid operation.
        String inStreamDefinition = "" +
                "@app:name('cdcTesting')" +
                "@source(type = 'cdc' , url = 'jdbc:mysql://localhost:3306/SimpleDB',  username = 'root'," +
                " password = '1234', table.name = 'login', " +
                " operation = '" + invalidOperation + "'," +
                " @map(type='keyvalue'))" +
                "define stream istm (id string, name string);";
        String query = ("@info(name = 'query1') " +
                "from istm " +
                "select *  " +
                "insert into outputStream;");


        try {
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
        } catch (SiddhiAppValidationException valEx) {
            Assert.assertEquals("Unsupported operation: '" + invalidOperation + "'. operation should be one of" +
                            " 'insert', 'update' or 'delete'",
                    valEx.getMessageWithOutContext());
        }


    }
}
