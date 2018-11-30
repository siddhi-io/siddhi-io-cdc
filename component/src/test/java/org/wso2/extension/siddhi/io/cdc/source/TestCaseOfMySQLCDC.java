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

public class TestCaseOfMySQLCDC {

    private static final Logger log = Logger.getLogger(TestCaseOfMySQLCDC.class);
    private Event currentEvent;
    private AtomicInteger eventCount = new AtomicInteger(0);
    private AtomicBoolean eventArrived = new AtomicBoolean(false);
    private int waitTime = 50;
    private int timeout = 10000;
    private String username;
    private String password;
    private String mysqlJdbcDriverName;
    private String databaseURL;
    private String tableName = "login";

    @BeforeClass
    public void initializeConnectionParams() {
        String port = System.getenv("PORT");
        String host = System.getenv("DOCKER_HOST_IP");
        databaseURL = "jdbc:mysql://" + host + ":" + port + "/SimpleDB?useSSL=false";
        username = System.getenv("DATABASE_USER");
        password = System.getenv("DATABASE_PASSWORD");
        mysqlJdbcDriverName = "com.mysql.jdbc.Driver";
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
        log.info("CDC TestCase: Capturing change data from MySQL with polling mode.");
        log.info("------------------------------------------------------------------------------------------------");

        SiddhiManager siddhiManager = new SiddhiManager();

        String pollingColumn = "id";
        String pollingTableName = "login";
        int pollingInterval = 1;
        String cdcinStreamDefinition = "@source(type = 'cdc', mode='polling'," +
                " polling.column='" + pollingColumn + "'," +
                " jdbc.driver.name='" + mysqlJdbcDriverName + "'," +
                " url = '" + databaseURL + "'," +
                " username = '" + username + "'," +
                " password = '" + password + "'," +
                " table.name = '" + pollingTableName + "', polling.interval = '" + pollingInterval + "'," +
                " @map(type='keyvalue'))" +
                "define stream istm (id string, name string);\n";

        String rdbmsStoreDefinition = "define stream insertionStream (id string, name string);" +
                "@Store(type='rdbms', jdbc.url='" + databaseURL + "'," +
                " username='" + username + "', password='" + password + "' ," +
                " jdbc.driver.name='" + mysqlJdbcDriverName + "')" +
                " define table login (id string, name string);";

        String rdbmsQuery = "@info(name='query2') " +
                "from insertionStream " +
                "insert into login;";

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

    /**
     * Test case to test state persistence of polling mode.
     */
    @Test
    public void testCDCPollingModeStatePersistence() throws InterruptedException {
        log.info("------------------------------------------------------------------------------------------------");
        log.info("CDC TestCase: Testing state persistence of the polling mode.");
        log.info("------------------------------------------------------------------------------------------------");

        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String pollingColumn = "id";
        String pollingTableName = "login";
        int pollingInterval = 1;
        String cdcinStreamDefinition = "@App:name('testing_siddhi_app')" +
                "\n@source(type = 'cdc', mode='polling'," +
                " polling.column='" + pollingColumn + "'," +
                " jdbc.driver.name='" + mysqlJdbcDriverName + "'," +
                " url = '" + databaseURL + "'," +
                " username = '" + username + "'," +
                " password = '" + password + "'," +
                " table.name = '" + pollingTableName + "', polling.interval = '" + pollingInterval + "'," +
                " @map(type='keyvalue'))" +
                "define stream istm (id string, name string);";

        String rdbmsStoreDefinition = "\ndefine stream insertionStream (id string, name string);" +
                "\n@Store(type='rdbms', jdbc.url='" + databaseURL + "'," +
                " username='" + username + "', password='" + password + "' ," +
                " jdbc.driver.name='" + mysqlJdbcDriverName + "')" +
                "\ndefine table login (id string, name string);";

        String rdbmsQuery = "@info(name='query2') " +
                "from insertionStream " +
                "insert into login;";

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
        Thread.sleep(500);
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
        siddhiAppRuntime.shutdown();

        //start CDC siddhi app
        init();
        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cdcinStreamDefinition);
        siddhiAppRuntime.addCallback("istm", insertionStreamCallback);
        siddhiAppRuntime.start();

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

    /**
     * Test case to Capture Insert operations from a MySQL table.
     */
    @Test
    public void testInsertCDC() throws InterruptedException {
        log.info("------------------------------------------------------------------------------------------------");
        log.info("CDC TestCase: Capturing Insert change data from MySQL.");
        log.info("------------------------------------------------------------------------------------------------");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cdcinStreamDefinition = "@app:name('cdcTesting')" +
                "@source(type = 'cdc'," +
                " url = '" + databaseURL + "'," +
                " username = '" + username + "'," +
                " password = '" + password + "'," +
                " table.name = '" + tableName + "', " +
                " operation = 'insert', " +
                " @map(type='keyvalue'))" +
                "define stream istm (id string, name string);";

        String rdbmsStoreDefinition = "define stream insertionStream (id string, name string);" +
                "@Store(type='rdbms', jdbc.url='" + databaseURL + "'," +
                " username='" + username + "', password='" + password + "' ," +
                " jdbc.driver.name='" + mysqlJdbcDriverName + "')" +
                "define table login (id string, name string);";

        String rdbmsQuery = "@info(name='query2') " +
                "from insertionStream " +
                "insert into login;";

        SiddhiAppRuntime cdcAppRuntime = siddhiManager.createSiddhiAppRuntime(cdcinStreamDefinition);

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

        cdcAppRuntime.addCallback("istm", insertionStreamCallback);
        cdcAppRuntime.start();

        QueryCallback rdbmsQueryCallback = new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    log.info("insert done: " + event);
                }
            }
        };

        SiddhiAppRuntime rdbmsAppRuntime = siddhiManager.createSiddhiAppRuntime(rdbmsStoreDefinition + rdbmsQuery);
        rdbmsAppRuntime.addCallback("query2", rdbmsQueryCallback);
        rdbmsAppRuntime.start();

        //Do an insert and wait for cdc app to capture.
        InputHandler rdbmsInputHandler = rdbmsAppRuntime.getInputHandler("insertionStream");
        Object[] insertingObject = new Object[]{"e001", "testEmployer"};
        rdbmsInputHandler.send(insertingObject);
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);

        //Assert event arrival.
        Assert.assertTrue(eventArrived.get());

        //Assert event data.
        Assert.assertEquals(insertingObject, currentEvent.getData());

        cdcAppRuntime.shutdown();
        rdbmsAppRuntime.shutdown();
        siddhiManager.shutdown();
    }

    /**
     * Test case to Capture Delete operations from a MySQL table.
     */
    @Test
    public void testDeleteCDC() throws InterruptedException {
        log.info("------------------------------------------------------------------------------------------------");
        log.info("CDC TestCase: Capturing Delete change data from MySQL.");
        log.info("------------------------------------------------------------------------------------------------");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cdcinStreamDefinition = "@app:name('cdcTesting')" +
                "@source(type = 'cdc'," +
                " url = '" + databaseURL + "'," +
                " username = '" + username + "'," +
                " password = '" + password + "'," +
                " table.name = '" + tableName + "', " +
                " operation = 'delete', " +
                " @map(type='keyvalue'))" +
                "define stream delstm (before_id string, before_name string);";


        String rdbmsStoreDefinition = "define stream DeletionStream (id string, name string);" +
                "define stream InsertStream(id string, name string);" +
                "@Store(type='rdbms', jdbc.url='" + databaseURL + "'," +
                " username='" + username + "', password='" + password + "' ," +
                " jdbc.driver.name='" + mysqlJdbcDriverName + "')" +
                "define table login (id string, name string);";

        String insertQuery = "@info(name='query3') " +
                "from InsertStream " +
                "insert into login;";

        String deleteQuery = "@info(name='queryDel') " +
                "from DeletionStream " +
                "delete login on login.id==id and login.name==name;";

        SiddhiAppRuntime cdcAppRuntime = siddhiManager.createSiddhiAppRuntime(cdcinStreamDefinition);

        StreamCallback deletionStreamCallback = new StreamCallback() {
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

        cdcAppRuntime.addCallback("delstm", deletionStreamCallback);
        cdcAppRuntime.start();

        QueryCallback rdbmsQuerycallback = new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    eventCount.getAndIncrement();
                    log.info("delete done: " + event);
                }
            }
        };

        SiddhiAppRuntime rdbmsAppRuntime = siddhiManager.createSiddhiAppRuntime(rdbmsStoreDefinition + insertQuery
                + deleteQuery);
        rdbmsAppRuntime.addCallback("queryDel", rdbmsQuerycallback);
        rdbmsAppRuntime.start();

        //Do an insert first.
        InputHandler rdbmsInputHandler = rdbmsAppRuntime.getInputHandler("InsertStream");
        Object[] insertingObject = new Object[]{"e001", "tobeDeletedName"};
        rdbmsInputHandler.send(insertingObject);

        //wait to complete deletion
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        eventCount.getAndDecrement();

        //Delete inserted row
        rdbmsInputHandler = rdbmsAppRuntime.getInputHandler("DeletionStream");
        Object[] deletingObject = new Object[]{"e001", "tobeDeletedName"};
        rdbmsInputHandler.send(deletingObject);

        //wait to capture the delete event.
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);

        //Assert event arrival.
        Assert.assertTrue(eventArrived.get());

        //Assert event data.
        Assert.assertEquals(deletingObject, currentEvent.getData());

        cdcAppRuntime.shutdown();
        rdbmsAppRuntime.shutdown();
        siddhiManager.shutdown();
    }

    /**
     * Test case to Capture Update operations from a MySQL table.
     */
    @Test
    public void testUpdateCDC() throws InterruptedException {
        log.info("------------------------------------------------------------------------------------------------");
        log.info("CDC TestCase: Capturing Update change data from MySQL.");
        log.info("------------------------------------------------------------------------------------------------");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cdcinStreamDefinition = "@app:name('cdcTesting')" +
                "@source(type = 'cdc'," +
                " url = '" + databaseURL + "'," +
                " username = '" + username + "'," +
                " password = '" + password + "'," +
                " table.name = '" + tableName + "', " +
                " operation = 'update', " +
                " @map(type='keyvalue'))" +
                "define stream updatestm (before_id string, id string, before_name string, name string);";

        String rdbmsStoreDefinition = "define stream UpdateStream(id string, name string);" +
                "define stream InsertStream(id string, name string);" +
                "@Store(type='rdbms', jdbc.url='" + databaseURL + "'," +
                " username='" + username + "', password='" + password + "' ," +
                " jdbc.driver.name='" + mysqlJdbcDriverName + "')" +
                "define table login (id string, name string);";

        String insertQuery = "@info(name='query3') " +
                "from InsertStream " +
                "insert into login;";

        String updateQuery = "@info(name='queryUpdate') " +
                "from UpdateStream " +
                "update login on login.id==id;";

        SiddhiAppRuntime cdcAppRuntime = siddhiManager.createSiddhiAppRuntime(cdcinStreamDefinition);

        StreamCallback updatingStreamCallback = new StreamCallback() {
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

        cdcAppRuntime.addCallback("updatestm", updatingStreamCallback);
        cdcAppRuntime.start();

        QueryCallback queryCallback2 = new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    log.info("update done: " + event);
                }
            }
        };
        SiddhiAppRuntime rdbmsAppRuntime = siddhiManager.createSiddhiAppRuntime(rdbmsStoreDefinition + insertQuery
                + updateQuery);
        rdbmsAppRuntime.addCallback("queryUpdate", queryCallback2);
        rdbmsAppRuntime.start();

        //Do an insert first.
        InputHandler rdbmsInputHandler = rdbmsAppRuntime.getInputHandler("InsertStream");
        Object[] insertingObject = new Object[]{"e001", "empName"};
        rdbmsInputHandler.send(insertingObject);

        Thread.sleep(100);

        //Update inserted row.
        rdbmsInputHandler = rdbmsAppRuntime.getInputHandler("UpdateStream");
        Object[] updatingObject = new Object[]{"e001", "newName"};
        rdbmsInputHandler.send(updatingObject);

        //wait to capture the update event.
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);

        //Assert event arrival.
        Assert.assertTrue(eventArrived.get());

        //Assert event data.
        Object[] expectedEventObject = new Object[]{"e001", "e001", "empName", "newName"};
        Assert.assertEquals(expectedEventObject, currentEvent.getData());

        cdcAppRuntime.shutdown();
        rdbmsAppRuntime.shutdown();
        siddhiManager.shutdown();
    }
}
