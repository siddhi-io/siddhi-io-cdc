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
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.SiddhiTestHelper;
import org.wso2.siddhi.core.util.config.InMemoryConfigManager;
import org.wso2.siddhi.core.util.persistence.InMemoryPersistenceStore;
import org.wso2.siddhi.core.util.persistence.PersistenceStore;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TestCaseOfCDCSource {

    private static final Logger log = Logger.getLogger(TestCaseOfCDCSource.class);
    private Event currentEvent;
    private AtomicInteger eventCount = new AtomicInteger(0);
    private AtomicBoolean eventArrived = new AtomicBoolean(false);
    private int waitTime = 50;
    private int timeout = 10000;
    private String username = "";
    private String password = "";
    private String jdbcDriverName = "com.mysql.jdbc.Driver";
    private String databaseURL = "jdbc:mysql://localhost:3306/SimpleDB";
    private String tableName = "login";

    @BeforeMethod
    public void init() {
        eventCount.set(0);
        eventArrived.set(false);
        currentEvent = new Event();
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
                " jdbc.driver.name='" + jdbcDriverName + "')" +
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

        cdcAppRuntime.addCallback("istm",insertionStreamCallback);
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
        log.info("CDC TestCase-3: Capturing Delete change data from MySQL.");
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
                " jdbc.driver.name='" + jdbcDriverName + "')" +
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

        cdcAppRuntime.addCallback("delstm",deletionStreamCallback);
        cdcAppRuntime.start();

        QueryCallback rdbmsQuerycallback = new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
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

        Thread.sleep(100);

        //Delete inserted row
        rdbmsInputHandler = rdbmsAppRuntime.getInputHandler("DeletionStream");
        Object[] deletingObject = new Object[]{"e001", "tobeDeletedName"};
        rdbmsInputHandler.send(deletingObject);

        //wait to capture the delete event.
        SiddhiTestHelper.waitForEvents(waitTime, 100, eventCount, timeout);

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
        log.info("CDC TestCase-2: Capturing Update change data from MySQL.");
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
                " jdbc.driver.name='" + jdbcDriverName + "')" +
                "define table login (id string, name string);";

        String insertQuery = "@info(name='query3') " +
                "from InsertStream " +
                "insert into login;";

        String updateQuery = "@info(name='queryUpdate') " +
                "from UpdateStream " +
                "update login on login.id==id;";

        SiddhiAppRuntime cdcAppRuntime = siddhiManager.createSiddhiAppRuntime(cdcinStreamDefinition);

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    currentEvent = event;
                    eventCount.getAndIncrement();
                    log.info(eventCount + ". " + event);
                    eventArrived.set(true);
                }
            }
        };
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

        cdcAppRuntime.addCallback("updatestm",updatingStreamCallback);
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
                "@source(type = 'cdc' , url = '" + databaseURL + "',  username = '" + username + "'," +
                " password = '" + password + "', table.name = '" + tableName + "', " +
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

    /**
     * Test case to validate url.
     */
    @Test
    public void cdcUrlValidation() throws InterruptedException {
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
            SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
            siddhiAppRuntime.shutdown();
        } catch (SiddhiAppCreationException valEx) {
            Assert.assertEquals("The cdc source couldn't get started because of invalid configurations." +
                            " Found configurations: {username='" + username + "', password=******," +
                            " url='" + wrongURL + "', tablename='" + tableName + "', connetorProperties=''}",
                    valEx.getMessageWithOutContext());
        }
    }

    /**
     * Test case to validate connector.properties.
     */
    @Test
    public void cdcConnectorPropertiesValidation() throws InterruptedException {
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
            SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
            siddhiAppRuntime.shutdown();
        } catch (SiddhiAppValidationException valEx) {
            Assert.assertEquals("connector.properties input is invalid. Check near :" + invalidConnectorProperties,
                    valEx.getMessageWithOutContext());
        }
    }

    /**
     * Test case to check for the missing mandatory parameter: url.
     */
    @Test
    public void checkForParameterURL() throws InterruptedException {
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

        try {
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streamDefinition + query);
            siddhiAppRuntime.addCallback("query1", queryCallback);
            siddhiAppRuntime.start();
            SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
            siddhiAppRuntime.shutdown();
        } catch (SiddhiAppValidationException valEx) {
            Assert.assertEquals("Option 'url' does not exist in the configuration of 'source:cdc'.",
                    valEx.getMessageWithOutContext());
        }
    }

    /**
     * Test case to check for the missing mandatory parameter: username.
     */
    @Test
    public void checkForParameterUsername() throws InterruptedException {
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

        try {
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streamDefinition + query);
            siddhiAppRuntime.addCallback("query1", queryCallback);
            siddhiAppRuntime.start();
            SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
            siddhiAppRuntime.shutdown();
        } catch (SiddhiAppValidationException valEx) {
            Assert.assertEquals("Option 'username' does not exist in the configuration of 'source:cdc'.",
                    valEx.getMessageWithOutContext());
        }
    }

    /**
     * Test case to check for the missing mandatory parameter: password.
     */
    @Test
    public void checkForParameterPassword() throws InterruptedException {
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

        try {
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streamDefinition + query);
            siddhiAppRuntime.addCallback("query1", queryCallback);
            siddhiAppRuntime.start();
            SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
            siddhiAppRuntime.shutdown();
        } catch (SiddhiAppValidationException valEx) {
            Assert.assertEquals("Option 'password' does not exist in the configuration of 'source:cdc'.",
                    valEx.getMessageWithOutContext());
        }
    }

    /**
     * Test case to check for the missing mandatory parameter: operation.
     */
    @Test
    public void checkForParameterOperation() throws InterruptedException {
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

        try {
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streamDefinition + query);
            siddhiAppRuntime.addCallback("query1", queryCallback);
            siddhiAppRuntime.start();
            SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
            siddhiAppRuntime.shutdown();
        } catch (SiddhiAppValidationException valEx) {
            Assert.assertEquals("Option 'operation' does not exist in the configuration of 'source:cdc'.",
                    valEx.getMessageWithOutContext());
        }
    }

    /**
     * Test case to check for the missing mandatory parameter: table.name.
     */
    @Test
    public void checkForParameterTableName() throws InterruptedException {
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

        try {
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streamDefinition + query);
            siddhiAppRuntime.addCallback("query1", queryCallback);
            siddhiAppRuntime.start();
            SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
            siddhiAppRuntime.shutdown();
        } catch (SiddhiAppValidationException valEx) {
            Assert.assertEquals("Option 'table.name' does not exist in the configuration of 'source:cdc'.",
                    valEx.getMessageWithOutContext());
        }
    }
}
