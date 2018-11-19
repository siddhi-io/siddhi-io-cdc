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
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.SiddhiTestHelper;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TestCaseOfOracleCDC {

    private static final Logger log = Logger.getLogger(TestCaseOfOracleCDC.class);
    private Event currentEvent;
    private AtomicInteger eventCount = new AtomicInteger(0);
    private AtomicBoolean eventArrived = new AtomicBoolean(false);
    private int waitTime = 50;
    private int timeout = 10000;
    private String username;
    private String password;
    private String oracleJdbcDriverName;
    private String databaseURL;
    // TODO: 11/19/18 add doc in readme, test with docker

    @BeforeClass
    public void initializeConnectionParams() {
        String port = System.getenv("PORT");
        String host = System.getenv("DOCKER_HOST_IP");
        String sid = System.getenv("SID");
        databaseURL = "jdbc:oracle:thin:@" + host + ":" + port + ":" + sid;
        username = System.getenv("DATABASE_USER");
        password = System.getenv("DATABASE_PASSWORD");
        oracleJdbcDriverName = "oracle.jdbc.driver.OracleDriver";
    }

    @BeforeMethod
    public void init() {
        eventCount.set(0);
        eventArrived.set(false);
        currentEvent = new Event();
    }

    /**
     * Test case to Capture Insert, Update operations from a Oracle table using polling mode.
     */
    @Test
    public void testCDCPollingMode() throws InterruptedException {
        log.info("------------------------------------------------------------------------------------------------");
        log.info("CDC TestCase: Capturing change data from Oracle with polling mode.");
        log.info("------------------------------------------------------------------------------------------------");

        SiddhiManager siddhiManager = new SiddhiManager();

        String pollingColumn = "id";
        String pollingTableName = "login";
        int pollingInterval = 500;
        String cdcinStreamDefinition = "@app:name('cdcTesting')" +
                "@source(type = 'cdc', mode='polling'," +
                " polling.column='" + pollingColumn + "'," +
                " jdbc.driver.name='" + oracleJdbcDriverName + "'," +
                " url = '" + databaseURL + "'," +
                " username = '" + username + "'," +
                " password = '" + password + "'," +
                " table.name = '" + pollingTableName + "', polling.interval = '" + pollingInterval + "'," +
                " @map(type='keyvalue'))" +
                "define stream istm (id string, name string);";

        String rdbmsStoreDefinition = "define stream insertionStream (id string, name string);" +
                "@Store(type='rdbms', jdbc.url='" + databaseURL + "'," +
                " username='" + username + "', password='" + password + "' ," +
                " jdbc.driver.name='" + oracleJdbcDriverName + "')" +
                "define table login (id string, name string);";

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

        SiddhiAppRuntime rdbmsAppRuntime = siddhiManager.createSiddhiAppRuntime(rdbmsStoreDefinition + rdbmsQuery);
        rdbmsAppRuntime.addCallback("query2", rdbmsQueryCallback);
        rdbmsAppRuntime.start();

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

        //Do an insert and wait for cdc app to capture.
        InputHandler rdbmsInputHandler = rdbmsAppRuntime.getInputHandler("insertionStream");
        Object[] insertingObject = new Object[]{"e002", "testEmployer"};
        rdbmsInputHandler.send(insertingObject);

        //wait polling interval + 200 ms.
        Thread.sleep(pollingInterval + 200);

        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);

        //Assert event arrival.
        Assert.assertTrue(eventArrived.get());

        //Assert event data.
        Assert.assertEquals(insertingObject, currentEvent.getData());

        cdcAppRuntime.shutdown();
        rdbmsAppRuntime.shutdown();
        siddhiManager.shutdown();
    }
}
