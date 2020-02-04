package io.siddhi.extension.io.cdc.source;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.SiddhiTestHelper;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TestCaseOfCDCListeningModeMongo {

    private static final Logger log = Logger.getLogger(TestCaseOfCDCListeningModeMongo.class);
    private Event currentEvent;
    private AtomicInteger eventCount = new AtomicInteger(0);
    private AtomicBoolean eventArrived = new AtomicBoolean(false);
    private int waitTime = 200;
    private int timeout = 10000;
    private String username;
    private String password;
    private String databaseUri;
    private String replicaSetUri;
    private String collectionName = "SweetProductionTable";

    @BeforeClass
    public void initializeConnectionParams() {
        databaseUri = "mongodb://<host>:<port>/<collection_name>";
        replicaSetUri = "jdbc:mongodb://<replica_set_name>/<host>:<port>/<database_name>";
        username = "user_name";
        password = "password";
    }

    @BeforeMethod
    public void init() {
        eventCount.set(0);
        eventArrived.set(false);
        currentEvent = new Event();
    }

    /**
     * Test case to Capture Insert operations from a Mongo table.
     */
    @Test
    public void testInsertCDC() throws InterruptedException {
        log.info("------------------------------------------------------------------------------------------------");
        log.info("CDC TestCase: Capturing Insert change data from Mongo.");
        log.info("------------------------------------------------------------------------------------------------");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cdcinStreamDefinition = "@app:name('cdcTesting')" +
                "@source(type = 'cdc'," +
                " url = '" + replicaSetUri + "'," +
                " username = '" + username + "'," +
                " password = '" + password + "'," +
                " table.name = '" + collectionName + "', " +
                " operation = 'insert', " +
                " @map(type='keyvalue'))" +
                "define stream istm (name string, amount double, volume int);";

        String mongoStoreDefinition = "define stream insertionStream (name string, amount double, volume int);" +
                "@Store(type='mongodb', mongodb.uri='" + databaseUri + "')" +
                "define table SweetProductionTable (name string, amount double, volume int);";

        String mongoQuery = "@info(name='query2') " +
                "from insertionStream " +
                "insert into SweetProductionTable;";

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

        SiddhiAppRuntime mongoAppRuntime = siddhiManager.createSiddhiAppRuntime(mongoStoreDefinition + mongoQuery);
        mongoAppRuntime.addCallback("query2", rdbmsQueryCallback);
        mongoAppRuntime.start();

        //Do an insert and wait for cdc app to capture.
        InputHandler rdbmsInputHandler = mongoAppRuntime.getInputHandler("insertionStream");
        Object[] insertingObject = new Object[]{"e001", 100.00, 5};
        rdbmsInputHandler.send(insertingObject);
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);

        //Assert event arrival.
        Assert.assertTrue(eventArrived.get());

        //Assert event data.
        Assert.assertEquals(insertingObject, currentEvent.getData());

        cdcAppRuntime.shutdown();
        mongoAppRuntime.shutdown();
        siddhiManager.shutdown();
    }

    /**
     * Test case to Capture Delete operations from a Mongo table.
     */
    @Test
    public void testDeleteCDC() throws InterruptedException {
        log.info("------------------------------------------------------------------------------------------------");
        log.info("CDC TestCase: Capturing Delete change data from Mongo.");
        log.info("------------------------------------------------------------------------------------------------");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cdcinStreamDefinition = "@app:name('cdcTesting')" +
                "@source(type = 'cdc'," +
                " url = '" + replicaSetUri + "'," +
                " username = '" + username + "'," +
                " password = '" + password + "'," +
                " table.name = '" + collectionName + "', " +
                " operation = 'delete', " +
                " @map(type='keyvalue'))" +
                "define stream delstm (id string);";


        String mongoStoreDefinition = "define stream DeletionStream (name string, amount double);" +
                "define stream InsertStream(name string, amount double);" +
                "@Store(type='mongodb', mongodb.uri='" + databaseUri + "')" +
                "define table SweetProductionTable (name string, amount double);";

        String insertQuery = "@info(name='query3') " +
                "from InsertStream " +
                "insert into SweetProductionTable;";

        String deleteQuery = "@info(name='queryDel') " +
                "from DeletionStream " +
                "delete SweetProductionTable on SweetProductionTable.name==name;";

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

        QueryCallback mongoQuerycallback = new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    eventCount.getAndIncrement();
                    log.info("delete done: " + event);
                }
            }
        };

        SiddhiAppRuntime mongoAppRuntime = siddhiManager.createSiddhiAppRuntime(mongoStoreDefinition + insertQuery
                + deleteQuery);
        mongoAppRuntime.addCallback("queryDel", mongoQuerycallback);
        mongoAppRuntime.start();

        //Do an insert first.
        InputHandler mongoInputHandler = mongoAppRuntime.getInputHandler("InsertStream");
        Object[] insertingObject = new Object[]{"e001", 100.00};
        mongoInputHandler.send(insertingObject);

        //wait to complete deletion
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        eventCount.getAndDecrement();

        //Delete inserted row
        mongoInputHandler = mongoAppRuntime.getInputHandler("DeletionStream");
        Object[] deletingObject = new Object[]{"e001"};
        mongoInputHandler.send(deletingObject);

        //wait to capture the delete event.
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);

        //Assert event arrival.
        Assert.assertTrue(eventArrived.get());

        cdcAppRuntime.shutdown();
        mongoAppRuntime.shutdown();
        siddhiManager.shutdown();
    }

    /**
     * Test case to Capture Update operations from a Mongo table.
     */
    @Test
    public void testUpdateCDC() throws InterruptedException {
        log.info("------------------------------------------------------------------------------------------------");
        log.info("CDC TestCase: Capturing Update change data from Mongo.");
        log.info("------------------------------------------------------------------------------------------------");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cdcinStreamDefinition = "@app:name('cdcTesting')" +
                "@source(type = 'cdc'," +
                " url = '" + replicaSetUri + "'," +
                " username = '" + username + "'," +
                " password = '" + password + "'," +
                " table.name = '" + collectionName + "', " +
                " operation = 'update', " +
                " @map(type='keyvalue'))" +
                "define stream updatestm (id string, amount double);";

        String mongoStoreDefinition = "define stream UpdateStream(name string, amount double);" +
                "define stream InsertStream(name string, amount double);" +
                "@Store(type='mongodb', mongodb.uri='" + databaseUri + "')" +
                "define table SweetProductionTable (name string, amount double);";

        String insertQuery = "@info(name='query3') " +
                "from InsertStream " +
                "insert into SweetProductionTable;";

        String updateQuery = "@info(name='queryUpdate') " +
                "from UpdateStream " +
                "select name, amount " +
                "update SweetProductionTable " +
                "set SweetProductionTable.amount = amount " +
                "on SweetProductionTable.name == name;";

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
        SiddhiAppRuntime mongoAppRuntime = siddhiManager.createSiddhiAppRuntime(mongoStoreDefinition + insertQuery
                + updateQuery);
        mongoAppRuntime.addCallback("queryUpdate", queryCallback2);
        mongoAppRuntime.start();

        //Do an insert first.
        InputHandler mongoInputHandler = mongoAppRuntime.getInputHandler("InsertStream");
        Object[] insertingObject = new Object[]{"sweets", 100.00};
        mongoInputHandler.send(insertingObject);

        Thread.sleep(1000);

        //Update inserted row.
        mongoInputHandler = mongoAppRuntime.getInputHandler("UpdateStream");
        Object[] updatingObject = new Object[]{"sweets", 500.00};
        mongoInputHandler.send(updatingObject);

        //wait to capture the update event.
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);

        //Assert event arrival.
        Assert.assertTrue(eventArrived.get());

        cdcAppRuntime.shutdown();
        mongoAppRuntime.shutdown();
        siddhiManager.shutdown();
    }
}
