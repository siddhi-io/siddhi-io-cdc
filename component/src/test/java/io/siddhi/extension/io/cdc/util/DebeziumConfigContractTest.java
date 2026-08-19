/*
 * Copyright (c) 2026, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.extension.io.cdc.util;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;
import io.debezium.connector.mongodb.MongoDbFieldName;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.data.Envelope;
import io.debezium.embedded.EmbeddedEngineConfig;
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.storage.file.history.FileSchemaHistory;
import io.siddhi.extension.io.cdc.source.listening.WrongConfigurationException;
import org.apache.kafka.connect.source.SourceConnector;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Pins the Debezium and Kafka Connect configuration keys this extension hardcodes against the constants those
 * libraries actually publish.
 * <p>
 * Debezium silently ignores configuration properties it does not recognise, so a renamed key produces a source that
 * starts up, logs nothing and emits no events. Nothing else in the test suite can detect that. These tests are
 * deliberately cheap and database free so that they can be run before and after a dependency upgrade.
 */
public class DebeziumConfigContractTest {

    private static final String HISTORY_FILE_DIRECTORY = "/tmp/cdc/history/";
    private static final String SIDDHI_APP_NAME = "testApp";
    private static final String STREAM_NAME = "testStream";

    /**
     * Keys that are consumed by the embedded engine or the schema history storage module rather than by a connector,
     * so they legitimately do not appear in any connector ConfigDef. Each is separately pinned to its owning
     * constant by {@link #engineAndStorageKeysMatchDebeziumConstants()}.
     */
    private static final Set<String> NON_CONNECTOR_KEYS = new HashSet<>(Arrays.asList(
            CDCSourceConstants.CONNECTOR_CLASS,
            CDCSourceConstants.CONNECTOR_NAME,
            CDCSourceConstants.OFFSET_STORAGE,
            CDCSourceConstants.DATABASE_HISTORY,
            CDCSourceConstants.DATABASE_HISTORY_FILE_NAME,
            CDCSourceConstants.CDC_SOURCE_OBJECT));

    @Test
    public void connectorLevelKeysMatchDebeziumConstants() {
        Assert.assertEquals(CDCSourceConstants.DATABASE_HOSTNAME, RelationalDatabaseConnectorConfig.HOSTNAME.name());
        Assert.assertEquals(CDCSourceConstants.DATABASE_PORT, RelationalDatabaseConnectorConfig.PORT.name());
        Assert.assertEquals(CDCSourceConstants.DATABASE_USER, RelationalDatabaseConnectorConfig.USER.name());
        Assert.assertEquals(CDCSourceConstants.DATABASE_PASSWORD, RelationalDatabaseConnectorConfig.PASSWORD.name());
        Assert.assertEquals(CDCSourceConstants.DATABASE_DBNAME,
                RelationalDatabaseConnectorConfig.DATABASE_NAME.name());
        Assert.assertEquals(CDCSourceConstants.TABLE_TABLE_INCLUDE_LIST,
                RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST.name());
        Assert.assertEquals(CDCSourceConstants.DATABASE_SERVER_NAME, CommonConnectorConfig.TOPIC_PREFIX.name());
        Assert.assertEquals(CDCSourceConstants.DATABASE_SERVER_ID, BinlogConnectorConfig.SERVER_ID.name());
        Assert.assertEquals(CDCSourceConstants.PLUGIN_NAME, PostgresConnectorConfig.PLUGIN_NAME.name());
        Assert.assertEquals(CDCSourceConstants.SQLSERVER_DATABASE_NAMES,
                SqlServerConnectorConfig.DATABASE_NAMES.name());
        Assert.assertEquals(CDCSourceConstants.MONGODB_CONNECTION_STRING,
                MongoDbConnectorConfig.CONNECTION_STRING.name());
        Assert.assertEquals(CDCSourceConstants.MONGODB_COLLECTION_INCLUDE_LIST,
                MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST.name());
        Assert.assertEquals(CDCSourceConstants.MONGODB_USER, MongoDbConnectorConfig.USER.name());
        Assert.assertEquals(CDCSourceConstants.MONGODB_PASSWORD, MongoDbConnectorConfig.PASSWORD.name());
    }

    @Test
    public void engineAndStorageKeysMatchDebeziumConstants() {
        Assert.assertEquals(CDCSourceConstants.CONNECTOR_NAME, EmbeddedEngineConfig.ENGINE_NAME.name());
        Assert.assertEquals(CDCSourceConstants.CONNECTOR_CLASS, EmbeddedEngineConfig.CONNECTOR_CLASS.name());
        Assert.assertEquals(CDCSourceConstants.OFFSET_STORAGE, EmbeddedEngineConfig.OFFSET_STORAGE.name());
        Assert.assertEquals(CDCSourceConstants.DATABASE_HISTORY,
                HistorizedRelationalDatabaseConnectorConfig.SCHEMA_HISTORY.name());
        Assert.assertEquals(CDCSourceConstants.DATABASE_HISTORY_FILE_NAME, FileSchemaHistory.FILE_PATH.name());
        Assert.assertEquals(CDCSourceConstants.DATABASE_HISTORY_FILEBASE_HISTORY, FileSchemaHistory.class.getName());
    }

    @Test
    public void changeEventFieldNamesMatchDebeziumConstants() {
        Assert.assertEquals(CDCSourceConstants.CONNECT_RECORD_OPERATION, Envelope.FieldName.OPERATION);
        Assert.assertEquals(CDCSourceConstants.BEFORE, Envelope.FieldName.BEFORE);
        Assert.assertEquals(CDCSourceConstants.AFTER, Envelope.FieldName.AFTER);
        Assert.assertEquals(CDCSourceConstants.SOURCE_SCHEMA, Envelope.FieldName.SOURCE);
        Assert.assertEquals(CDCSourceConstants.EVENT_TIMESTAMP, Envelope.FieldName.TIMESTAMP);
        Assert.assertEquals(CDCSourceConstants.MONGO_UPDATE_DESCRIPTION, MongoDbFieldName.UPDATE_DESCRIPTION);
        Assert.assertEquals(CDCSourceConstants.MONGO_UPDATED_FIELDS, MongoDbFieldName.UPDATED_FIELDS);
    }

    @Test
    public void changeEventOperationCodesMatchDebeziumConstants() {
        Assert.assertEquals(CDCSourceConstants.CONNECT_RECORD_INSERT_OPERATION, Envelope.Operation.CREATE.code());
        Assert.assertEquals(CDCSourceConstants.CONNECT_RECORD_UPDATE_OPERATION, Envelope.Operation.UPDATE.code());
        Assert.assertEquals(CDCSourceConstants.CONNECT_RECORD_DELETE_OPERATION, Envelope.Operation.DELETE.code());
        Assert.assertEquals(CDCSourceConstants.CONNECT_RECORD_INITIAL_SYNC, Envelope.Operation.READ.code());
    }

    @DataProvider(name = "connectorClasses")
    public Object[][] connectorClasses() {
        return new Object[][]{
                {CDCSourceConstants.MYSQL_CONNECTOR_CLASS},
                {CDCSourceConstants.POSTGRESQL_CONNECTOR_CLASS},
                {CDCSourceConstants.ORACLE_CONNECTOR_CLASS},
                {CDCSourceConstants.SQLSERVER_CONNECTOR_CLASS},
                {CDCSourceConstants.MONGODB_CONNECTOR_CLASS}
        };
    }

    /**
     * A connector class that has been moved or renamed fails only when a Siddhi app starts, so resolve them up front.
     */
    @Test(dataProvider = "connectorClasses")
    public void connectorClassesResolveAndAreSourceConnectors(String connectorClass) throws Exception {
        Class<?> resolved = Class.forName(connectorClass);
        Assert.assertTrue(SourceConnector.class.isAssignableFrom(resolved),
                connectorClass + " is no longer a Kafka Connect SourceConnector");
    }

    @DataProvider(name = "jdbcUrls")
    public Object[][] jdbcUrls() {
        // The final column is the set of emitted keys the connector does not recognise. database.server.id is a
        // MySQL binlog setting that getConfigMap emits unconditionally; Debezium 2.x ignores it everywhere else.
        return new Object[][]{
                {"jdbc:mysql://localhost:3306/SimpleDB", CDCSourceConstants.MYSQL_CONNECTOR_CLASS, "",
                        Collections.emptyList()},
                {"jdbc:postgresql://localhost:5432/SimpleDB", CDCSourceConstants.POSTGRESQL_CONNECTOR_CLASS, "",
                        Collections.singletonList(CDCSourceConstants.DATABASE_SERVER_ID)},
                {"jdbc:sqlserver://localhost:1433;databaseName=SimpleDB",
                        CDCSourceConstants.SQLSERVER_CONNECTOR_CLASS, "",
                        Collections.singletonList(CDCSourceConstants.DATABASE_SERVER_ID)},
                {"jdbc:oracle:thin:@localhost:1521/XE", CDCSourceConstants.ORACLE_CONNECTOR_CLASS,
                        CDCSourceConstants.ORACLE_OUTSERVER_PROPERTY_NAME + "=dbzxout",
                        Collections.singletonList(CDCSourceConstants.DATABASE_SERVER_ID)},
                {"jdbc:mongodb://rs0/localhost:27017/SimpleDB", CDCSourceConstants.MONGODB_CONNECTOR_CLASS, "",
                        Collections.singletonList(CDCSourceConstants.DATABASE_SERVER_ID)}
        };
    }

    /**
     * Pins which keys {@code getConfigMap} emits that the target connector does not understand. Debezium drops
     * unknown properties without complaint, so a key renamed or removed by an upgrade shows up here as a new entry,
     * and a key that starts being recognised shows up as a missing one.
     * <p>
     * The expectation is not empty today: {@code database.server.id} is emitted to every connector even though only
     * the MySQL binlog connector defines it. That is harmless on Debezium 2.x but worth revisiting on an upgrade that
     * validates configuration more strictly.
     */
    @Test(dataProvider = "jdbcUrls")
    public void emittedKeysTheConnectorIgnoresAreThoseExpected(String url, String connectorClass,
                                                               String connectorProperties,
                                                               List<String> expectedUnrecognised) throws Exception {
        Map<String, Object> configMap = CDCSourceUtil.getConfigMap("user", "pass", url, "login",
                HISTORY_FILE_DIRECTORY, SIDDHI_APP_NAME, STREAM_NAME, 5555, "", connectorProperties, 1234, null,
                CDCSourceConstants.DECORDERBUFS_PLUGIN);

        Assert.assertEquals(configMap.get(CDCSourceConstants.CONNECTOR_CLASS), connectorClass);

        SourceConnector connector = (SourceConnector) Class.forName(connectorClass)
                .getDeclaredConstructor().newInstance();
        Set<String> knownKeys = connector.config().configKeys().keySet();

        List<String> unrecognised = new ArrayList<>();
        for (String key : configMap.keySet()) {
            if (!NON_CONNECTOR_KEYS.contains(key) && !knownKeys.contains(key)) {
                unrecognised.add(key);
            }
        }
        Collections.sort(unrecognised);
        List<String> expected = new ArrayList<>(expectedUnrecognised);
        Collections.sort(expected);

        Assert.assertEquals(unrecognised, expected,
                "Change in the keys emitted for " + url + " that " + connectorClass + " does not recognise, and "
                        + "which Debezium will therefore silently ignore");
    }

    /**
     * Documents that the schema history settings are emitted even to MongoDB, which keeps no relational schema
     * history. Unlike {@code database.server.id} these are storage level keys, so they never appear in a connector
     * {@code ConfigDef} and are not covered by {@link #emittedKeysTheConnectorIgnoresAreThoseExpected}.
     */
    @Test
    public void schemaHistoryKeysAreStillEmittedForMongoDb() throws WrongConfigurationException {
        Map<String, Object> configMap = CDCSourceUtil.getConfigMap("user", "pass",
                "jdbc:mongodb://rs0/localhost:27017/SimpleDB", "login", HISTORY_FILE_DIRECTORY, SIDDHI_APP_NAME,
                STREAM_NAME, 5555, "", "", 1234, null, CDCSourceConstants.DECORDERBUFS_PLUGIN);

        Assert.assertTrue(configMap.containsKey(CDCSourceConstants.DATABASE_HISTORY));
        Assert.assertTrue(configMap.containsKey(CDCSourceConstants.DATABASE_HISTORY_FILE_NAME));
    }
}
