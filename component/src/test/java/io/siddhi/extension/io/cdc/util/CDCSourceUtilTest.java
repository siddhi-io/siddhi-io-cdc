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

import io.siddhi.extension.io.cdc.source.listening.InMemoryOffsetBackingStore;
import io.siddhi.extension.io.cdc.source.listening.WrongConfigurationException;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Golden tests for the Debezium configuration {@link CDCSourceUtil#getConfigMap} assembles for each supported
 * database. The whole map is asserted rather than individual entries, so that an upgrade which adds, drops or renames
 * an entry shows up as a diff instead of passing unnoticed.
 *
 * @see DebeziumConfigContractTest for the checks that the keys themselves are still the ones Debezium expects.
 */
public class CDCSourceUtilTest {

    private static final String HISTORY_FILE_DIRECTORY = "/tmp/cdc/history/";
    private static final String SIDDHI_APP_NAME = "testApp";
    private static final String STREAM_NAME = "testStream";
    private static final String TABLE_NAME = "login";
    private static final int SERVER_ID = 5555;
    private static final int SOURCE_HASH_CODE = 1234;

    private Map<String, Object> configMapFor(String url, String connectorProperties)
            throws WrongConfigurationException {
        return CDCSourceUtil.getConfigMap("myuser", "mypassword", url, TABLE_NAME, HISTORY_FILE_DIRECTORY,
                SIDDHI_APP_NAME, STREAM_NAME, SERVER_ID, "", connectorProperties, SOURCE_HASH_CODE, null,
                CDCSourceConstants.DECORDERBUFS_PLUGIN);
    }

    /**
     * The settings every connector receives regardless of the database type.
     */
    private Map<String, Object> commonEntries(String expectedTopicPrefix) {
        Map<String, Object> expected = new HashMap<>();
        expected.put(CDCSourceConstants.DATABASE_SERVER_ID, SERVER_ID);
        expected.put(CDCSourceConstants.DATABASE_SERVER_NAME, expectedTopicPrefix);
        expected.put(CDCSourceConstants.OFFSET_STORAGE, InMemoryOffsetBackingStore.class.getName());
        expected.put(CDCSourceConstants.CDC_SOURCE_OBJECT, SOURCE_HASH_CODE);
        expected.put(CDCSourceConstants.DATABASE_HISTORY, CDCSourceConstants.DATABASE_HISTORY_FILEBASE_HISTORY);
        expected.put(CDCSourceConstants.DATABASE_HISTORY_FILE_NAME, HISTORY_FILE_DIRECTORY + STREAM_NAME + ".dat");
        expected.put(CDCSourceConstants.CONNECTOR_NAME, SIDDHI_APP_NAME + STREAM_NAME);
        return expected;
    }

    @Test
    public void mysqlUrlProducesTheExpectedConfiguration() throws WrongConfigurationException {
        Map<String, Object> expected = commonEntries("localhost_3306");
        expected.put(CDCSourceConstants.DATABASE_HOSTNAME, "localhost");
        expected.put(CDCSourceConstants.DATABASE_PORT, 3306);
        expected.put(CDCSourceConstants.TABLE_TABLE_INCLUDE_LIST, "SimpleDB." + TABLE_NAME);
        expected.put(CDCSourceConstants.CONNECTOR_CLASS, CDCSourceConstants.MYSQL_CONNECTOR_CLASS);
        expected.put(CDCSourceConstants.DATABASE_USER, "myuser");
        expected.put(CDCSourceConstants.DATABASE_PASSWORD, "mypassword");

        Assert.assertEquals(configMapFor("jdbc:mysql://localhost:3306/SimpleDB", ""), expected);
    }

    @Test
    public void postgresUrlProducesTheExpectedConfiguration() throws WrongConfigurationException {
        Map<String, Object> expected = commonEntries("localhost_5432");
        expected.put(CDCSourceConstants.DATABASE_HOSTNAME, "localhost");
        expected.put(CDCSourceConstants.DATABASE_PORT, 5432);
        expected.put(CDCSourceConstants.DATABASE_DBNAME, "SimpleDB");
        expected.put(CDCSourceConstants.TABLE_TABLE_INCLUDE_LIST, TABLE_NAME);
        expected.put(CDCSourceConstants.PLUGIN_NAME, CDCSourceConstants.DECORDERBUFS_PLUGIN);
        expected.put(CDCSourceConstants.CONNECTOR_CLASS, CDCSourceConstants.POSTGRESQL_CONNECTOR_CLASS);
        expected.put(CDCSourceConstants.DATABASE_USER, "myuser");
        expected.put(CDCSourceConstants.DATABASE_PASSWORD, "mypassword");

        Assert.assertEquals(configMapFor("jdbc:postgresql://localhost:5432/SimpleDB", ""), expected);
    }

    @Test
    public void sqlServerUrlProducesTheExpectedConfiguration() throws WrongConfigurationException {
        Map<String, Object> expected = commonEntries("localhost_1433");
        expected.put(CDCSourceConstants.DATABASE_HOSTNAME, "localhost");
        expected.put(CDCSourceConstants.DATABASE_PORT, 1433);
        expected.put(CDCSourceConstants.TABLE_TABLE_INCLUDE_LIST, TABLE_NAME);
        expected.put(CDCSourceConstants.SQLSERVER_DATABASE_NAMES, "SimpleDB");
        expected.put(CDCSourceConstants.CONNECTOR_CLASS, CDCSourceConstants.SQLSERVER_CONNECTOR_CLASS);
        expected.put(CDCSourceConstants.DATABASE_USER, "myuser");
        expected.put(CDCSourceConstants.DATABASE_PASSWORD, "mypassword");

        Assert.assertEquals(configMapFor("jdbc:sqlserver://localhost:1433;databaseName=SimpleDB", ""), expected);
    }

    @Test
    public void oracleUrlProducesTheExpectedConfiguration() throws WrongConfigurationException {
        Map<String, Object> expected = commonEntries("localhost_1521");
        expected.put(CDCSourceConstants.DATABASE_HOSTNAME, "localhost");
        expected.put(CDCSourceConstants.DATABASE_PORT, 1521);
        expected.put(CDCSourceConstants.TABLE_TABLE_INCLUDE_LIST, TABLE_NAME);
        expected.put(CDCSourceConstants.DATABASE_DBNAME, "XE");
        expected.put(CDCSourceConstants.CONNECTOR_CLASS, CDCSourceConstants.ORACLE_CONNECTOR_CLASS);
        expected.put(CDCSourceConstants.DATABASE_USER, "myuser");
        expected.put(CDCSourceConstants.DATABASE_PASSWORD, "mypassword");
        expected.put(CDCSourceConstants.ORACLE_OUTSERVER_PROPERTY_NAME, "dbzxout");

        Assert.assertEquals(configMapFor("jdbc:oracle:thin:@localhost:1521/XE",
                CDCSourceConstants.ORACLE_OUTSERVER_PROPERTY_NAME + "=dbzxout"), expected);
    }

    /**
     * Records existing behaviour rather than endorsing it: the SID form of an Oracle URL, which is what the Oracle
     * integration profile uses, leaves {@code database.dbname} empty because the URL pattern only recognises a SID
     * introduced by a slash.
     */
    @Test
    public void oracleSidUrlLeavesTheDatabaseNameEmpty() throws WrongConfigurationException {
        Map<String, Object> configMap = configMapFor("jdbc:oracle:thin:@localhost:1521:XE",
                CDCSourceConstants.ORACLE_OUTSERVER_PROPERTY_NAME + "=dbzxout");

        Assert.assertEquals(configMap.get(CDCSourceConstants.DATABASE_DBNAME), "");
        Assert.assertEquals(configMap.get(CDCSourceConstants.DATABASE_HOSTNAME), "localhost");
        Assert.assertEquals(configMap.get(CDCSourceConstants.DATABASE_PORT), 1521);
    }

    @Test(expectedExceptions = WrongConfigurationException.class)
    public void oracleWithoutTheOutServerPropertyIsRejected() throws WrongConfigurationException {
        configMapFor("jdbc:oracle:thin:@localhost:1521/XE", "");
    }

    @Test
    public void mongoReplicaSetUrlProducesTheExpectedConfiguration() throws WrongConfigurationException {
        Map<String, Object> expected = commonEntries("rs0/localhost_27017");
        expected.put(CDCSourceConstants.CONNECTOR_CLASS, CDCSourceConstants.MONGODB_CONNECTOR_CLASS);
        expected.put(CDCSourceConstants.MONGODB_CONNECTION_STRING, "mongodb://localhost:27017/?replicaSet=rs0");
        expected.put(CDCSourceConstants.MONGODB_COLLECTION_INCLUDE_LIST, "SimpleDB." + TABLE_NAME);
        expected.put(CDCSourceConstants.MONGODB_USER, "myuser");
        expected.put(CDCSourceConstants.MONGODB_PASSWORD, "mypassword");

        Assert.assertEquals(configMapFor("jdbc:mongodb://rs0/localhost:27017/SimpleDB", ""), expected);
    }

    @Test
    public void mongoUrlWithoutAReplicaSetProducesTheExpectedConfiguration() throws WrongConfigurationException {
        Map<String, Object> expected = commonEntries("localhost_27017");
        expected.put(CDCSourceConstants.CONNECTOR_CLASS, CDCSourceConstants.MONGODB_CONNECTOR_CLASS);
        expected.put(CDCSourceConstants.MONGODB_CONNECTION_STRING, "mongodb://localhost:27017/");
        expected.put(CDCSourceConstants.MONGODB_COLLECTION_INCLUDE_LIST, "SimpleDB." + TABLE_NAME);
        expected.put(CDCSourceConstants.MONGODB_USER, "myuser");
        expected.put(CDCSourceConstants.MONGODB_PASSWORD, "mypassword");

        Assert.assertEquals(configMapFor("jdbc:mongodb://localhost:27017/SimpleDB", ""), expected);
    }

    /**
     * MongoDB authenticates through the dedicated mongodb.* settings, not the relational ones.
     */
    @Test
    public void mongoCredentialsDoNotUseTheRelationalKeys() throws WrongConfigurationException {
        Map<String, Object> configMap = configMapFor("jdbc:mongodb://localhost:27017/SimpleDB", "");

        Assert.assertFalse(configMap.containsKey(CDCSourceConstants.DATABASE_USER));
        Assert.assertFalse(configMap.containsKey(CDCSourceConstants.DATABASE_PASSWORD));
    }

    @Test
    public void explicitServerNameOverridesTheHostPortDefault() throws WrongConfigurationException {
        Map<String, Object> configMap = CDCSourceUtil.getConfigMap("myuser", "mypassword",
                "jdbc:mysql://localhost:3306/SimpleDB", TABLE_NAME, HISTORY_FILE_DIRECTORY, SIDDHI_APP_NAME,
                STREAM_NAME, SERVER_ID, "myTopicPrefix", "", SOURCE_HASH_CODE, null,
                CDCSourceConstants.DECORDERBUFS_PLUGIN);

        Assert.assertEquals(configMap.get(CDCSourceConstants.DATABASE_SERVER_NAME), "myTopicPrefix");
    }

    @Test
    public void defaultServerIdIsRandomisedWithinTheExpectedRange() throws WrongConfigurationException {
        for (int i = 0; i < 50; i++) {
            Map<String, Object> configMap = CDCSourceUtil.getConfigMap("myuser", "mypassword",
                    "jdbc:mysql://localhost:3306/SimpleDB", TABLE_NAME, HISTORY_FILE_DIRECTORY, SIDDHI_APP_NAME,
                    STREAM_NAME, CDCSourceConstants.DEFAULT_SERVER_ID, "", "", SOURCE_HASH_CODE, null,
                    CDCSourceConstants.DECORDERBUFS_PLUGIN);

            int serverId = (Integer) configMap.get(CDCSourceConstants.DATABASE_SERVER_ID);
            Assert.assertTrue(serverId >= 5400 && serverId <= 6400, "Unexpected generated server id: " + serverId);
        }
    }

    @Test
    public void connectorPropertiesAreAppendedToTheConfiguration() throws WrongConfigurationException {
        Map<String, Object> configMap = configMapFor("jdbc:mysql://localhost:3306/SimpleDB",
                "snapshot.mode=schema_only,max.batch.size=1024");

        Assert.assertEquals(configMap.get("snapshot.mode"), "schema_only");
        Assert.assertEquals(configMap.get("max.batch.size"), "1024");
    }

    @Test
    public void connectorPropertiesCanOverrideAGeneratedSetting() throws WrongConfigurationException {
        Map<String, Object> configMap = configMapFor("jdbc:mysql://localhost:3306/SimpleDB",
                CDCSourceConstants.DATABASE_SERVER_NAME + "=overridden");

        Assert.assertEquals(configMap.get(CDCSourceConstants.DATABASE_SERVER_NAME), "overridden");
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void malformedConnectorPropertiesAreRejected() throws WrongConfigurationException {
        configMapFor("jdbc:mysql://localhost:3306/SimpleDB", "snapshot.mode");
    }

    @Test(expectedExceptions = WrongConfigurationException.class)
    public void nonJdbcUrlIsRejected() throws WrongConfigurationException {
        configMapFor("mysql://localhost:3306/SimpleDB", "");
    }

    @Test(expectedExceptions = WrongConfigurationException.class)
    public void unsupportedDatabaseSchemeIsRejected() throws WrongConfigurationException {
        configMapFor("jdbc:db2://localhost:50000/SimpleDB", "");
    }

    @Test(expectedExceptions = WrongConfigurationException.class)
    public void mysqlUrlWithoutAPortIsRejected() throws WrongConfigurationException {
        configMapFor("jdbc:mysql://localhost/SimpleDB", "");
    }

    @Test(expectedExceptions = WrongConfigurationException.class)
    public void sqlServerUrlWithoutADatabaseNameIsRejected() throws WrongConfigurationException {
        configMapFor("jdbc:sqlserver://localhost:1433", "");
    }

    @Test
    public void carbonHomeFallsBackToTheWorkingDirectory() {
        Assert.assertEquals(CDCSourceUtil.getCarbonHome(), System.getProperty(CDCSourceConstants.USER_DIRECTORY));
    }
}
