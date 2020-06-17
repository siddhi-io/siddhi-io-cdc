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

package io.siddhi.extension.io.cdc.util;

/**
 * CDC source constants
 */
public class CDCSourceConstants {
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String DATABASE_CONNECTION_URL = "url";
    public static final String POOL_PROPERTIES = "pool.properties";
    public static final String OPERATION = "operation";
    public static final String DATABASE_HISTORY_FILEBASE_HISTORY = "io.debezium.relational.history.FileDatabaseHistory";
    public static final String DATABASE_HISTORY_FILE_NAME = "database.history.file.filename";
    public static final String DATABASE_SERVER_NAME = "database.server.name";
    public static final String DATABASE_SERVER_ID = "database.server.id";
    public static final String SERVER_ID = "server.id";
    public static final String TABLE_NAME = "table.name";
    public static final String CONNECTOR_PROPERTIES = "connector.properties";
    public static final String EMPTY_STRING = "";
    public static final String INSERT = "insert";
    public static final String UPDATE = "update";
    public static final String DELETE = "delete";
    public static final String CONNECTOR_CLASS = "connector.class";
    public static final String DATABASE_PORT = "database.port";
    public static final String TABLE_WHITELIST = "table.whitelist";
    public static final String DATABASE_DBNAME = "database.dbname";
    public static final String DATABASE_HOSTNAME = "database.hostname";
    public static final String DATABASE_USER = "database.user";
    public static final String DATABASE_PASSWORD = "database.password";
    public static final String OFFSET_STORAGE = "offset.storage";
    public static final String CDC_SOURCE_OBJECT = "cdc.source.object";
    public static final String DATABASE_HISTORY = "database.history";
    public static final String MYSQL_CONNECTOR_CLASS = "io.debezium.connector.mysql.MySqlConnector";
    public static final String POSTGRESQL_CONNECTOR_CLASS = "io.debezium.connector.postgresql.PostgresConnector";
    public static final String ORACLE_CONNECTOR_CLASS = "io.debezium.connector.oracle.OracleConnector";
    public static final String SQLSERVER_CONNECTOR_CLASS = "io.debezium.connector.sqlserver.SqlServerConnector";
    public static final String MONGODB_CONNECTOR_CLASS = "io.debezium.connector.mongodb.MongoDbConnector";
    public static final String BEFORE_PREFIX = "before_";
    public static final String CACHE_OBJECT = "cacheObj";
    public static final int DEFAULT_SERVER_ID = -1;
    public static final String CONNECT_RECORD_OPERATION = "op";
    public static final String CONNECT_RECORD_INSERT_OPERATION = "c";
    public static final String CONNECT_RECORD_UPDATE_OPERATION = "u";
    public static final String CONNECT_RECORD_DELETE_OPERATION = "d";
    public static final String CONNECT_RECORD_INITIAL_SYNC = "r";
    public static final String BEFORE = "before";
    public static final String AFTER = "after";
    public static final String CARBON_HOME = "carbon.home";
    public static final String USER_DIRECTORY = "user.dir";
    public static final String MODE = "mode";
    public static final String MODE_LISTENING = "listening";
    public static final String MODE_POLLING = "polling";
    public static final String JDBC_DRIVER_NAME = "jdbc.driver.name";
    public static final String POLLING_COLUMN = "polling.column";
    public static final String POLLING_INTERVAL = "polling.interval";
    public static final int DEFAULT_POLLING_INTERVAL_SECONDS = 1;
    public static final String DATASOURCE_NAME = "datasource.name";
    public static final String JNDI_RESOURCE = "jndi.resource";
    public static final String ORACLE_PDB_PROPERTY_NAME = "database.pdb.name";
    public static final String ORACLE_OUTSERVER_PROPERTY_NAME = "database.out.server.name";
    public static final String WAIT_ON_MISSED_RECORD = "wait.on.missed.record";
    public static final String MISSED_RECORD_WAITING_TIMEOUT = "missed.record.waiting.timeout";
    public static final String CONNECTOR_NAME = "name";
    public static final String MONGODB_USER = "mongodb.user";
    public static final String MONGODB_PASSWORD = "mongodb.password";
    public static final String MONGODB_HOSTS = "mongodb.hosts";
    public static final String MONGODB_NAME = "mongodb.name";
    public static final String MONGODB_COLLECTION_WHITELIST = "collection.whitelist";
    public static final String MONGO_COLLECTION_OBJECT_ID = "$oid";
    public static final String MONGO_COLLECTION_ID = "id";
    public static final String MONGO_COLLECTION_INSERT_ID = "_id";
    public static final String MONGO_PATCH = "patch";
    public static final String MONGO_SET = "$set";
    public static final String MONGO_OBJECT_NUMBER_LONG = "$numberLong";
    public static final String MONGO_OBJECT_NUMBER_DECIMAL = "$numberDecimal";

    // Constants for Cron Support
    public static final String CRON_EXPRESSION = "cron.expression";
    public static final String JOB_GROUP = "CDCPollingGroup";
    public static final String JOB_NAME = "CDCPollingJobName";
    public static final String TRIGGER_NAME = "CDCCronTriggerName";
    public static final String TRIGGER_GROUP = "CDCCronTriggerGroup";
    public static final String POLLING_STRATEGY = "PollingStrategy";
}
