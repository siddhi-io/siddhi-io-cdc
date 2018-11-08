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

import io.debezium.embedded.EmbeddedEngine;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.cdc.util.CDCSourceConstants;
import org.wso2.extension.siddhi.io.cdc.util.CDCSourceUtil;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;
import org.wso2.siddhi.core.stream.input.source.Source;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.io.File;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Extension to the siddhi to retrieve Database Changes - implementation of cdc source.
 **/
@Extension(
        name = "cdc",
        namespace = "source",
        description = "The CDC source receives events when a Database table's change event " +
                "(INSERT, UPDATE, DELETE) is triggered. The events are received in key-value format." +
                "\nThe following are key values of the map of a CDC change event and their descriptions." +
                "\n\tFor insert: Keys will be specified table's columns." +
                "\n\tFor delete: Keys will be 'before_' followed by specified table's columns. Eg: before_X." +
                "\n\tFor update: Keys will be specified table's columns and 'before_' followed by specified table's " +
                "columns." +
                "\nFor 'polling' mode: Keys will be specified table's columns." +
                "\nSee parameter: mode for supported databases and change events.",
        parameters = {
                @Parameter(name = "url",
                        description = "Connection url to the database." +
                                "\nuse format: " +
                                "jdbc:mysql://<host>:<port>/<database_name> ",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "mode",
                        description = "Mode to capture the change data. Mode ‘polling’ uses a polling.column to" +
                                " monitor the given table. Mode 'streaming' uses logs to monitor the given table." +
                                "\nThe required parameters are different for each modes." +
                                "\nmode 'streaming' currently supports only MySQL. INSERT, UPDATE, DELETE events" +
                                " can be received." +
                                "\nmode 'polling' supports RDBs. INSERT, UPDATE events can be received.",
                        type = DataType.STRING,
                        defaultValue = "streaming",
                        optional = true
                ),
                @Parameter(
                        name = "jdbc.driver.name",
                        description = "The driver class name for connecting the database." +
                                " Required for ‘polling’ mode.",
                        type = DataType.STRING,
                        defaultValue = "<Empty_String>",
                        optional = true
                ),
                @Parameter(
                        name = "username",
                        description = "Username of a user with SELECT, RELOAD, SHOW DATABASES," +
                                " REPLICATION SLAVE, REPLICATION CLIENT privileges on Change Data Capturing table." +
                                "\nFor polling mode, a user with SELECT privileges.",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "password",
                        description = "Password for the above user.",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "datasource.name",
                        description = "Name of the wso2 datasource to connect to the database." +
                                " When datasource.name is provided, the url, username and password are not needed. " +
                                "Has a more priority over url based connection." +
                                "\nAccepted only when mode is set to 'polling'.",
                        type = DataType.STRING,
                        defaultValue = "<Empty_String>",
                        optional = true
                ),
                @Parameter(
                        name = "table.name",
                        description = "Name of the table which needs to be monitored for data changes.",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "polling.column",
                        description = "Column name on which the polling is done to capture the change data. " +
                                "Provide an AUTO_INCREMENT field or a TIMESTAMP field for better change data" +
                                " capturing. Setting an AUTO_INCREMENT field might fail to receive UPDATE change" +
                                " data." +
                                "\nMandatory when mode is ‘polling’."
                        ,
                        type = DataType.STRING,
                        defaultValue = "<Empty_String>",
                        optional = true
                ),
                @Parameter(
                        name = "polling.interval",
                        description = "The interval in milliseconds to poll the given table for changes." +
                                "\nAccepted only when mode is set to 'polling'."
                        ,
                        type = DataType.INT,
                        defaultValue = "1000",
                        optional = true
                ),
                @Parameter(
                        name = "operation",
                        description = "Interested change event operation. 'insert', 'update' or 'delete'. Required" +
                                " for 'streaming' mode." +
                                "\nNot case sensitive.",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "connector.properties",
                        description = "Debezium connector specified properties as a comma separated string. " +
                                "\nThis properties will have more priority over the parameters. Only for 'streaming'" +
                                " mode",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "Empty_String"
                ),
                @Parameter(name = "database.server.id",
                        description = "For MySQL, a unique integer between 1 to 2^32 as the ID," +
                                " This is used when joining MySQL database cluster to read binlog. Only for" +
                                " 'streaming'mode.",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "Random integer between 5400 and 6400"
                ),
                @Parameter(name = "database.server.name",
                        description = "Logical name that identifies and provides a namespace for the " +
                                "particular database server. Only for 'streaming' mode.",
                        defaultValue = "{host}_{port}",
                        optional = true,
                        type = DataType.STRING
                )
        },
        examples = {
                @Example(
                        syntax = "@source(type = 'cdc' , url = 'jdbc:mysql://localhost:3306/SimpleDB', " +
                                "\nusername = 'cdcuser', password = 'pswd4cdc', " +
                                "\ntable.name = 'students', operation = 'insert', " +
                                "\n@map(type='keyvalue', @attributes(id = 'id', name = 'name')))" +
                                "\ndefine stream inputStream (id string, name string);",
                        description = "In this example, the cdc source starts listening to the row insertions " +
                                " on students table with columns name and id which is under MySQL" +
                                " SimpleDB database that" +
                                " can be accessed with the given url"
                ),
                @Example(
                        syntax = "@source(type = 'cdc' , url = 'jdbc:mysql://localhost:3306/SimpleDB', " +
                                "\nusername = 'cdcuser', password = 'pswd4cdc', " +
                                "\ntable.name = 'students', operation = 'update', " +
                                "\n@map(type='keyvalue', @attributes(id = 'id', name = 'name', " +
                                "\nbefore_id = 'before_id', before_name = 'before_name')))" +
                                "\ndefine stream inputStream (before_id string, id string, " +
                                "\nbefore_name string, ," +
                                " name string);",
                        description = "In this example, the cdc source starts listening to the row updates" +
                                " on students table which is under MySQL SimpleDB database that" +
                                " can be accessed with the given url."
                ),
                @Example(
                        syntax = "@source(type = 'cdc' , url = 'jdbc:mysql://localhost:3306/SimpleDB', " +
                                "\nusername = 'cdcuser', password = 'pswd4cdc', " +
                                "\ntable.name = 'students', operation = 'delete', " +
                                "\n@map(type='keyvalue', @attributes(before_id = 'before_id'," +
                                " before_name = 'before_name')))" +
                                "\ndefine stream inputStream (before_id string, before_name string);",
                        description = "In this example, the cdc source starts listening to the row deletions" +
                                " on students table which is under MySQL SimpleDB database that" +
                                " can be accessed with the given url."
                ),
                @Example(
                        syntax = "@source(type = 'cdc', mode='polling', polling.column = 'id', " +
                                "\njdbc.driver.name = 'com.mysql.jdbc.Driver', " +
                                "url = 'jdbc:mysql://localhost:3306/SimpleDB', " +
                                "\nusername = 'cdcuser', password = 'pswd4cdc', " +
                                "\ntable.name = 'students', " +
                                "\n@map(type='keyvalue'), @attributes(id = 'id', name = 'name'))" +
                                "\ndefine stream istm (id int, name string);",
                        description = "In this example, the cdc source starts polling students table for inserts " +
                                "and updates. polling.column is an auto incremental field. url, username, password, " +
                                "and jdbc.driver.name are used to connect to the database."
                ),
                @Example(
                        syntax = "@source(type = 'cdc', mode='polling', polling.column = 'id', " +
                                "datasource.name = 'SimpleDB'" +
                                "\ntable.name = 'students', " +
                                "\n@map(type='keyvalue'), @attributes(id = 'id', name = 'name'))" +
                                "\ndefine stream istm (id int, name string);",
                        description = "In this example, the cdc source starts polling students table for inserts " +
                                "and updates. polling.column is an auto incremental field. datasource.name is used " +
                                "to connect to the database."
                ),
                @Example(
                        syntax = "@source(type = 'cdc', mode='polling', polling.column = 'last_updated', " +
                                "datasource.name = 'SimpleDB'" +
                                "\ntable.name = 'students', " +
                                "\n@map(type='keyvalue'))" +
                                "\ndefine stream istm (name string);",
                        description = "In this example, the cdc source starts polling students table for inserts " +
                                "and updates. polling.column is a timestamp field."
                ),
        }
)

public class CDCSource extends Source {
    private static final Logger log = Logger.getLogger(CDCSource.class);
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private int pollingInterval;
    private String mode;
    private Map<byte[], byte[]> offsetData = new HashMap<>();
    private String operation;
    private ChangeDataCapture changeDataCapture;
    private String historyFileDirectory;
    private CDCSourceObjectKeeper cdcSourceObjectKeeper = CDCSourceObjectKeeper.getCdcSourceObjectKeeper();
    private String carbonHome;
    private CDCPollar cdcPollar;
    private String lastOffset;

    @Override
    public void init(SourceEventListener sourceEventListener, OptionHolder optionHolder,
                     String[] requestedTransportPropertyNames, ConfigReader configReader,
                     SiddhiAppContext siddhiAppContext) {
        //initialize mode
        mode = optionHolder.validateAndGetStaticValue(CDCSourceConstants.MODE, CDCSourceConstants.MODE_STREAMING);

        //initialize common mandatory parameters
        String tableName = optionHolder.validateAndGetOption(CDCSourceConstants.TABLE_NAME).getValue();

        switch (mode) {
            case CDCSourceConstants.MODE_STREAMING:

                String url = optionHolder.validateAndGetOption(CDCSourceConstants.DATABASE_CONNECTION_URL).getValue();
                String username = optionHolder.validateAndGetOption(CDCSourceConstants.USERNAME).getValue();
                String password = optionHolder.validateAndGetOption(CDCSourceConstants.PASSWORD).getValue();

                String siddhiAppName = siddhiAppContext.getName();
                String streamName = sourceEventListener.getStreamDefinition().getId();

                //initialize mandatory parameters
                operation = optionHolder.validateAndGetOption(CDCSourceConstants.OPERATION).getValue();

                //initialize optional parameters
                int serverID;
                serverID = Integer.parseInt(optionHolder.validateAndGetStaticValue(
                        CDCSourceConstants.DATABASE_SERVER_ID, Integer.toString(CDCSourceConstants.DEFAULT_SERVER_ID)));

                String serverName;
                serverName = optionHolder.validateAndGetStaticValue(CDCSourceConstants.DATABASE_SERVER_NAME,
                        CDCSourceConstants.EMPTY_STRING);

                //initialize parameters from connector.properties
                String connectorProperties = optionHolder.validateAndGetStaticValue(
                        CDCSourceConstants.CONNECTOR_PROPERTIES, CDCSourceConstants.EMPTY_STRING);

                //initialize history file directory
                carbonHome = CDCSourceUtil.getCarbonHome();
                historyFileDirectory = carbonHome + File.separator + "cdc" + File.separator + "history"
                        + File.separator + siddhiAppName + File.separator;

                validateStreamingModeParameters(optionHolder);

                //send sourceEventListener and preferred operation to changeDataCapture object
                changeDataCapture = new ChangeDataCapture(operation, sourceEventListener);

                //create the folder for history file if not exists
                File directory = new File(historyFileDirectory);
                if (!directory.exists()) {
                    boolean isDirectoryCreated = directory.mkdirs();
                    if (isDirectoryCreated && log.isDebugEnabled()) {
                        log.debug("Directory created for history file.");
                    }
                }

                try {
                    Map<String, Object> configMap = CDCSourceUtil.getConfigMap(username, password, url, tableName,
                            historyFileDirectory, siddhiAppName, streamName, serverID, serverName, connectorProperties,
                            this.hashCode());
                    changeDataCapture.setConfig(configMap);
                } catch (WrongConfigurationException ex) {
                    throw new SiddhiAppCreationException("The cdc source couldn't get started because of invalid" +
                            " configurations. Found configurations: {username='" + username + "', password=******," +
                            " url='" + url + "', tablename='" + tableName + "'," +
                            " connetorProperties='" + connectorProperties + "'}", ex);
                }
                break;
            case CDCSourceConstants.MODE_POLLING:

                String pollingColumn = optionHolder.validateAndGetStaticValue(CDCSourceConstants.POLLING_COLUMN);
                boolean isDatasourceNameAvailable = optionHolder.isOptionExists(CDCSourceConstants.DATASOURCE_NAME);
                String datasourceName = "";
                if (isDatasourceNameAvailable) {
                    datasourceName = optionHolder.validateAndGetStaticValue(CDCSourceConstants.DATASOURCE_NAME);
                }

                pollingInterval = Integer.parseInt(
                        optionHolder.validateAndGetStaticValue(CDCSourceConstants.POLLING_INTERVAL,
                                Integer.toString(CDCSourceConstants.DEFAULT_POLLING_INTERVAL_MS)));

                validatePollingModeParameters();
                if (!isDatasourceNameAvailable) {
                    String driverClassName;
                    try {
                        driverClassName = optionHolder.validateAndGetStaticValue(CDCSourceConstants.JDBC_DRIVER_NAME);
                        url = optionHolder.validateAndGetOption(CDCSourceConstants.DATABASE_CONNECTION_URL).getValue();
                        username = optionHolder.validateAndGetOption(CDCSourceConstants.USERNAME).getValue();
                        password = optionHolder.validateAndGetOption(CDCSourceConstants.PASSWORD).getValue();
                    } catch (SiddhiAppValidationException ex) {
                        throw new SiddhiAppValidationException(ex.getMessage() + " Alternatively, define "
                                + CDCSourceConstants.DATASOURCE_NAME + ".");
                    }
                    cdcPollar = new CDCPollar(url, username, password, tableName, driverClassName, lastOffset,
                            pollingColumn, pollingInterval, sourceEventListener, this);
                } else {
                    cdcPollar = new CDCPollar(datasourceName, tableName, lastOffset, pollingColumn, pollingInterval,
                            sourceEventListener, this);
                }
                break;
            default:
                throw new SiddhiAppValidationException("unsupported " + CDCSourceConstants.MODE + ": " + mode);
        }
    }

    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{Map.class};
    }

    @Override
    public void connect(ConnectionCallback connectionCallback) throws ConnectionUnavailableException {

        switch (mode) {
            case CDCSourceConstants.MODE_STREAMING:
                //keep the object reference in Object keeper
                cdcSourceObjectKeeper.addCdcObject(this);

                //create completion callback to handle the exceptions from debezium engine.
                EmbeddedEngine.CompletionCallback completionCallback = (success, message, error) -> {
                    if (!success) {
                        connectionCallback.onError(new ConnectionUnavailableException(
                                "Connection to the database lost.", error));
                    }
                };

                EmbeddedEngine engine = changeDataCapture.getEngine(completionCallback);
                executorService.execute(engine);
                break;
            case CDCSourceConstants.MODE_POLLING:
                //create a completion callback to handle exceptions from CDCPollar
                CDCPollar.CompletionCallback cdcCompletionCallback = (Throwable error) ->
                {
                    if (error.getClass().equals(SQLException.class)) {
                        connectionCallback.onError(new ConnectionUnavailableException(
                                "Connection to the database lost.", error));
                    } else {
                        destroy();
                        throw new SiddhiAppRuntimeException("CDC Polling mode run failed.", error);
                    }
                };

                cdcPollar.setCompletionCallback(cdcCompletionCallback);
                executorService.execute(cdcPollar);
                break;
            default:
                break;
        }
    }

    @Override
    public void disconnect() {
    }

    @Override
    public void destroy() {
        if (mode.equals(CDCSourceConstants.MODE_STREAMING)) {
            //Remove this CDCSource object from the CDCObjectKeeper.
            cdcSourceObjectKeeper.removeObject(this.hashCode());
        }
        //shutdown the executor service.
        executorService.shutdown();
    }

    @Override
    public void pause() {
        switch (mode) {
            case CDCSourceConstants.MODE_POLLING:
                cdcPollar.pause();
                break;
            case CDCSourceConstants.MODE_STREAMING:
                changeDataCapture.pause();
                break;
            default:
                break;
        }
    }

    @Override
    public void resume() {
        switch (mode) {
            case CDCSourceConstants.MODE_POLLING:
                cdcPollar.resume();
                break;
            case CDCSourceConstants.MODE_STREAMING:
                changeDataCapture.resume();
                break;
            default:
                break;
        }
    }

    @Override
    public Map<String, Object> currentState() {
        Map<String, Object> currentState = new HashMap<>();
        switch (mode) {
            case CDCSourceConstants.MODE_POLLING:
                currentState.put("last.offset", lastOffset);
                break;
            case CDCSourceConstants.MODE_STREAMING:
                currentState.put(CDCSourceConstants.CACHE_OBJECT, offsetData);
                break;
            default:
                break;
        }
        return currentState;
    }

    @Override
    public void restoreState(Map<String, Object> map) {
        switch (mode) {
            case CDCSourceConstants.MODE_POLLING:
                Object lastOffsetObj = map.get("last.offset");
                this.lastOffset = (String) lastOffsetObj;
                break;
            case CDCSourceConstants.MODE_STREAMING:
                Object cacheObj = map.get(CDCSourceConstants.CACHE_OBJECT);
                this.offsetData = (HashMap<byte[], byte[]>) cacheObj;
                break;
            default:
                break;
        }
    }

    void setLastOffset(String lastOffset) {
        this.lastOffset = lastOffset;
    }

    Map<byte[], byte[]> getOffsetData() {
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            log.error("Offset data retrieval failed.", e);
        }
        return offsetData;
    }

    void setOffsetData(Map<byte[], byte[]> offsetData) {
        this.offsetData = offsetData;
    }

    /**
     * Used to Validate the parameters for the mode: streaming.
     */
    private void validateStreamingModeParameters(OptionHolder optionHolder) {
        //datasource.name should not be accepted for streaming mode.
        if (optionHolder.isOptionExists(CDCSourceConstants.DATASOURCE_NAME)) {
            throw new SiddhiAppValidationException("Parameter: " + CDCSourceConstants.DATASOURCE_NAME + " should" +
                    " not be defined for streaming mode");
        }

        if (!(operation.equalsIgnoreCase(CDCSourceConstants.INSERT)
                || operation.equalsIgnoreCase(CDCSourceConstants.UPDATE)
                || operation.equalsIgnoreCase(CDCSourceConstants.DELETE))) {
            throw new SiddhiAppValidationException("Unsupported operation: '" + operation + "'." +
                    " operation should be one of 'insert', 'update' or 'delete'");
        }

        if (carbonHome.isEmpty()) {
            throw new SiddhiAppValidationException("Couldn't initialize Carbon Home.");
        } else if (!historyFileDirectory.endsWith(File.separator)) {
            historyFileDirectory = historyFileDirectory + File.separator;
        }
    }

    /**
     * Used to Validate the parameters for the mode: polling.
     */
    private void validatePollingModeParameters() {
        if (pollingInterval < 0) {
            throw new SiddhiAppValidationException(CDCSourceConstants.POLLING_INTERVAL + " should be a " +
                    "non negative integer.");
        }
    }
}
