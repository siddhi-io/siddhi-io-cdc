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
import org.wso2.extension.siddhi.io.cdc.util.CDCSourceConstants;
import org.wso2.extension.siddhi.io.cdc.util.CDCSourceUtil;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.input.source.Source;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.io.File;
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
        description = "The CDC source receives events when a specified MySQL table's change event " +
                "(INSERT, UPDATE, DELETE) is triggered. The events are received in key-value map format." +
                "\nThe following are key values of the map of a CDC change event and their descriptions." +
                "\n\tFor insert: Keys will be specified table's columns" +
                "\n\tFor delete: Keys will be 'before_' followed by specified table's columns. Eg: before_X" +
                "\n\tFor update: Keys will be specified table's columns and 'before_' followed by specified table's " +
                "columns.",
        parameters = {
                @Parameter(name = "url",
                        description = "Connection url to the database." +
                                "\nuse format: " +
                                "jdbc:mysql://<host>:<port>/<database_name> ",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "username",
                        description = "Username of a user with SELECT, RELOAD, SHOW DATABASES," +
                                " REPLICATION SLAVE, REPLICATION CLIENT privileges on Change Data Capturing table.",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "password",
                        description = "Password for the above user.",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "table.name",
                        description = "Name of the table which needs to be monitored for data changes.",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "operation",
                        description = "Interested change event operation. 'insert', 'update' or 'delete'. " +
                                "\nNot case sensitive.",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "connector.properties",
                        description = "Debezium connector specified properties as a comma separated string. " +
                                "\nThis properties will have more priority over the parameters.",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "Empty_String"
                ),
                @Parameter(name = "database.server.id",
                        description = "For MySQL, a unique integer between 1 to 2^32 as the ID," +
                                " This is used when joining MySQL database cluster to read binlog",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "Random integer between 5400 and 6400"
                ),
                @Parameter(name = "database.server.name",
                        description = "Logical name that identifies and provides a namespace for the " +
                                "particular database server",
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
                                " can be accessed with the given url"
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
                                " can be accessed with the given url"
                )
        }
)

public class CDCSource extends Source {
    private static final Logger log = Logger.getLogger(CDCSource.class);
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private Map<byte[], byte[]> offsetData = new HashMap<>();
    private String operation;
    private ChangeDataCapture changeDataCapture;
    private String historyFileDirectory;
    private CDCSourceObjectKeeper cdcSourceObjectKeeper = CDCSourceObjectKeeper.getCdcSourceObjectKeeper();
    private String carbonHome;

    /**
     * The initialization method for {@link Source}, will be called before other methods. It used to validate
     * all configurations and to get initial values.
     *
     * @param sourceEventListener After receiving events, the source should trigger onEvent() of this listener.
     *                            Listener will then pass on the events to the appropriate mappers for processing .
     * @param optionHolder        Option holder containing static configuration related to the {@link Source}
     * @param configReader        ConfigReader is used to read the {@link Source} related system configuration.
     * @param siddhiAppContext    The context of the {@link org.wso2.siddhi.query.api.SiddhiApp} used to get Siddhi
     *                            related utility functions.
     */
    @Override
    public void init(SourceEventListener sourceEventListener, OptionHolder optionHolder,
                     String[] requestedTransportPropertyNames, ConfigReader configReader,
                     SiddhiAppContext siddhiAppContext) {
        String siddhiAppName = siddhiAppContext.getName();
        String streamName = sourceEventListener.getStreamDefinition().getId();

        //initialize mandatory parameters
        String url = optionHolder.validateAndGetOption(CDCSourceConstants.DATABASE_CONNECTION_URL).getValue();
        String tableName = optionHolder.validateAndGetOption(CDCSourceConstants.TABLE_NAME).getValue();
        String username = optionHolder.validateAndGetOption(CDCSourceConstants.USERNAME).getValue();
        String password = optionHolder.validateAndGetOption(CDCSourceConstants.PASSWORD).getValue();
        operation = optionHolder.validateAndGetOption(CDCSourceConstants.OPERATION).getValue();

        //initialize optional parameters
        int serverID;
        serverID = Integer.parseInt(optionHolder.validateAndGetStaticValue(CDCSourceConstants.DATABASE_SERVER_ID,
                "-1"));

        String serverName;
        serverName = optionHolder.validateAndGetStaticValue(CDCSourceConstants.DATABASE_SERVER_NAME,
                CDCSourceConstants.EMPTY_STRING);

        //initialize parameters from connector.properties
        String connectorProperties = optionHolder.validateAndGetStaticValue(CDCSourceConstants.CONNECTOR_PROPERTIES,
                CDCSourceConstants.EMPTY_STRING);

        //initialize history file directory
        carbonHome = CDCSourceUtil.getCarbonHome();
        historyFileDirectory = carbonHome + File.separator + "cdc" + File.separator + "history"
                + File.separator + siddhiAppName + File.separator;

        validateParameter();

        //send this object reference and preferred operation to changeDataCapture object
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
            throw new SiddhiAppCreationException("The cdc source couldn't get started. Invalid" +
                    " configuration parameters.", ex);
        }
    }

    /**
     * Returns the list of classes which this source can output.
     *
     * @return Array of classes that will be output by the source.
     * Null or empty array if it can produce any type of class.
     */
    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{Map.class};
    }

    /**
     * Initially Called to connect to the debezium embedded engine to receive change data events asynchronously.
     */
    @Override
    public void connect(ConnectionCallback connectionCallback) throws ConnectionUnavailableException {
        //keep the object reference in Object keeper
        cdcSourceObjectKeeper.addCdcObject(this);

        try {
            executorService.execute(changeDataCapture.getEngine());
        } catch (NullPointerException ex) {
            throw new ConnectionUnavailableException("Connection is unavailable. Check parameters.", ex);
        }
    }

    /**
     * This method can be called when it is needed to disconnect from the end point.
     */
    @Override
    public void disconnect() {
    }

    /**
     * Called at the end to clean all the resources consumed by the {@link Source}
     */
    @Override
    public void destroy() {
        //Remove this CDCSource object from the CDCObjectKeeper.
        cdcSourceObjectKeeper.removeObject(this.hashCode());

        //shutdown the executor service.
        executorService.shutdown();
    }

    /**
     * Called to pause event consumption
     */
    @Override
    public void pause() {
        changeDataCapture.pause();
    }

    /**
     * Called to resume event consumption
     */
    @Override
    public void resume() {
        changeDataCapture.resume();
    }

    /**
     * Used to collect the serializable state of the processing element, that need to be
     * persisted for the reconstructing the element to the same state on a different point of time
     *
     * @return stateful objects of the processing element as a map
     */
    @Override
    public Map<String, Object> currentState() {
        Map<String, Object> currentState = new HashMap<>();
        currentState.put("cacheObj", offsetData);
        return currentState;
    }

    /**
     * Used to restore serialized state of the processing element, for reconstructing
     * the element to the same state as if was on a previous point of time.
     *
     * @param map the stateful objects of the processing element as a map.
     *            This map will have the  same keys that is created upon calling currentState() method.
     */
    @Override
    public void restoreState(Map<String, Object> map) {
        Object cacheObj = map.get("cacheObj");
        this.offsetData = (HashMap<byte[], byte[]>) cacheObj;
    }

    Map<byte[], byte[]> getOffsetData() {
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            log.error(e);
        }
        return offsetData;
    }


    void setOffsetData(Map<byte[], byte[]> offsetData) {
        this.offsetData = offsetData;
    }

    /**
     * Used to Validate the parameters.
     */
    private void validateParameter() {
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
}
