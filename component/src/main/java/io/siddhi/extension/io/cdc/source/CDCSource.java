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

package io.siddhi.extension.io.cdc.source;

import io.debezium.embedded.EmbeddedEngine;
import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.cdc.source.listening.CDCSourceObjectKeeper;
import io.siddhi.extension.io.cdc.source.listening.ChangeDataCapture;
import io.siddhi.extension.io.cdc.source.listening.WrongConfigurationException;
import io.siddhi.extension.io.cdc.source.polling.CDCPoller;
import io.siddhi.extension.io.cdc.util.CDCSourceConstants;
import io.siddhi.extension.io.cdc.util.CDCSourceUtil;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.apache.log4j.Logger;

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
        description = "The CDC source receives events when change events (i.e., INSERT, UPDATE, DELETE) are triggered" +
                " for a database table. Events are received in the 'key-value' format.\n\n" +

                "There are two modes you could perform CDC: " +
                "Listening mode and Polling mode.\n\n" +

                "In polling mode, the datasource is periodically polled for capturing the changes. " +
                "The polling period can be configured.\n" +
                "In polling mode, you can only capture INSERT and UPDATE changes.\n\n" +

                "On listening mode, the Source will keep listening to the Change Log of the database" +
                " and notify in case a change has taken place. Here, you are immediately notified about the change, " +
                "compared to polling mode.\n\n" +

                "The key values of the map of a CDC change event are as follows.\n\n" +

                "For 'listening' mode: \n" +
                "\tFor insert: Keys are specified as columns of the table.\n" +
                "\tFor delete: Keys are followed by the specified table columns. This is achieved via " +
                "'before_'. e.g., specifying 'before_X' results in the key being added before the column named 'X'.\n" +
                "\tFor update: Keys are followed followed by the specified table columns. This is achieved via " +
                "'before_'. e.g., specifying 'before_X' results in the key being added before the column named 'X'." +
                "\n\nFor 'polling' mode: Keys are specified as the columns of the table." +

                "#### Preparations required for working with Oracle Databases in listening mode\n" +
                "\n" +
                "Using the extension in Windows, Mac OSX and AIX are pretty straight forward inorder to achieve the " +
                "required behaviour please follow the steps given below\n" +
                "\n" +
                "  - Download the compatible version of oracle instantclient for the database version from [here]" +
                "(https://www.oracle.com/database/technologies/instant-client/downloads.html) and extract\n" +
                "  - Extract and set the environment variable `LD_LIBRARY_PATH` to the location of instantclient " +
                "which was exstracted as shown below\n" +
                "  ```\n" +
                "    export LD_LIBRARY_PATH=<path to the instant client location>\n" +
                "  ```\n" +
                "  - Inside the instantclient folder which was download there are two jars `xstreams.jar` and " +
                "`ojdbc<version>.jar` convert them to OSGi bundles using the tools which were provided in the " +
                "`<distribution>/bin` for converting the `ojdbc.jar` use the tool `spi-provider.sh|bat` and for " +
                "the conversion of `xstreams.jar` use the jni-provider.sh as shown below(Note: this way of " +
                "converting Xstreams jar is applicable only for Linux environments for other OSs this step is not " +
                "required and converting it through the `jartobundle.sh` tool is enough)\n" +
                "  ```\n" +
                "    ./jni-provider.sh <input-jar> <destination> <comma seperated native library names>\n" +
                "  ```\n" +
                "  once ojdbc and xstreams jars are converted to OSGi copy the generated jars to the " +
                "`<distribution>/lib`. Currently siddhi-io-cdc only supports the oracle database distributions " +
                "12 and above" +

                "\n\nSee parameter: mode for supported databases and change events.",
        parameters = {
                @Parameter(name = CDCSourceConstants.DATABASE_CONNECTION_URL,
                        description = "The connection URL to the database." +
                                "\nF=The format used is: " +
                                "'jdbc:mysql://<host>:<port>/<database_name>' ",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = CDCSourceConstants.MODE,
                        description = "Mode to capture the change data. The type of events that can be received, " +
                                "and the required parameters differ based on the mode. The mode can be one of the " +
                                "following:\n" +
                                "'polling': This mode uses a column named 'polling.column' to monitor the given " +
                                "table. It captures change events of the 'RDBMS', 'INSERT, and 'UPDATE' types.\n" +
                                "'listening': This mode uses logs to monitor the given table. It currently supports" +
                                " change events only of the 'MySQL', 'INSERT', 'UPDATE', and 'DELETE' types.",
                        type = DataType.STRING,
                        defaultValue = "listening",
                        optional = true
                ),
                @Parameter(
                        name = CDCSourceConstants.JDBC_DRIVER_NAME,
                        description = "The driver class name for connecting the database." +
                                " **It is required to specify a value for this parameter when the mode is 'polling'.**",
                        type = DataType.STRING,
                        defaultValue = "<Empty_String>",
                        optional = true
                ),
                @Parameter(
                        name = CDCSourceConstants.USERNAME,
                        description = "The username to be used for accessing the database. This user needs to have" +
                                " the 'SELECT', 'RELOAD', 'SHOW DATABASES', 'REPLICATION SLAVE', and " +
                                "'REPLICATION CLIENT'privileges for the change data capturing table (specified via" +
                                " the 'table.name' parameter)." +
                                "\nTo operate in the polling mode, the user needs 'SELECT' privileges.",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = CDCSourceConstants.PASSWORD,
                        description = "The password of the username you specified for accessing the database.",
                        type = DataType.STRING
                ),
                @Parameter(name = CDCSourceConstants.POOL_PROPERTIES,
                        description = "The pool parameters for the database connection can be specified as key-value" +
                                " pairs.",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "<Empty_String>"
                ),
                @Parameter(
                        name = CDCSourceConstants.DATASOURCE_NAME,
                        description = "Name of the wso2 datasource to connect to the database." +
                                " When datasource name is provided, the URL, username and password are not needed. " +
                                "A datasource based connection is given more priority over the URL based connection." +
                                "\n This parameter is applicable only when the mode is set to 'polling', and it can" +
                                " be applied only when you use this extension with WSO2 Stream Processor.",
                        type = DataType.STRING,
                        defaultValue = "<Empty_String>",
                        optional = true
                ),
                @Parameter(
                        name = CDCSourceConstants.TABLE_NAME,
                        description = "The name of the table that needs to be monitored for data changes.",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = CDCSourceConstants.POLLING_COLUMN,
                        description = "The column name  that is polled to capture the change data. " +
                                "It is recommended to have a TIMESTAMP field as the 'polling.column' in order to" +
                                " capture the inserts and updates." +
                                "\nNumeric auto-incremental fields and char fields can also be" +
                                " used as 'polling.column'. However, note that fields of these types only support" +
                                " insert change capturing, and the possibility of using a char field also depends on" +
                                " how the data is input." +
                                "\n**It is required to enter a value for this parameter when the mode is 'polling'.**"
                        ,
                        type = DataType.STRING,
                        defaultValue = "<Empty_String>",
                        optional = true
                ),
                @Parameter(
                        name = CDCSourceConstants.POLLING_INTERVAL,
                        description = "The time interval (specified in seconds) to poll the given table for changes." +
                                "\nThis parameter is applicable only when the mode is set to 'polling'."
                        ,
                        type = DataType.INT,
                        defaultValue = "1",
                        optional = true
                ),
                @Parameter(
                        name = CDCSourceConstants.OPERATION,
                        description = "The change event operation you want to carry out. Possible values are" +
                                " 'insert', 'update' or 'delete'. It is required to specify a value when the mode is" +
                                " 'listening'." +
                                "\nThis parameter is not case sensitive.",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = CDCSourceConstants.CONNECTOR_PROPERTIES,
                        description = "Here, you can specify Debezium connector properties as a comma-separated " +
                                "string. " +
                                "\nThe properties specified here are given more priority over the parameters. This" +
                                " parameter is applicable only for the 'listening' mode.",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "Empty_String"
                ),
                @Parameter(name = CDCSourceConstants.DATABASE_SERVER_ID,
                        description = "An ID to be used when joining MySQL database cluster to read the bin log. " +
                                "This should be a unique integer between 1 to 2^32. This parameter is applicable " +
                                "only when the mode is 'listening'.",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "Random integer between 5400 and 6400"
                ),
                @Parameter(name = CDCSourceConstants.DATABASE_SERVER_NAME,
                        description = "A logical name that identifies and provides a namespace for the database " +
                                "server. This parameter is applicable only when the mode is 'listening'.",
                        defaultValue = "{host}_{port}",
                        optional = true,
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "wait.on.missed.record",
                        description = "Indicates whether the process needs to wait on missing/out-of-order records. " +
                                "\nWhen this flag is set to 'true' the process will be held once it identifies a " +
                                "missing record. The missing recrod is identified by the sequence of the " +
                                "polling.column value. This can be used only with number fields and not recommended " +
                                "to use with time values as it will not be sequential." +
                                "\nThis should be enabled ONLY where the records can be written out-of-order, (eg. " +
                                "concurrent writers) as this degrades the performance.",
                        type = DataType.BOOL,
                        optional = true,
                        defaultValue = "false"
                ),
                @Parameter(
                        name = "missed.record.waiting.timeout",
                        description = "The timeout (specified in seconds) to retry for missing/out-of-order record. " +
                                "This should be used along with the wait.on.missed.record parameter. If the " +
                                "parameter is not set, the process will indefinitely wait for the missing record.",
                        type = DataType.INT,
                        optional = true,
                        defaultValue = "-1"
                )
        },
        examples = {
                @Example(
                        syntax = "@source(type = 'cdc' , url = 'jdbc:mysql://localhost:3306/SimpleDB', " +
                                "\nusername = 'cdcuser', password = 'pswd4cdc', " +
                                "\ntable.name = 'students', operation = 'insert', " +
                                "\n@map(type='keyvalue', @attributes(id = 'id', name = 'name')))" +
                                "\ndefine stream inputStream (id string, name string);",
                        description = "In this example, the CDC source listens to the row insertions that are made " +
                                "in the 'students' table with the column name, and the ID. This table belongs to the " +
                                "'SimpleDB' MySQL database that can be accessed via the given URL."
                ),
                @Example(
                        syntax = "@source(type = 'cdc' , url = 'jdbc:mysql://localhost:3306/SimpleDB', " +
                                "\nusername = 'cdcuser', password = 'pswd4cdc', " +
                                "\ntable.name = 'students', operation = 'update', " +
                                "\n@map(type='keyvalue', @attributes(id = 'id', name = 'name', " +
                                "\nbefore_id = 'before_id', before_name = 'before_name')))" +
                                "\ndefine stream inputStream (before_id string, id string, " +
                                "\nbefore_name string , name string);",
                        description = "In this example, the CDC source listens to the row updates that are made in " +
                                "the 'students' table. This table belongs to the 'SimpleDB' MySQL database that can" +
                                " be accessed via the given URL."
                ),
                @Example(
                        syntax = "@source(type = 'cdc' , url = 'jdbc:mysql://localhost:3306/SimpleDB', " +
                                "\nusername = 'cdcuser', password = 'pswd4cdc', " +
                                "\ntable.name = 'students', operation = 'delete', " +
                                "\n@map(type='keyvalue', @attributes(before_id = 'before_id'," +
                                " before_name = 'before_name')))" +
                                "\ndefine stream inputStream (before_id string, before_name string);",
                        description = "In this example, the CDC source listens to the row deletions made in the " +
                                "'students' table. This table belongs to the 'SimpleDB' database that can be accessed" +
                                " via the given URL."
                ),
                @Example(
                        syntax = "@source(type = 'cdc', mode='polling', polling.column = 'id', " +
                                "\njdbc.driver.name = 'com.mysql.jdbc.Driver', " +
                                "url = 'jdbc:mysql://localhost:3306/SimpleDB', " +
                                "\nusername = 'cdcuser', password = 'pswd4cdc', " +
                                "\ntable.name = 'students', " +
                                "\n@map(type='keyvalue'), @attributes(id = 'id', name = 'name'))" +
                                "\ndefine stream inputStream (id int, name string);",
                        description = "In this example, the CDC source polls the 'students' table for inserts. 'id'" +
                                " that is specified as the polling colum' is an auto incremental field. The " +
                                "connection to the database is made via the URL, username, password, and the JDBC" +
                                " driver name."
                ),
                @Example(
                        syntax = "@source(type = 'cdc', mode='polling', polling.column = 'id', " +
                                "datasource.name = 'SimpleDB'," +
                                "\ntable.name = 'students', " +
                                "\n@map(type='keyvalue'), @attributes(id = 'id', name = 'name'))" +
                                "\ndefine stream inputStream (id int, name string);",
                        description = "In this example, the CDC source polls the 'students' table for inserts. The" +
                                " given polling column is a char column with the 'S001, S002, ... .' pattern." +
                                " The connection to the database is made via a data source named 'SimpleDB'. Note " +
                                "that the 'datasource.name' parameter works only with the Stream Processor."
                ),
                @Example(
                        syntax = "@source(type = 'cdc', mode='polling', polling.column = 'last_updated', " +
                                "datasource.name = 'SimpleDB'," +
                                "\ntable.name = 'students', " +
                                "\n@map(type='keyvalue'))" +
                                "\ndefine stream inputStream (name string);",
                        description = "In this example, the CDC source polls the 'students' table for inserts " +
                                "and updates. The polling column is a timestamp field."
                ),
                @Example(
                        syntax = "@source(type='cdc', jdbc.driver.name='com.mysql.jdbc.Driver', " +
                                "url='jdbc:mysql://localhost:3306/SimpleDB', username='cdcuser', " +
                                "password='pswd4cdc', table.name='students', mode='polling', polling.column='id', " +
                                "operation='insert', wait.on.missed.record='true', " +
                                "missed.record.waiting.timeout='10'," +
                                "\n@map(type='keyvalue'), " +
                                "\n@attributes(batch_no='batch_no', item='item', qty='qty'))" +
                                "\ndefine stream inputStream (id int, name string);",
                        description = "In this example, the CDC source polls the 'students' table for inserts. The " +
                                "polling column is a numeric field. This source expects the records in the database " +
                                "to be written concurrently/out-of-order so it waits if it encounters a missing " +
                                "record. If the record doesn't appear within 10 seconds it resumes the process."
                ),
                @Example(
                        syntax = "@source(type = 'cdc', url = 'jdbc:oracle:thin://localhost:1521/ORCLCDB', " +
                                "username='c##xstrm', password='xs', table.name='DEBEZIUM.sweetproductiontable', " +
                                "operation = 'insert', connector.properties='oracle.outserver.name=DBZXOUT,oracle." +
                                "pdb=ORCLPDB1' @map(type = 'keyvalue'))\n" +
                                "define stream insertSweetProductionStream (ID int, NAME string, WEIGHT int);\n",
                        description = "In this example, the CDC source connect to an Oracle database and listens for" +
                                " insert queries of sweetproduction table"
                )
        }
)
public class CDCSource extends Source<CDCSource.CdcState> {
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
    private CDCPoller cdcPoller;

    @Override
    protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return null;
    }

    @Override
    public StateFactory<CdcState> init(SourceEventListener sourceEventListener, OptionHolder optionHolder,
                                       String[] requestedTransportPropertyNames, ConfigReader configReader,
                                       SiddhiAppContext siddhiAppContext) {
        //initialize mode
        mode = optionHolder.validateAndGetStaticValue(CDCSourceConstants.MODE, CDCSourceConstants.MODE_LISTENING);
        //initialize common mandatory parameters
        String tableName = optionHolder.validateAndGetOption(CDCSourceConstants.TABLE_NAME).getValue();
        String siddhiAppName = siddhiAppContext.getName();

        switch (mode) {
            case CDCSourceConstants.MODE_LISTENING:
                String url = optionHolder.validateAndGetOption(CDCSourceConstants.DATABASE_CONNECTION_URL).getValue();
                String username = optionHolder.validateAndGetOption(CDCSourceConstants.USERNAME).getValue();
                String password = optionHolder.validateAndGetOption(CDCSourceConstants.PASSWORD).getValue();
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

                validateListeningModeParameters(optionHolder);

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
                boolean isJndiResourceAvailable = optionHolder.isOptionExists(CDCSourceConstants.JNDI_RESOURCE);
                pollingInterval = Integer.parseInt(
                        optionHolder.validateAndGetStaticValue(CDCSourceConstants.POLLING_INTERVAL,
                                Integer.toString(CDCSourceConstants.DEFAULT_POLLING_INTERVAL_SECONDS)));
                validatePollingModeParameters();
                String poolPropertyString = optionHolder.validateAndGetStaticValue(CDCSourceConstants.POOL_PROPERTIES,
                        null);
                boolean waitOnMissedRecord = Boolean.parseBoolean(
                        optionHolder.validateAndGetStaticValue(CDCSourceConstants.WAIT_ON_MISSED_RECORD, "false"));
                int missedRecordWaitingTimeout = Integer.parseInt(
                        optionHolder.validateAndGetStaticValue(
                                CDCSourceConstants.MISSED_RECORD_WAITING_TIMEOUT, "-1"));

                if (isDatasourceNameAvailable) {
                    String datasourceName = optionHolder.validateAndGetStaticValue(CDCSourceConstants.DATASOURCE_NAME);
                    cdcPoller = new CDCPoller(null, null, null, tableName, null,
                            datasourceName, null, pollingColumn, pollingInterval,
                            poolPropertyString, sourceEventListener, configReader, waitOnMissedRecord,
                            missedRecordWaitingTimeout, siddhiAppName);
                } else if (isJndiResourceAvailable) {
                    String jndiResource = optionHolder.validateAndGetStaticValue(CDCSourceConstants.JNDI_RESOURCE);
                    cdcPoller = new CDCPoller(null, null, null, tableName, null,
                            null, jndiResource, pollingColumn, pollingInterval, poolPropertyString,
                            sourceEventListener, configReader, waitOnMissedRecord, missedRecordWaitingTimeout,
                            siddhiAppName);
                } else {
                    String driverClassName;
                    try {
                        driverClassName = optionHolder.validateAndGetStaticValue(CDCSourceConstants.JDBC_DRIVER_NAME);
                        url = optionHolder.validateAndGetOption(CDCSourceConstants.DATABASE_CONNECTION_URL).getValue();
                        username = optionHolder.validateAndGetOption(CDCSourceConstants.USERNAME).getValue();
                        password = optionHolder.validateAndGetOption(CDCSourceConstants.PASSWORD).getValue();
                    } catch (SiddhiAppValidationException ex) {
                        throw new SiddhiAppValidationException(ex.getMessage() + " Alternatively, define "
                                + CDCSourceConstants.DATASOURCE_NAME + " or " + CDCSourceConstants.JNDI_RESOURCE +
                                ". Current mode: " + CDCSourceConstants.MODE_POLLING);
                    }
                    cdcPoller = new CDCPoller(url, username, password, tableName, driverClassName,
                            null, null, pollingColumn, pollingInterval, poolPropertyString,
                            sourceEventListener, configReader, waitOnMissedRecord, missedRecordWaitingTimeout,
                            siddhiAppName);
                }
                break;
            default:
                throw new SiddhiAppValidationException("Unsupported " + CDCSourceConstants.MODE + ": " + mode);
        }

        return () -> new CdcState(mode);
    }

    @Override
    public Class[] getOutputEventClasses() {
        return new Class[] {Map.class};
    }

    @Override
    public void connect(ConnectionCallback connectionCallback, CdcState cdcState)
            throws ConnectionUnavailableException {
        switch (mode) {
            case CDCSourceConstants.MODE_LISTENING:
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
                //create a completion callback to handle exceptions from CDCPoller
                CDCPoller.CompletionCallback cdcCompletionCallback = (Throwable error) ->
                {
                    if (error.getClass().equals(SQLException.class)) {
                        connectionCallback.onError(new ConnectionUnavailableException(
                                "Connection to the database lost.", error));
                    } else {
                        destroy();
                        throw new SiddhiAppRuntimeException("CDC Polling mode run failed.", error);
                    }
                };

                cdcPoller.setCompletionCallback(cdcCompletionCallback);
                executorService.execute(cdcPoller);
                break;
            default:
                break; //Never get executed since mode is validated.
        }
    }

    @Override
    public void disconnect() {
        if (mode.equals(CDCSourceConstants.MODE_POLLING)) {
            cdcPoller.pause();
            if (cdcPoller.isLocalDataSource()) {
                cdcPoller.getDataSource().close();
                if (log.isDebugEnabled()) {
                    log.debug("Closing the pool for CDC polling mode.");
                }
            }
        }
    }

    @Override
    public void destroy() {
        this.disconnect();

        if (mode.equals(CDCSourceConstants.MODE_LISTENING)) {
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
                cdcPoller.pause();
                break;
            case CDCSourceConstants.MODE_LISTENING:
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
                cdcPoller.resume();
                break;
            case CDCSourceConstants.MODE_LISTENING:
                changeDataCapture.resume();
                break;
            default:
                break;
        }
    }

    public Map<byte[], byte[]> getOffsetData() {
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            log.error("Offset data retrieval failed.", e);
        }
        return offsetData;
    }

    public void setOffsetData(Map<byte[], byte[]> offsetData) {
        this.offsetData = offsetData;
    }

    /**
     * Used to Validate the parameters for the mode: listening.
     */
    private void validateListeningModeParameters(OptionHolder optionHolder) {
        //datasource.name should not be accepted for listening mode.
        if (optionHolder.isOptionExists(CDCSourceConstants.DATASOURCE_NAME)) {
            throw new SiddhiAppValidationException("Parameter: " + CDCSourceConstants.DATASOURCE_NAME + " should" +
                    " not be defined for listening mode");
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
                    "non negative integer. Current mode: " + CDCSourceConstants.MODE_POLLING);
        }
    }

    class CdcState extends State {

        private final String mode;

        private final Map<String, Object> state;

        private CdcState(String mode) {
            this.mode = mode;
            state = new HashMap<>();
        }

        @Override
        public boolean canDestroy() {
            return false;
        }

        @Override
        public Map<String, Object> snapshot() {
            switch (mode) {
                case CDCSourceConstants.MODE_POLLING:
                    state.put("last.offset", cdcPoller.getLastReadPollingColumnValue());
                    break;
                case CDCSourceConstants.MODE_LISTENING:
                    state.put(CDCSourceConstants.CACHE_OBJECT, offsetData);
                    break;
                default:
                    break;
            }
            return state;
        }

        @Override
        public void restore(Map<String, Object> map) {
            switch (mode) {
                case CDCSourceConstants.MODE_POLLING:
                    Object lastOffsetObj = map.get("last.offset");
                    cdcPoller.setLastReadPollingColumnValue((String) lastOffsetObj);
                    break;
                case CDCSourceConstants.MODE_LISTENING:
                    Object cacheObj = map.get(CDCSourceConstants.CACHE_OBJECT);
                    offsetData = (HashMap<byte[], byte[]>) cacheObj;
                    break;
                default:
                    break;
            }
        }
    }
}
