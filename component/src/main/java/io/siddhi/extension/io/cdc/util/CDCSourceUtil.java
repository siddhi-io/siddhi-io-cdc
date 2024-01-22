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

import io.siddhi.extension.io.cdc.source.listening.InMemoryOffsetBackingStore;
import io.siddhi.extension.io.cdc.source.listening.WrongConfigurationException;
import io.siddhi.extension.io.cdc.source.metrics.ListeningMetrics;
import io.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class contains Util methods for the CDCSource.
 */
public class CDCSourceUtil {
    public static Map<String, Object> getConfigMap(String username, String password, String url, String tableName,
                                                   String historyFileDirectory, String siddhiAppName,
                                                   String siddhiStreamName, int serverID, String serverName,
                                                   String connectorProperties, int cdcSourceHashCode,
                                                   ListeningMetrics metrics, String pluginName)
            throws WrongConfigurationException {
        Map<String, Object> configMap = new HashMap<>();
        String host;
        int port;
        String database;
        Boolean isMongodb = false;

        Map<String, String> connectorPropertiesMap = getConnectorPropertiesMap(connectorProperties);

        //Add schema specific details to configMap
        String[] splittedURL = url.split(":");
        if (!splittedURL[0].equalsIgnoreCase("jdbc")) {
            throw new WrongConfigurationException("Invalid JDBC url: " + url + " received for stream: " +
                    siddhiStreamName + ". Expected url format: jdbc:mysql://<host>:<port>/<database_name>");
        } else {
            switch (splittedURL[1]) {
                case "mysql": {
                    //Extract url details
                    String regex = "jdbc:mysql://([a-zA-Z0-9-_\\.]+):(\\d++)/(\\w*)";
                    Pattern p = Pattern.compile(regex);
                    Matcher matcher = p.matcher(url);
                    if (matcher.find()) {
                        host = matcher.group(1);
                        port = Integer.parseInt(matcher.group(2));
                        database = matcher.group(3);

                    } else {
                        throw new WrongConfigurationException("Invalid JDBC url: " + url + " received for stream: " +
                                siddhiStreamName + ". Expected url format: jdbc:mysql://<host>:<port>/<database_name>");
                    }

                    //Add extracted url details to configMap.
                    configMap.put(CDCSourceConstants.DATABASE_PORT, port);
                    configMap.put(CDCSourceConstants.TABLE_TABLE_INCLUDE_LIST, database + "." + tableName);
                    configMap.put(CDCSourceConstants.DATABASE_HOSTNAME, host);

                    //Add other MySQL specific details to configMap.
                    configMap.put(CDCSourceConstants.CONNECTOR_CLASS, CDCSourceConstants.MYSQL_CONNECTOR_CLASS);
                    if (metrics != null) {
                        metrics.setDbType("MySQL");
                    }
                    break;
                }
                case "postgresql": {
                    //Extract url details
                    String regex = "jdbc:postgresql://([a-zA-Z0-9-_\\.]+):(\\d++)/(\\w*)";
                    Pattern p = Pattern.compile(regex);
                    Matcher matcher = p.matcher(url);
                    if (matcher.find()) {
                        host = matcher.group(1);
                        port = Integer.parseInt(matcher.group(2));
                        database = matcher.group(3);
                    } else {
                        throw new WrongConfigurationException("Invalid JDBC url: " + url + " received for stream: " +
                                siddhiStreamName + ". Expected url format: jdbc:postgresql://<host>:<port>/" +
                                "<database_name>");
                    }

                    //Add extracted url details to configMap.
                    configMap.put(CDCSourceConstants.DATABASE_HOSTNAME, host);
                    configMap.put(CDCSourceConstants.DATABASE_PORT, port);
                    configMap.put(CDCSourceConstants.DATABASE_DBNAME, database);
                    configMap.put(CDCSourceConstants.TABLE_TABLE_INCLUDE_LIST, tableName);
                    configMap.put(CDCSourceConstants.PLUGIN_NAME, pluginName);

                    //Add other PostgreSQL specific details to configMap.
                    configMap.put(CDCSourceConstants.CONNECTOR_CLASS, CDCSourceConstants.POSTGRESQL_CONNECTOR_CLASS);
                    if (metrics != null) {
                        metrics.setDbType("PostgreSQL");
                    }
                    break;
                }
                case "sqlserver": {
                    //Extract url details
                    String regex = "jdbc:sqlserver://([a-zA-Z0-9-_\\.]+):(\\d++);databaseName=(\\w*)";
                    Pattern p = Pattern.compile(regex);
                    Matcher matcher = p.matcher(url);
                    if (matcher.find()) {
                        host = matcher.group(1);
                        port = Integer.parseInt(matcher.group(2));
                        database = matcher.group(3);
                    } else {
                        throw new WrongConfigurationException("Invalid JDBC url: " + url + " received for stream: " +
                                siddhiStreamName + ". Expected url format: jdbc:sqlserver://<host>:<port>;" +
                                "databaseName=<database_name>");
                    }
                    //Add extracted url details to configMap.
                    configMap.put(CDCSourceConstants.DATABASE_HOSTNAME, host);
                    configMap.put(CDCSourceConstants.DATABASE_PORT, port);
                    configMap.put(CDCSourceConstants.TABLE_TABLE_INCLUDE_LIST, tableName);
                    configMap.put(CDCSourceConstants.DATABASE_DBNAME, database);

                    //Add other SQLServer specific details to configMap.
                    configMap.put(CDCSourceConstants.CONNECTOR_CLASS, CDCSourceConstants.SQLSERVER_CONNECTOR_CLASS);
                    if (metrics != null) {
                        metrics.setDbType("SQL Server");
                    }
                    break;
                }
                case "oracle": {
                    String regex = "jdbc:oracle:(\\w*):@?\\/?\\/?([a-zA-Z0-9-_\\.]+):(\\d+)([\\/]?)([a-zA-Z0-9-_\\.]*)";
                    Pattern p = Pattern.compile(regex);
                    Matcher matcher = p.matcher(url);

                    if (matcher.find()) {
                        host = matcher.group(2);
                        port = Integer.parseInt(matcher.group(3));
                        database = matcher.group(5);
                    } else {
                        throw new WrongConfigurationException("Invalid JDBC url: " + url + " received for stream: " +
                                siddhiStreamName + ". Expected url format: jdbc:oracle:<driver>://<host>:<port>/<sid>");
                    }

                    // Validate required connector properties
                    if (!connectorPropertiesMap.containsKey(CDCSourceConstants.ORACLE_OUTSERVER_PROPERTY_NAME)) {
                        throw new WrongConfigurationException("Required properties " +
                                CDCSourceConstants.ORACLE_OUTSERVER_PROPERTY_NAME + " is missing in the " +
                                CDCSourceConstants.CONNECTOR_PROPERTIES + " configurations.");
                    }

                    configMap.put(CDCSourceConstants.DATABASE_HOSTNAME, host);
                    configMap.put(CDCSourceConstants.DATABASE_PORT, port);
                    configMap.put(CDCSourceConstants.TABLE_TABLE_INCLUDE_LIST, tableName);
                    configMap.put(CDCSourceConstants.DATABASE_DBNAME, database);
                    configMap.put(CDCSourceConstants.CONNECTOR_CLASS, CDCSourceConstants.ORACLE_CONNECTOR_CLASS);
                    if (metrics != null) {
                        metrics.setDbType("Oracle");
                    }
                    break;
                }
                case "mongodb": {
                    //Extract url details
                    isMongodb = true;
                    String regex = "jdbc:mongodb://(\\w*|(\\w*)/[a-zA-Z0-9-_\\.]+):(\\d++)/(\\w*)";
                    Pattern p = Pattern.compile(regex);
                    Matcher matcher = p.matcher(url);
                    String replicaSetName;
                    if (matcher.find()) {
                        host = matcher.group(1);
                        replicaSetName = matcher.group(2);
                        port = Integer.parseInt(matcher.group(3));
                        database = matcher.group(4);
                    } else {
                        throw new WrongConfigurationException("Invalid MongoDB uri: " + url +
                                " received for stream: " + siddhiStreamName +
                                ". Expected uri format: jdbc:mongodb://<replica_set_name>/<host>:<port>/" +
                                "<database_name>");
                    }
                    configMap.put(CDCSourceConstants.CONNECTOR_CLASS, CDCSourceConstants.MONGODB_CONNECTOR_CLASS);
                    //hostname and port pairs of the MongoDB servers in the replica set.
                    configMap.put(CDCSourceConstants.MONGODB_HOSTS, host + ":" + port);
                    //unique name that identifies the connector and/or MongoDB replica set or sharded cluster
                    configMap.put(CDCSourceConstants.MONGODB_NAME, replicaSetName);
                    //fully-qualified namespaces for MongoDB collections to be monitored
                    configMap.put(CDCSourceConstants.MONGODB_COLLECTION_INCLUDE_LIST, database + "." + tableName);
                    if (metrics != null) {
                        metrics.setDbType("MongoDB");
                    }
                    break;
                }
                default:
                    throw new WrongConfigurationException("Unsupported schema. Expected schema: mysql or postgresql" +
                            "or sqlserver, oracle or mongodb Found: " + splittedURL[1]);

            }
            if (metrics != null) {
                metrics.setHost(host);
                metrics.setDatabaseName(database);
                metrics.getTotalReadsMetrics();
                metrics.getEventCountMetric();
                metrics.getTotalEventCounterMetric();
                metrics.getValidEventCountMetric();
            }

            //Add general config details to configMap
            if (!isMongodb) {
                configMap.put(CDCSourceConstants.DATABASE_USER, username);
                configMap.put(CDCSourceConstants.DATABASE_PASSWORD, password);
            } else {
                configMap.put(CDCSourceConstants.MONGODB_USER, username);
                configMap.put(CDCSourceConstants.MONGODB_PASSWORD, password);
            }

            if (serverID == CDCSourceConstants.DEFAULT_SERVER_ID) {
                Random random = new Random();
                configMap.put(CDCSourceConstants.SERVER_ID, random.nextInt(1001) + 5400);
            } else {
                configMap.put(CDCSourceConstants.SERVER_ID, serverID);
            }

            //set the database server name if specified, otherwise set <host>_<port> as default
            if (serverName.equals("")) {
                configMap.put(CDCSourceConstants.DATABASE_SERVER_NAME, host + "_" + port);
            } else {
                configMap.put(CDCSourceConstants.DATABASE_SERVER_NAME, serverName);
            }

            configMap.put(CDCSourceConstants.OFFSET_STORAGE, InMemoryOffsetBackingStore.class.getName());
            configMap.put(CDCSourceConstants.CDC_SOURCE_OBJECT, cdcSourceHashCode);

            //set history file path.
            configMap.put(CDCSourceConstants.DATABASE_HISTORY, CDCSourceConstants.DATABASE_HISTORY_FILEBASE_HISTORY);
            configMap.put(CDCSourceConstants.DATABASE_HISTORY_FILE_NAME,
                    historyFileDirectory + siddhiStreamName + ".dat");

            //set connector property: name
            configMap.put(CDCSourceConstants.CONNECTOR_NAME, siddhiAppName + siddhiStreamName);

            //set additional connector properties using comma separated key value pair string
            for (Map.Entry<String, String> entry : connectorPropertiesMap.entrySet()) {
                configMap.put(entry.getKey(), entry.getValue());
            }
            return configMap;
        }
    }

    private static Map<String, String> getConnectorPropertiesMap(String connectorProperties) {

        Map<String, String> connectorPropertiesMap = new HashMap<>();

        if (!connectorProperties.isEmpty()) {
            String[] keyValuePairs = connectorProperties.split(",");
            for (String keyValuePair : keyValuePairs) {
                String[] keyAndValue = keyValuePair.split("=");
                if (keyAndValue.length != 2) {
                    throw new SiddhiAppValidationException("connector.properties input is invalid. Check near :" +
                            keyValuePair);
                } else {
                    connectorPropertiesMap.put(keyAndValue[0], keyAndValue[1]);
                }
            }
        }
        return connectorPropertiesMap;
    }

    /**
     * Get the WSO2 Stream Processor's local path from System Variables.
     * if carbon.home is not set, return the current project path. (for test cases and for use as a java library)
     */
    public static String getCarbonHome() {
        String path = System.getProperty(CDCSourceConstants.CARBON_HOME);

        if (path == null) {
            path = System.getProperty(CDCSourceConstants.USER_DIRECTORY);
        }
        return path;
    }
}
