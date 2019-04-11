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

package org.wso2.extension.siddhi.io.cdc.util;

import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.wso2.extension.siddhi.io.cdc.source.listening.InMemoryOffsetBackingStore;
import org.wso2.extension.siddhi.io.cdc.source.listening.WrongConfigurationException;

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
                                                   String connectorProperties, int cdcSourceHashCode)
            throws WrongConfigurationException {

        Map<String, Object> configMap = new HashMap<>();
        String host;
        int port;
        String database;

        //Add schema specific details to configMap
        String[] splittedURL = url.split(":");
        if (!splittedURL[0].equalsIgnoreCase("jdbc")) {
            throw new WrongConfigurationException("Invalid JDBC url: " + url + " received for stream: " +
                    siddhiStreamName + ". Expected url format: jdbc:mysql://<host>:<port>/<database_name>");
        } else {
            switch (splittedURL[1]) {
                case "mysql": {
                    //Extract url details
                    String regex = "jdbc:mysql://(\\w*|[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}):" +
                            "(\\d++)/(\\w*)";
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
                    configMap.put(CDCSourceConstants.TABLE_WHITELIST, database + "." + tableName);
                    configMap.put(CDCSourceConstants.DATABASE_HOSTNAME, host);

                    //Add other MySQL specific details to configMap.
                    configMap.put(CDCSourceConstants.CONNECTOR_CLASS, CDCSourceConstants.MYSQL_CONNECTOR_CLASS);

                    break;
                }
                default: {
                    throw new WrongConfigurationException("Unsupported schema. Expected schema: mysql, Found: "
                            + splittedURL[1]);
                }
            }

            //Add general config details to configMap
            configMap.put(CDCSourceConstants.DATABASE_USER, username);
            configMap.put(CDCSourceConstants.DATABASE_PASSWORD, password);

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
            configMap.put("name", siddhiAppName + siddhiStreamName);

            //set additional connector properties using comma separated key value pair string
            for (Map.Entry<String, String> entry : getConnectorPropertiesMap(connectorProperties).entrySet()) {
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
