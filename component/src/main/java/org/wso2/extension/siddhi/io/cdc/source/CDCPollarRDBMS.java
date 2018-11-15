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
import org.wso2.siddhi.core.exception.CannotLoadConfigurationException;
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;

import javax.sql.DataSource;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.wso2.extension.siddhi.io.cdc.util.CDCSourceUtil.cleanupConnection;


public class CDCPollarRDBMS {
    private static final Logger log = Logger.getLogger(CDCPollarRDBMS.class);
    private static String RDBMS_QUERY_CONFIG_FILE = "query-config.xml";
    private static final String DATABASE_PRODUCT_NAME = "Database Product Name";
    private static final String VERSION = "Version";

    private Object loadConfiguration() throws CannotLoadConfigurationException {
        InputStream inputStream = null;
        try {
            JAXBContext ctx = JAXBContext.newInstance(CDCPollarRDBMS.class);
            Unmarshaller unmarshaller = ctx.createUnmarshaller();
            ClassLoader classLoader = getClass().getClassLoader();
            inputStream = classLoader.getResourceAsStream(RDBMS_QUERY_CONFIG_FILE);
            if (inputStream == null) {
                throw new CannotLoadConfigurationException(RDBMS_QUERY_CONFIG_FILE
                        + " is not found in the classpath");
            }
            return unmarshaller.unmarshal(inputStream);
        } catch (JAXBException e) {
            throw new CannotLoadConfigurationException(
                    "Error in processing query configuration: " + e.getMessage(), e);
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    log.error(String.format("Failed to close the input stream for %s", RDBMS_QUERY_CONFIG_FILE));
                }
            }
        }
    }

    public static Map<String, Object> lookupDatabaseInfo(DataSource ds) {
        Connection conn = null;
        try {
            conn = ds.getConnection();
            DatabaseMetaData dmd = conn.getMetaData();
            Map<String, Object> result = new HashMap<>();
            result.put(DATABASE_PRODUCT_NAME, dmd.getDatabaseProductName());
            result.put(VERSION, Double.parseDouble(dmd.getDatabaseMajorVersion() + "."
                    + dmd.getDatabaseMinorVersion()));
            return result;
        } catch (SQLException e) {
            throw new SiddhiAppRuntimeException("Error in looking up database type: " + e.getMessage(), e);
        } finally {
            cleanupConnection(null, null, conn);
        }
    }
}
