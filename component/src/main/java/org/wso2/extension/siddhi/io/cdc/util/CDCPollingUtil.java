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

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * This class contains Util methods for the CDCPollar.
 */
public class CDCPollingUtil {
    private static final Logger log = Logger.getLogger(CDCPollingUtil.class);

    /**
     * Utility method which can be used to check if a given string instance is null or empty.
     *
     * @param field the string instance to be checked.
     * @return true if the field is null or empty.
     */
    public static boolean isEmpty(String field) {
        return (field == null || field.trim().length() == 0);
    }

    /**
     * Method which can be used to clear up and ephemeral SQL connectivity artifacts.
     *
     * @param rs   {@link ResultSet} instance (can be null)
     * @param stmt {@link Statement} instance (can be null)
     * @param conn {@link Connection} instance (can be null)
     */
    public static void cleanupConnection(ResultSet rs, Statement stmt, Connection conn) {
        if (rs != null) {
            try {
                rs.close();
                if (log.isDebugEnabled()) {
                    log.debug("Closed ResultSet");
                }
            } catch (SQLException e) {
                if (log.isDebugEnabled()) {
                    log.debug("Error in closing ResultSet: " + e.getMessage(), e);
                }
            }
        }
        if (stmt != null) {
            try {
                stmt.close();
                if (log.isDebugEnabled()) {
                    log.debug("Closed PreparedStatement");
                }
            } catch (SQLException e) {
                if (log.isDebugEnabled()) {
                    log.debug("Error in closing PreparedStatement: " + e.getMessage(), e);
                }
            }
        }
        if (conn != null) {
            try {
                conn.close();
                if (log.isDebugEnabled()) {
                    log.debug("Closed Connection");
                }
            } catch (SQLException e) {
                if (log.isDebugEnabled()) {
                    log.debug("Error in closing Connection: " + e.getMessage(), e);
                }
            }
        }
    }
}
