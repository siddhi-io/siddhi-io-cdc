/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package io.siddhi.extension.io.cdc.source.metrics;

/**
 * This class contains Util methods for the Metrics.
 */
public class MetricsUtils {

    private static String getShortenURL(String jdbcURL) {
        if (jdbcURL.length() <= 30) {
            return jdbcURL;
        }
        int n = jdbcURL.length();
        int i = 30; // to get last 30 characters
        char c = jdbcURL.charAt(i);
        while (Character.isLetterOrDigit(c)) {
            if (i == n - 1) {
                break;
            }
            i++;
            c = jdbcURL.charAt(i);
        }
        if (i == (n - 1)) {
            return jdbcURL;
        }
        return jdbcURL.substring(0, i) + "..";
    }

    public static String getShortenJDBCURL(String url) {
        String[] splittedURL = url.split(":");
        String formattedURL;
        switch (splittedURL[1]) {
            case "mysql":
            case "postgresql": {
                String[] split = url.split("\\?");
                if (split.length == 1) {
                    formattedURL = split[0];
                } else {
                    formattedURL = split[0] + "..";
                }
                break;
            }
            case "sqlserver":
            case "derby": {
                String[] split = url.split(";");
                if (split.length == 1) {
                    formattedURL = split[0];
                } else {
                    formattedURL = split[0] + "..";
                }
                break;
            }
            case "oracle": {
                url  = new StringBuilder(url).reverse().toString();
                url = url.replaceAll("@.*?:", "@");
                url = new StringBuilder(url).reverse().toString();
                int index = url.indexOf('@');
                int split = 0;
                for (int i = index + 1; i < url.length(); i++) {
                    if (url.charAt(i) == ':') {
                        split = i;
                        break;
                    }
                }
                if (split != 0) {
                    formattedURL = url.substring(0, split) + "..";
                } else {
                    formattedURL = url;
                }
                break;
            }
            case "db2": {
                int index = url.lastIndexOf("db2:") + 3;
                int split = 0;
                for (int i = index + 1; i < url.length(); i++) {
                    if (url.charAt(i) == ':') {
                        split = i;
                        break;
                    }
                }
                if (split != 0) {
                    formattedURL = url.substring(0, split) + "..";
                } else {
                    formattedURL = url;
                }
                break;
            }
            default: {
                formattedURL = url;
            }
        }
        return getShortenURL(formattedURL);
    }
}
