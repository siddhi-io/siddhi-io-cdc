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

import io.siddhi.extension.io.cdc.source.config.Database;
import io.siddhi.extension.io.cdc.source.config.QueryConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests the loading of {@code query-config.yaml}, which supplies the vendor specific select query used by polling
 * mode. Reproduces the exact SnakeYAML wiring {@code PollingStrategy} uses, so that a SnakeYAML upgrade which changes
 * the {@code Constructor} contract or tightens tag handling is caught here rather than at runtime.
 */
public class QueryConfigurationTest {

    private static final String SELECT_QUERY_CONFIG_FILE = "query-config.yaml";
    private static final String EXPECTED_QUERY = "SELECT {{COLUMN_LIST}} FROM {{TABLE_NAME}} {{CONDITION}}";

    private QueryConfiguration loadQueryConfiguration() throws Exception {
        MyYamlConstructor constructor = new MyYamlConstructor(QueryConfiguration.class);
        TypeDescription queryTypeDescription = new TypeDescription(QueryConfiguration.class);
        queryTypeDescription.putListPropertyType("databases", Database.class);
        constructor.addTypeDescription(queryTypeDescription);
        Yaml yaml = new Yaml(constructor);
        try (InputStream inputStream = getClass().getClassLoader()
                .getResourceAsStream(SELECT_QUERY_CONFIG_FILE)) {
            Assert.assertNotNull(inputStream, SELECT_QUERY_CONFIG_FILE + " is not on the test classpath");
            return (QueryConfiguration) yaml.load(inputStream);
        }
    }

    @Test
    public void queryConfigurationLoadsEveryConfiguredDatabase() throws Exception {
        QueryConfiguration queryConfiguration = loadQueryConfiguration();

        Assert.assertNotNull(queryConfiguration);
        Assert.assertNotNull(queryConfiguration.getDatabases());

        List<String> names = new ArrayList<>();
        for (Database database : queryConfiguration.getDatabases()) {
            names.add(database.getName());
        }
        Assert.assertEquals(names,
                Arrays.asList("mysql", "oracle", "PostgreSQL", "H2", "Microsoft SQL Server"));
    }

    /**
     * The names are matched against {@code DatabaseMetaData.getDatabaseProductName()} case insensitively, and every
     * vendor currently shares the same query template.
     */
    @Test
    public void everyConfiguredDatabaseHasTheExpectedSelectQuery() throws Exception {
        for (Database database : loadQueryConfiguration().getDatabases()) {
            Assert.assertEquals(database.getSelectQuery(), EXPECTED_QUERY,
                    "Unexpected select query for " + database.getName());
        }
    }

    /**
     * The constructor deliberately refuses classes outside the configuration model, so that a crafted tag in the yaml
     * cannot instantiate arbitrary types.
     */
    @Test(expectedExceptions = org.yaml.snakeyaml.error.YAMLException.class)
    public void unknownClassTagsAreRejected() {
        MyYamlConstructor constructor = new MyYamlConstructor(QueryConfiguration.class);
        Yaml yaml = new Yaml(constructor);
        yaml.load("!!java.util.Random {}");
    }
}
