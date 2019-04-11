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

import org.wso2.extension.siddhi.io.cdc.source.config.Database;
import org.wso2.extension.siddhi.io.cdc.source.config.QueryConfiguration;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.nodes.Node;

import java.util.HashMap;
import java.util.Map;

/**
 * This class is used to read query-config.yaml
 */
public class MyYamlConstructor extends Constructor {

    private Map<String, Class<?>> classMap = new HashMap<>();

    public MyYamlConstructor(Class<?> theRoot) {
        super(theRoot);
        classMap.put(QueryConfiguration.class.getName(), QueryConfiguration.class);
        classMap.put(Database.class.getName(), Database.class);
    }

    protected Class<?> getClassForNode(Node node) {
        String name = node.getTag().getClassName();
        Class<?> classForNode = classMap.get(name);
        if (classForNode == null) {
            throw new YAMLException("Class not found: " + name);
        } else {
            return classForNode;
        }
    }

}
