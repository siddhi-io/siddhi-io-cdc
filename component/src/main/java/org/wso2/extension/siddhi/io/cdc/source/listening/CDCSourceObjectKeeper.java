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

package org.wso2.extension.siddhi.io.cdc.source.listening;

import org.wso2.extension.siddhi.io.cdc.source.CDCSource;

import java.util.HashMap;
import java.util.Map;

/**
 * This class Contains methods to store and retrieve the CDCSource objects.
 */
public class CDCSourceObjectKeeper {

    private static CDCSourceObjectKeeper cdcSourceObjectKeeper = new CDCSourceObjectKeeper();
    private Map<Integer, CDCSource> objectMap;

    private CDCSourceObjectKeeper() {
        objectMap = new HashMap<>();
    }

    /**
     * @param cdcSource is added to the objectMap against it's toString() value.
     */
    public void addCdcObject(CDCSource cdcSource) {
        objectMap.put(cdcSource.hashCode(), cdcSource);
    }

    /**
     * @param cdcSourceHashCode is the CDCSource's hashcode to be removed from the objectMap.
     */
    public void removeObject(int cdcSourceHashCode) {
        objectMap.remove(cdcSourceHashCode);
    }

    /**
     * @param hashCode cdcSource object's hashcode.
     * @return cdcObject if the particular object is already added. Otherwise, return null.
     */
    CDCSource getCdcObject(int hashCode) {
        return objectMap.get(hashCode);
    }

    public static CDCSourceObjectKeeper getCdcSourceObjectKeeper() {
        return cdcSourceObjectKeeper;
    }
}
