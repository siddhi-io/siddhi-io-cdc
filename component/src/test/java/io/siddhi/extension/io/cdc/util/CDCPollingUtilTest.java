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

import io.siddhi.extension.io.cdc.source.polling.CDCPollingModeException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Unit tests for {@link CDCPollingUtil}, which parses the {@code pool.properties} value used to configure the Hikari
 * connection pool in polling mode.
 */
public class CDCPollingUtilTest {

    @Test
    public void isEmptyRecognisesNullBlankAndWhitespace() {
        Assert.assertTrue(CDCPollingUtil.isEmpty(null));
        Assert.assertTrue(CDCPollingUtil.isEmpty(""));
        Assert.assertTrue(CDCPollingUtil.isEmpty("   "));
        Assert.assertFalse(CDCPollingUtil.isEmpty("value"));
        Assert.assertFalse(CDCPollingUtil.isEmpty("  value  "));
    }

    @Test
    public void poolPropertiesAreSplitIntoTrimmedPairs() {
        List<String[]> pairs = CDCPollingUtil.processKeyValuePairs(
                "maximumPoolSize:10, idleTimeout : 60000,connectionTimeout:30000");

        Assert.assertEquals(pairs.size(), 3);
        Assert.assertEquals(pairs.get(0), new String[]{"maximumPoolSize", "10"});
        Assert.assertEquals(pairs.get(1), new String[]{"idleTimeout", "60000"});
        Assert.assertEquals(pairs.get(2), new String[]{"connectionTimeout", "30000"});
    }

    @Test
    public void emptyPoolPropertiesProduceNoPairs() {
        Assert.assertTrue(CDCPollingUtil.processKeyValuePairs(null).isEmpty());
        Assert.assertTrue(CDCPollingUtil.processKeyValuePairs("").isEmpty());
        Assert.assertTrue(CDCPollingUtil.processKeyValuePairs("   ").isEmpty());
    }

    @Test(expectedExceptions = CDCPollingModeException.class)
    public void aPropertyWithoutASeparatorIsRejected() {
        CDCPollingUtil.processKeyValuePairs("maximumPoolSize10");
    }

    @Test(expectedExceptions = CDCPollingModeException.class)
    public void aPropertyWithTooManySeparatorsIsRejected() {
        CDCPollingUtil.processKeyValuePairs("maximumPoolSize:10:20");
    }

    /**
     * Closing null artifacts is the normal path when a query fails before the statement is created.
     */
    @Test
    public void cleanupToleratesNullArtifacts() {
        CDCPollingUtil.cleanupConnection(null, null, null);
    }
}
