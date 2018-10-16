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

import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.embedded.spi.OffsetCommitPolicy;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.wso2.extension.siddhi.io.cdc.util.CDCSourceUtil;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;

import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is for capturing change data using debezium embedded engine.
 **/
class ChangeDataCapture {

    private String operation;
    private Configuration config;
    private SourceEventListener sourceEventListener;
    private ReentrantLock lock = new ReentrantLock();
    private Condition condition = lock.newCondition();
    private boolean paused = false;

    ChangeDataCapture(String operation, SourceEventListener sourceEventListener) {
        this.operation = operation;
        this.sourceEventListener = sourceEventListener;
    }

    /**
     * Initialize this.config according to user specified parameters.
     */
    void setConfig(Map<String, Object> configMap) {

        //set config from configMap.
        config = Configuration.empty();
        for (Map.Entry<String, Object> entry : configMap.entrySet()) {
            config = config.edit().with(entry.getKey(), entry.getValue()).build();
        }

    }

    /**
     * Create a new Debezium embedded engine with the configuration {@code config} and,
     *
     * @return engine.
     */
    EmbeddedEngine getEngine() throws NullPointerException {
        // Create an engine with above set configuration ...
        EmbeddedEngine engine = EmbeddedEngine.create()
                .using(OffsetCommitPolicy.always())
                .using(config)
                .notifying(this::handleEvent)
                .build();

        return engine;
    }

    void pause() {
        paused = true;
    }

    void resume() {
        paused = false;
        try {
            lock.lock();
            condition.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * When an event is received, create and send the event details to the sourceEventListener.
     */
    private void handleEvent(ConnectRecord connectRecord) {
        Map<String, Object> detailsMap;

        if (paused) {
            lock.lock();
            try {
                while (paused) {
                    condition.await();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } finally {
                lock.unlock();
            }
        }
        detailsMap = CDCSourceUtil.createMap(connectRecord, operation);
        if (!detailsMap.isEmpty()) {
            sourceEventListener.onEvent(detailsMap, null);
        }
    }
}
