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

import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.embedded.spi.OffsetCommitPolicy;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.wso2.extension.siddhi.io.cdc.util.CDCSourceConstants;
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is for capturing change data using debezium embedded engine.
 **/
public class ChangeDataCapture {

    private String operation;
    private Configuration config;
    private SourceEventListener sourceEventListener;
    private ReentrantLock lock = new ReentrantLock();
    private Condition condition = lock.newCondition();
    private boolean paused = false;

    public ChangeDataCapture(String operation, SourceEventListener sourceEventListener) {
        this.operation = operation;
        this.sourceEventListener = sourceEventListener;
    }

    /**
     * Initialize this.config according to user specified parameters.
     */
    public void setConfig(Map<String, Object> configMap) {

        //set config from configMap.
        config = Configuration.empty();
        for (Map.Entry<String, Object> entry : configMap.entrySet()) {
            config = config.edit().with(entry.getKey(), entry.getValue()).build();
        }
    }

    /**
     * Create a new Debezium embedded engine with the configuration {@code config} and,
     *
     * @return {@code engine}.
     */
    public EmbeddedEngine getEngine(EmbeddedEngine.CompletionCallback completionCallback) {
        // Create and return Engine with above set configuration ...
        EmbeddedEngine.Builder builder = EmbeddedEngine.create()
                .using(OffsetCommitPolicy.always())
                .using(completionCallback)
                .using(config);
        if (builder == null) {
            throw new SiddhiAppRuntimeException("CDC Engine create failed. Check parameters.");
        } else {
            EmbeddedEngine engine = builder.notifying(this::handleEvent).build();
            return engine;
        }
    }

    public void pause() {
        paused = true;
    }

    public void resume() {
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
        detailsMap = createMap(connectRecord, operation);
        if (!detailsMap.isEmpty()) {
            sourceEventListener.onEvent(detailsMap, null);
        }
    }

    /**
     * Create Hash map using the connect record and operation,
     *
     * @param connectRecord is the change data object which is received from debezium embedded engine.
     * @param operation     is the change data event which is specified by the user.
     **/

    private Map<String, Object> createMap(ConnectRecord connectRecord, String operation) {

        //Map to return
        Map<String, Object> detailsMap = new HashMap<>();

        Struct record = (Struct) connectRecord.value();

        //get the change data object's operation.
        String op;

        try {
            op = (String) record.get("op");
        } catch (NullPointerException | DataException ex) {
            return detailsMap;
        }

        //match the change data's operation with user specifying operation and proceed.
        if (operation.equalsIgnoreCase(CDCSourceConstants.INSERT) &&
                op.equals(CDCSourceConstants.CONNECT_RECORD_INSERT_OPERATION)
                || operation.equalsIgnoreCase(CDCSourceConstants.DELETE) &&
                op.equals(CDCSourceConstants.CONNECT_RECORD_DELETE_OPERATION)
                || operation.equalsIgnoreCase(CDCSourceConstants.UPDATE) &&
                op.equals(CDCSourceConstants.CONNECT_RECORD_UPDATE_OPERATION)) {

            Struct rawDetails;
            List<Field> fields;
            String fieldName;

            switch (op) {
                case CDCSourceConstants.CONNECT_RECORD_INSERT_OPERATION:
                    //append row details after insert.
                    rawDetails = (Struct) record.get(CDCSourceConstants.AFTER);
                    fields = rawDetails.schema().fields();
                    for (Field key : fields) {
                        fieldName = key.name();
                        detailsMap.put(fieldName, rawDetails.get(fieldName));
                    }
                    break;
                case CDCSourceConstants.CONNECT_RECORD_DELETE_OPERATION:
                    //append row details before delete.
                    rawDetails = (Struct) record.get(CDCSourceConstants.BEFORE);
                    fields = rawDetails.schema().fields();
                    for (Field key : fields) {
                        fieldName = key.name();
                        detailsMap.put(CDCSourceConstants.BEFORE_PREFIX + fieldName, rawDetails.get(fieldName));
                    }
                    break;
                case CDCSourceConstants.CONNECT_RECORD_UPDATE_OPERATION:
                    //append row details before update.
                    rawDetails = (Struct) record.get(CDCSourceConstants.BEFORE);
                    fields = rawDetails.schema().fields();
                    for (Field key : fields) {
                        fieldName = key.name();
                        detailsMap.put(CDCSourceConstants.BEFORE_PREFIX + fieldName, rawDetails.get(fieldName));
                    }
                    //append row details after update.
                    rawDetails = (Struct) record.get(CDCSourceConstants.AFTER);
                    fields = rawDetails.schema().fields();
                    for (Field key : fields) {
                        fieldName = key.name();
                        detailsMap.put(fieldName, rawDetails.get(fieldName));
                    }
                    break;
            }
        }
        return detailsMap;
    }
}
