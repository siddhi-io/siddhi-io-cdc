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

package io.siddhi.extension.io.cdc.source.listening;

import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.extension.io.cdc.source.metrics.ListeningMetrics;
import io.siddhi.extension.io.cdc.util.CDCSourceConstants;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * This class is for capturing change data for MongoDB using debezium embedded engine.
 **/
public class MongoChangeDataCapture extends ChangeDataCapture {
    private static final Logger log = Logger.getLogger(MongoChangeDataCapture.class);

    public MongoChangeDataCapture(String operation, SourceEventListener sourceEventListener, ListeningMetrics metrics,
                                  ExecutorService executorService) {
        super(operation, sourceEventListener, metrics, executorService);
    }

    Map<String, Object> createMap(ConnectRecord connectRecord, String operation) {
        //Map to return
        Map<String, Object> detailsMap = new HashMap<>();
        Struct record = (Struct) connectRecord.value();
        //get the change data object's operation.
        String op;
        try {
            op = (String) record.get(CDCSourceConstants.CONNECT_RECORD_OPERATION);
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
            switch (op) {
                case CDCSourceConstants.CONNECT_RECORD_INSERT_OPERATION:
                    //append row details after insert.
                    String insertString = (String) record.get(CDCSourceConstants.AFTER);
                    JSONObject jsonObj = new JSONObject(insertString);
                    detailsMap = getMongoDetailMap(jsonObj);
                    break;
                case CDCSourceConstants.CONNECT_RECORD_DELETE_OPERATION:
                    //append row details before delete.
                    String deleteDocumentId = (String) ((Struct) connectRecord.key())
                            .get(CDCSourceConstants.MONGO_COLLECTION_ID);
                    JSONObject jsonObjId = new JSONObject(deleteDocumentId);
                    detailsMap.put(CDCSourceConstants.MONGO_COLLECTION_ID,
                            jsonObjId.get(CDCSourceConstants.MONGO_COLLECTION_OBJECT_ID));

                    break;
                case CDCSourceConstants.CONNECT_RECORD_UPDATE_OPERATION:
                    //append row details before update.
                    String updateDocument = (String) record.get(CDCSourceConstants.MONGO_PATCH);
                    JSONObject jsonObj1 = new JSONObject(updateDocument);
                    JSONObject setJsonObj = (JSONObject) jsonObj1.get(CDCSourceConstants.MONGO_SET);
                    detailsMap = getMongoDetailMap(setJsonObj);
                    String updateDocumentId = (String) ((Struct) connectRecord.key())
                            .get(CDCSourceConstants.MONGO_COLLECTION_ID);
                    JSONObject jsonObjId1 = new JSONObject(updateDocumentId);
                    detailsMap.put(CDCSourceConstants.MONGO_COLLECTION_ID,
                            jsonObjId1.get(CDCSourceConstants.MONGO_COLLECTION_OBJECT_ID));
                    break;
                default:
                    log.info("Provided value for \"op\" : " + op + " is not supported.");
                    break;
            }
        }
        return detailsMap;
    }

    private Map<String, Object> getMongoDetailMap(JSONObject jsonObj) {
        Map<String, Object> detailsMap = new HashMap<>();
        Iterator<String> keys = jsonObj.keys();
        for (Iterator<String> it = keys; it.hasNext(); ) {
            String key = it.next();
            if (jsonObj.get(key) instanceof Boolean) {
                detailsMap.put(key, jsonObj.getBoolean(key));
            } else if (jsonObj.get(key) instanceof Integer) {
                detailsMap.put(key, jsonObj.getInt(key));
            } else if (jsonObj.get(key) instanceof Long) {
                detailsMap.put(key, jsonObj.getDouble(key));
            } else if (jsonObj.get(key) instanceof Double) {
                detailsMap.put(key, jsonObj.getDouble(key));
            } else if (jsonObj.get(key) instanceof String) {
                detailsMap.put(key, jsonObj.getString(key));
            } else if (jsonObj.get(key) instanceof JSONObject) {
                try {
                    detailsMap.put(key, Long.parseLong((String) jsonObj.getJSONObject(key)
                            .get(CDCSourceConstants.MONGO_OBJECT_NUMBER_LONG)));
                } catch (JSONException notLongObjectEx) {
                    try {
                        detailsMap.put(key, Double.parseDouble((String) jsonObj.getJSONObject(key)
                                .get(CDCSourceConstants.MONGO_OBJECT_NUMBER_DECIMAL)));
                    } catch (JSONException notDoubleObjectEx) {
                        if (key.equals(CDCSourceConstants.MONGO_COLLECTION_INSERT_ID)) {
                            detailsMap.put(CDCSourceConstants.MONGO_COLLECTION_ID, jsonObj.getJSONObject(key)
                                    .get(CDCSourceConstants.MONGO_COLLECTION_OBJECT_ID));
                        } else {
                            detailsMap.put(key, jsonObj.getJSONObject(key).toString());
                        }
                    }
                }
            }
        }
        return detailsMap;
    }
}
