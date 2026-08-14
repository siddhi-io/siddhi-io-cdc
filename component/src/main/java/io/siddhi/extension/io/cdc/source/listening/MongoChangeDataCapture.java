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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This class is for capturing change data for MongoDB using debezium embedded engine.
 **/
public class MongoChangeDataCapture extends ChangeDataCapture {
    private static final Logger log = LogManager.getLogger(MongoChangeDataCapture.class);

    public MongoChangeDataCapture(String operation, SourceEventListener sourceEventListener, ListeningMetrics metrics) {
        super(operation, sourceEventListener, metrics);
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
        if (op == null) {
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
                    //append document details after insert.
                    String insertedDocument = getStringField(record, CDCSourceConstants.AFTER);
                    if (insertedDocument != null) {
                        detailsMap = getMongoDetailMap(new JSONObject(insertedDocument));
                    }
                    addTransportProperties(detailsMap, CDCSourceConstants.INSERT);
                    break;
                case CDCSourceConstants.CONNECT_RECORD_DELETE_OPERATION:
                    //only the document id is available for a delete.
                    detailsMap.put(CDCSourceConstants.MONGO_COLLECTION_ID, getDocumentId(connectRecord));
                    addTransportProperties(detailsMap, CDCSourceConstants.DELETE);
                    break;
                case CDCSourceConstants.CONNECT_RECORD_UPDATE_OPERATION:
                    /*
                     * Debezium reports the changed fields of an update through updateDescription.updatedFields. A
                     * replace, and a capture mode that omits the update description, carry the full document in
                     * "after" instead; fall back to it so that those events are not dropped.
                     */
                    String updatedFields = getUpdatedFields(record);
                    if (updatedFields == null) {
                        updatedFields = getStringField(record, CDCSourceConstants.AFTER);
                    }
                    if (updatedFields != null) {
                        detailsMap = getMongoDetailMap(new JSONObject(updatedFields));
                    }
                    detailsMap.put(CDCSourceConstants.MONGO_COLLECTION_ID, getDocumentId(connectRecord));
                    addTransportProperties(detailsMap, CDCSourceConstants.UPDATE);
                    break;
                default:
                    log.info("Provided value for \"op\" : {} is not supported.", op);
                    break;
            }
        }
        return detailsMap;
    }

    /**
     * {@code ChangeDataCapture.handleEvent} expects this entry to be a {@link List}, so it has to be populated after
     * any reassignment of the details map.
     */
    private void addTransportProperties(Map<String, Object> detailsMap, String operation) {
        List<Object> transportProperties = new ArrayList<>();
        transportProperties.add(operation);
        detailsMap.put(CDCSourceConstants.TRANSPORT_PROPERTIES, transportProperties);
    }

    /**
     * Reads the fields changed by an update from {@code updateDescription.updatedFields}.
     *
     * @return the changed fields as extended JSON, or null when the change event carries no update description.
     */
    private String getUpdatedFields(Struct record) {
        Struct updateDescription;
        try {
            updateDescription = record.getStruct(CDCSourceConstants.MONGO_UPDATE_DESCRIPTION);
        } catch (DataException ex) {
            log.debug("Change event has no {} field.", CDCSourceConstants.MONGO_UPDATE_DESCRIPTION);
            return null;
        }
        if (updateDescription == null) {
            return null;
        }
        return getStringField(updateDescription, CDCSourceConstants.MONGO_UPDATED_FIELDS);
    }

    /**
     * Resolves the document id from the change event key. An ObjectId is serialised as {@code {"$oid": "..."}} while
     * other id types are serialised as bare JSON scalars.
     */
    private Object getDocumentId(ConnectRecord connectRecord) {
        Struct key = (Struct) connectRecord.key();
        if (key == null) {
            return null;
        }
        String documentId = getStringField(key, CDCSourceConstants.MONGO_COLLECTION_ID);
        if (documentId == null) {
            return null;
        }
        try {
            Object parsedId = new JSONTokener(documentId).nextValue();
            if (parsedId instanceof JSONObject) {
                JSONObject idObject = (JSONObject) parsedId;
                if (idObject.has(CDCSourceConstants.MONGO_COLLECTION_OBJECT_ID)) {
                    return idObject.get(CDCSourceConstants.MONGO_COLLECTION_OBJECT_ID);
                }
                return idObject.toString();
            }
            return parsedId;
        } catch (JSONException ex) {
            return documentId;
        }
    }

    private String getStringField(Struct struct, String fieldName) {
        try {
            return struct.getString(fieldName);
        } catch (DataException ex) {
            log.debug("Change event has no {} field.", fieldName);
            return null;
        }
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
