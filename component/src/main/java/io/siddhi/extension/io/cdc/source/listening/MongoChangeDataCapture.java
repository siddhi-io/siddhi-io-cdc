package io.siddhi.extension.io.cdc.source.listening;

import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.extension.io.cdc.util.CDCSourceConstants;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * This class is for capturing change data for MongoDB using debezium embedded engine.
 **/
public class MongoChangeDataCapture extends ChangeDataCapture {

    public MongoChangeDataCapture(String operation, SourceEventListener sourceEventListener) {
        super(operation, sourceEventListener);
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
            } else if (jsonObj.get(key) instanceof Double) {
                detailsMap.put(key, jsonObj.getDouble(key));
            } else if (jsonObj.get(key) instanceof String) {
                detailsMap.put(key, jsonObj.getString(key));
            } else if (jsonObj.get(key) instanceof JSONObject) {
                if (jsonObj.getJSONObject(key).toString().contains(CDCSourceConstants.MONGO_COLLECTION_OBJECT_ID)) {
                    detailsMap.put(CDCSourceConstants.MONGO_COLLECTION_ID, jsonObj.getJSONObject(key)
                            .get(CDCSourceConstants.MONGO_COLLECTION_OBJECT_ID));
                } else if (jsonObj.getJSONObject(key).toString()
                        .contains(CDCSourceConstants.MONGO_OBJECT_NUMBER_DECIMAL)) {
                    detailsMap.put(key, Double.parseDouble((String) jsonObj.getJSONObject(key)
                            .get(CDCSourceConstants.MONGO_OBJECT_NUMBER_DECIMAL)));
                } else if (jsonObj.getJSONObject(key).toString()
                        .contains(CDCSourceConstants.MONGO_OBJECT_NUMBER_LONG)) {
                    detailsMap.put(key, Long.parseLong((String) jsonObj.getJSONObject(key)
                            .get(CDCSourceConstants.MONGO_OBJECT_NUMBER_LONG)));
                } else {
                    detailsMap.put(key, jsonObj.getJSONObject(key).toString());
                }
            }
        }
        return detailsMap;
    }
}
