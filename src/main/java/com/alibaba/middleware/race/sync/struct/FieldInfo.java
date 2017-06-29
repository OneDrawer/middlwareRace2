package com.alibaba.middleware.race.sync.struct;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by mst on 2017/6/19.
 */
public class FieldInfo {
    private ArrayList<String> fields = new ArrayList<>();
    private byte primaryIndex;
    private ConcurrentHashMap<String, Byte> fieldToIndex = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Byte, String> indexTofield = new ConcurrentHashMap<>();
    //ConcurrentHashMap<Byte, byte[]> IndexToFiled = new ConcurrentHashMap<>();

    public ArrayList<String> getFields() {
        return fields;
    }

    public Map<String, Byte> getFieldToIndex() {
        return fieldToIndex;
    }

    public Map<Byte, String> getIndexToField() {
        return indexTofield;
    }

    public void setPrimaryIndex(byte primaryIndex) {
        this.primaryIndex = primaryIndex;
    }

}
