//package com.alibaba.middleware.race.sync.struct;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//
///**
// * Created by mst on 2017/6/7.
// */
//public class UpdateList {
//    HashMap<byte[], HashMap<byte[], byte[]>> update = new HashMap<>();
//    HashMap<byte[], byte[]> primaryKeyChanges = new HashMap<>();
//
//    // 当前主键key的更新字段列表
//    public void put(byte[] key, byte[] colName, byte[] colValue) {
//        if(update.get(key) == null) {
//            HashMap hm =new HashMap();
//            hm.put(colName, colValue);
//            update.put(key, hm);
//        } else {
//            update.get(key).put(colName, colValue);
//        }
//    }
//
//    public void add(byte[] newKey, byte[] oldKey) {
//        primaryKeyChanges.put(newKey, oldKey);
//    }
//
//    public Map<byte[], HashMap<byte[], byte[]>> getUpdate() {
//        return update;
//    }
//
//    public HashMap<byte[], byte[]> getPrimaryKeyChanges() {
//        return primaryKeyChanges;
//    }
//
//    public void remove(byte[] key) {
//        update.remove(key);
//    }
//
//    // 删除主键变更
//    // 应用场景：合并时已遇到变更后的Key对应的插入数据;执行更新，删除该条变更信息.
//    public void removePrimaryChange(KeyValue pkc) {
//        primaryKeyChanges.remove(pkc);
//    }
//
//    public void clear() {
//        update.clear();
//        primaryKeyChanges.clear();
//
//    }
//}
