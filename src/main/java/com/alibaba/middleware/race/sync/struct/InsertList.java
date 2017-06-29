//package com.alibaba.middleware.race.sync.struct;
//
//import sun.security.util.Length;
//
//import java.util.*;
//import java.util.concurrent.ConcurrentHashMap;
//
///**
// * Created by mst on 2017/6/7.
// */
//public class InsertList {
//    ConcurrentHashMap<byte[], byte[][]> chm = new ConcurrentHashMap<>();
//    //字段名列表,解析日志时在Insert语句中提取
//    public static HashMap<byte[], Byte> fields = new HashMap<>();
//    public static int sizeOfFields = 0;
//    //放入Insert日志数据
//    public void put(byte[] key, byte[][] values) {
//        chm.put(key, values);
//    }
//
//
//    //获取主键对应的数据
//    public byte[][] getData(byte[] key){
//        return chm.get(key);
//    }
//
//    //
//    public Set<Map.Entry<byte[], byte[][]>> keySet() {
//        return chm.entrySet();
//    }
//
//    // 更新字段，应将数据更新到field列表对应的位置。
//    public void update(byte[] key, KeyValue kv) {
//        int index = fields.get(kv.key);
//
//    }
//
//    //更新主键
//    public void updatePrimaryKey(byte[] key, byte[] newKey) {
//        byte[][] body = chm.remove(key);
//        chm.put(newKey, body);
//    }
//
//    // 删除主键
//    public byte[][] delete(byte[] key) {
//       return chm.remove(key);
//    }
//
//    public void clear() {
//        chm.clear();
//    }
//
//}