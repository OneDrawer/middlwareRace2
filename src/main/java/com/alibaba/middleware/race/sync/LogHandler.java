//package com.alibaba.middleware.race.sync;
//
//import com.alibaba.middleware.race.sync.struct.*;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.Map;
//
///**
// * Created by mst on 2017/6/11.
// */
//public class LogHandler {
//    static boolean compare(byte[] src, byte[] dest) {
//        int len = src.length;
//        if(len != dest.length) {
//            return false;
//        }
//        int i = 0;
//        while(i < len) {
//            if(src[i] != dest[i]) {
//                return false;
//            }
//            i++;
//        }
//        return true;
//    }
//    public static void putInsert(InsertList insertList, byte[] body, int size) {
//        insertList.put(getPrimary(body, size), getInsertColValue(body,size));
//    }
//    // 在InsertList中存在数据，更新InsertList
//    public static void updateBody(InsertList insertList, UpdateState us) {
//        byte[] key = us.getOldKey();
//        byte[] newKey = us.getNewKey();
//        byte[][] body = insertList.getData(key);
//        HashMap<byte[],byte[]> hm = us.getColChanged();
//        for(byte[] col:hm.keySet()) {
//            byte colIndex = InsertList.fields.get(col);
//            body[colIndex] = hm.get(col);
//        }
//
//        // 更新主键
//        if(!compare(key, newKey)) {
//            byte[][] update = insertList.delete(key);
//            insertList.put(newKey, update);
//        }
//
//    }
//    public static UpdateState getUpdateState(byte[] body, int size) {
//        HashMap<byte[],byte[]> hm =new HashMap<>();
//        byte[] oldKey = null;
//        byte[] newKey= null;
//        int last = 0;
//        int cur = 0;
//        while(cur < size){
//            // 解析数据 格式 [id:1:1|NULL|33445|]
//            byte[] col;
//            byte[] value;
//            last  = cur;
//            while(body[cur] != ':') {
//                cur++;
//            }
//            col= new byte[cur - last];
//            System.arraycopy(body, last, col, 0, cur - last);
//            last = cur;
//            while(body[cur]!='|') {
//                byte primary = body[cur - 1];
//                if(primary == 0) {
//                    // 非主键，取最后字段值
//                    while(body[cur]!='|'){
//                        cur++;
//                    }
//                    last  = cur;
//                    while(body[cur]!='|') {
//                        cur++;
//                    }
//                    value= new byte[cur - last];
//                    System.arraycopy(body,last,value,0,cur -last);
//                    last = cur;
//                    hm.put(col, value);
//                } else {
//                    // 主键处理
//                    while(body[cur]!='|'){
//                        cur++;
//                    }
//                    oldKey= new byte[cur -last];
//                    System.arraycopy(body, last, oldKey, 0, cur);
//                    last = cur;
//                    while(body[cur]!='|'){
//                        cur++;
//                    }
//                    newKey= new byte[cur -last];
//                    System.arraycopy(body, last, newKey, 0, cur -last);
//                }
//            }
//
//        }
//        return new UpdateState(hm, oldKey, newKey);
//
//    }
//    public static void putDelete(DeleteList deleteList, byte[] body, int size) {
//        deleteList.add(getPrimary(body, size));
//    }
//    public static byte[][] getInsertColValue(byte[] body, int size) {
//        byte[][] col = new byte[InsertList.sizeOfFields][];
//        int indexOfCol = 0;
//        int cur = 0;
//        int last = 0;
//        byte[] key;
//        while (body[cur] != '|') {
//            cur++;
//        }
//        // 主键 处理
//        if(body[cur - 1] == 1) {
//        while(cur < size) {
//                while (body[cur] != '|') {
//                    cur++;
//                }
//                last = cur;
//                cur++;
//                while (body[cur] != '|') {
//                    cur++;
//                }
//                key = new byte[cur -last];
//                col[indexOfCol++] = key;
//                System.arraycopy(body, last, key, 0, cur- last);
//            }
//
//            // 非主键处理
//            while (body[cur] != '|') {
//                cur++;
//            }
//            last = cur;
//            while (body[cur] != '|') {
//                cur++;
//            }
//            byte[] value = new byte[cur - last];
//            col[indexOfCol++] = value;
//            last  = cur;
//        }
//        return col;
//    }
//    public static byte[] getPrimary(byte[] body, int size) {
//        int cur = 0;
//        int primary;
//        byte[] value = null;
//        do {
//            // 查找主键
//            do{
//                if(body[cur] == '|') {
//                    primary = body[cur - 1];
//                    break;
//                }
//                cur ++;
//            } while(true);
//
//            if(primary == 0) {
//                while(body[cur++]!='|'){}
//                while(body[cur++]!='|'){}
//                continue;
//            }
//
//            // 查找到了主键
//            while(body[cur++]!='|'){}
//            int last = cur;
//            // 主键对应的值
//            do{
//                if(body[cur] == '|') {
//                    value = new byte[cur - last];
//                    System.arraycopy(body, last, value, 0, cur);
//                    cur = 0;
//                    break;
//                }
//                cur++;
//            } while(true);
//
//            break;
//        } while(cur < size);
//        return value;
//    }
//
//    public static QuerySlice mergeTask(QuerySlice cur, QuerySlice addition) {
//        InsertList curInsertList = cur.getInsertList();
//        UpdateList curUpdateList = cur.getUpdateList();
//        DeleteList curDeleteList = cur.getDeleteList();
//
//        // 将deleteList 更新到上一块
//        ArrayList<byte[]> additionDelete = addition.getDeleteList().getDeleted();
//        int len = additionDelete.size();
//        for(int i = 0; i < len; i++) {
//            byte[] key = additionDelete.get(i);
//
//            if(curInsertList.getData(key) == null) {
//
//                // 上一块发生了主键变更
//                if(curUpdateList.getPrimaryKeyChanges().containsKey(key)) {
//                    // 删除 主键变更条目，更新记录，并添加 变更前主键的删除记录
//                    byte[] oldKey = curUpdateList.getPrimaryKeyChanges().get(key);
//                    // 删除变更主键的记录，并将主键变更所在块内关于此条记录的所有更新删除
//                    curUpdateList.getPrimaryKeyChanges().remove(key);
//                    curUpdateList.getUpdate().remove(oldKey);
//                    // 删除变更主键之前的所有关于此记录的信息
//                    curDeleteList.add(oldKey);
//                } else {
//                    // 上一块未发生主键变更
//                    if(curUpdateList.getUpdate().containsKey(key)) {
//                        curUpdateList.getUpdate().remove(key);
//                    }
//                    curDeleteList.add(key);
//                }
//            } else {
//                //所有关于此条记录的信息被清除
//                curInsertList.delete(key);
//            }
//        }
//
//        // 在发生主键变更时，我们并不直接更新，除非合并时发现更新前主键的Insert记录，
//        // 当发生主键变更时:
//        //1)在InsertList中找到了变更前主键 -> 直接在Insert记录中更新
//        //2)未在InsertList中找到变更前主键 -> 在原主键(oldKey)上更新字段，
//        //  然后在主键变更列表(primaryUpdateList)添加一条变更记录，合并时首先要判断主键
//        //  是否包含在主键变更列表中，如果包含，则应使用最初的主键(oldKey)更新列
//        UpdateList additionUpdate = addition.getUpdateList();
//        Map<byte[], HashMap<byte[], byte[]>> update =  additionUpdate.getUpdate();
//        for(Map.Entry<byte[], HashMap<byte[],byte[]>> entry : update.entrySet()) {
//            byte[] key = entry.getKey();
//            // 若之前发生过主键变更，则将新的键值替换成旧的键值以执行更新
//            if(curUpdateList.getPrimaryKeyChanges().containsKey(key)){
//                key = curUpdateList.getPrimaryKeyChanges().get(key); // 替换为oldkey
//            }
//
//            if(curInsertList.getData(key) == null) {
//
//                if (curUpdateList.getUpdate().get(key) == null) {
//                    curUpdateList.getUpdate().put(key, entry.getValue());
//                } else {
////                    HashMap<byte[], byte[]> hm = curUpdateList.getUpdate().get(key);
//                    hm.putAll(entry.getValue());
//                }
//            } else {
//                //直接将更新应用到InsertList
//                //updateBody(curInsertList, );
//            }
//        }
//        return null;
//    }
//}
