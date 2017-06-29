//package com.alibaba.middleware.race.sync.struct;
//
//import java.nio.ByteBuffer;
//import java.util.*;
//
///**
// * 功能:
// * 将固定块大小的日志压缩成三个更新列表传输给客户端做进一步处理
// *
// * Parameters:
// * fileIndex  当前块文件名
// * last 当前块的上一块; -1 表示当前块是文件的第一块
// * next 当前块的下一块; -1 表示当前块是文件的最后一块
// * insertList 当前块压缩的插入列表
// * updateList 当前块压缩的更新列表
// * deleteList 当前块压缩的删除列表
// *
// * 合并逻辑,下一块数据作用于上一块的顺序
// * delete -> updateList -> primaryList -> InsertList
// * Created by mst on 2017/6/7.
// */
//public class QuerySlice {
//    int fileIndex;
//    boolean finished;
//    byte cur;
//    byte next;
//    byte last;
//    InsertList insertList = new InsertList() ;
//    UpdateList updateList = new UpdateList();
//    DeleteList deleteList = new DeleteList();
//
//    public QuerySlice(int fileIndex, byte current, boolean finished){
//        this.fileIndex = fileIndex;
//        this.cur = current;
//        this.finished = finished;
//    }
//    public QuerySlice(int fileIndex, byte last, byte next) {
//        this.fileIndex = fileIndex;
//        this.last = last;
//        this.next = next;
//    }
//
//    public InsertList getInsertList() {
//        return insertList;
//    }
//
//    public UpdateList getUpdateList() {
//        return updateList;
//    }
//
//    public DeleteList getDeleteList() {
//        return deleteList;
//    }
//
//    public byte getCur(){
//        return cur;
//    }
//
//    public byte getNext() {
//        return next;
//    }
//
//    public byte getLast() {
//        return last;
//    }
//
//    public int getFileIndex() {
//        return fileIndex;
//    }
//
//    public void clear() {
//        insertList.clear();
//        updateList.clear();
//        deleteList.clear();
//    }
//
//    // 序列化QuerySlice
//    static public void writeObject(ByteBuffer byteBuffer, QuerySlice qs) {
//        byte vert = '|';
//        byte tab ='\t';
//        byte br = '\n';
//        //写入块信息
//        byteBuffer.clear();
//        byteBuffer.position(4);
//        byteBuffer.putInt(qs.fileIndex);
//        byteBuffer.put(qs.last);
//        byteBuffer.put(qs.next);
//
//        // 写入Insert队列
//        for(Map.Entry<byte[], byte[][]> entry: qs.getInsertList().keySet()) {
//            byteBuffer.put(entry.getKey());
//            byteBuffer.put(vert);
//            byte[][] values = entry.getValue();
//            for(int i = 0; i < InsertList.sizeOfFields - 1; i++) {
//                byteBuffer.put(values[i]);
//                byteBuffer.put(vert);
//            }
//            byteBuffer.position(byteBuffer.position() - 1);
//            byteBuffer.put(tab);
//        }
//        byteBuffer.put(br);
//
//        // 写入Update队列
//        //  1) 写入主键字段变更
//        //  2）写入非主键字段变更
//        HashMap<byte[],byte[]> primaryKeyChanges = qs.getUpdateList().getPrimaryKeyChanges();
//        for(Map.Entry<byte[],byte[]> entry : primaryKeyChanges.entrySet()) {
//            byteBuffer.put(entry.getKey());
//            byteBuffer.put(tab);
//            byteBuffer.put(entry.getValue());
//            byteBuffer.put(tab);
//        }
//        //删除最后添加的tab,放入换行
//        byteBuffer.position(byteBuffer.position() - 1);
//        byteBuffer.put(br);
//
//        Map<byte[],HashMap<byte[],byte[]>> update = qs.getUpdateList().getUpdate();
//        for(Map.Entry<byte[], HashMap<byte[], byte[]>> entry : update.entrySet()) {
//            byteBuffer.put(entry.getKey());
//            byteBuffer.put(vert);
//            HashMap<byte[],byte[]> columnChange = entry.getValue();
//            for(Map.Entry<byte[],byte[]> col:columnChange.entrySet()) {
//                byteBuffer.put(InsertList.fields.get(entry.getKey()));
//                byteBuffer.put(col.getValue());
//                byteBuffer.put(vert);
//            }
//            byteBuffer.position(byteBuffer.position() - 1);
//            byteBuffer.put(tab);
//        }
//        byteBuffer.put(br);
//
//        // 写入delete队列
//        ArrayList<byte[]> deleted = qs.getDeleteList().getDeleted();
//        int lenOfDeleted = deleted.size();
//        int i = 0;
//        while(i++ < lenOfDeleted) {
//            byteBuffer.put(deleted.get(i));
//            byteBuffer.put(tab);
//        }
//
//        //写入ByteBuffer数据长度
//        int limit = byteBuffer.position();
//        byteBuffer.position(0);
//        byteBuffer.putInt(limit);
//        byteBuffer.limit(limit);
//        byteBuffer.position(0);
//    }
//
//    public int getIndex() {
//        return fileIndex << 16 + last * 256 + next;
//    }
//}
