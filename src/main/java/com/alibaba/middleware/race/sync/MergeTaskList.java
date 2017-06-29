//package com.alibaba.middleware.race.sync;
//
//import com.alibaba.middleware.race.sync.struct.*;
//
//import java.nio.ByteBuffer;
//import java.util.*;
//import java.util.concurrent.ConcurrentHashMap;
//
//import static com.alibaba.middleware.race.sync.Client.*;
///**
// * Created by mst on 2017/6/9.
// */
//public class MergeTaskList extends Thread {
//    public static TreeMap<SeqSlice, QuerySlice> taskList = new TreeMap<>(new Comparator<SeqSlice>() {
//
//        @Override
//        public int compare(SeqSlice o1, SeqSlice o2) {
//            return o1.getIndex() - o2.getIndex();
//        }
//    });
//    @Override
//    public void run() {
//        ByteBuffer curBuffer;
//        synchronized (fullBufferList) {
//            while(fullBufferList.size() == 0) {
//                try {
//                    fullBufferList.wait();
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//            curBuffer = fullBufferList.removeFirst();
//        }
//        int length = curBuffer.getInt();
//        int fileIndex = curBuffer.getInt();
//        byte last = curBuffer.get();
//        byte next = curBuffer.get();
//        SeqSlice curSlice;
//        curSlice = new SeqSlice(fileIndex, last + 1);
//        if(next == -1) {
//            curSlice.setEnd();
//        }
//        // next = -1, 当前片是文件的最后一片，特殊处理
//
////        SeqSlice curSeq = new SeqSlice(fileIndex, last, next);;
////        if(last == -1) {
////            SeqSlice ss = new SeqSlice(fileIndex, Integer.MAX_VALUE,-1);
////            if(lastSlice.containsKey(ss)) {
////                lastSlice.remove(ss);
////            }
////        }
////        if(nextSlice.contains(curSeq)) {
////            nextSlice.remove(curSeq);
////        }
////        SeqSlice lastSeq = curSeq.getLastSlice();
////        if(lastSeq != null) {
////            lastSlice.put(lastSeq, curSeq);
////        }
////        SeqSlice nextSeq = curSeq.getNextSlice();
////        if(nextSeq.getNextSlice()!= null) {
////            nextSlice.put(nextSeq, curSeq);
//////        }
//
//
//        QuerySlice qs = new QuerySlice(fileIndex, last, next);
//        InsertList insertList = qs.getInsertList();
//        UpdateList updateList = qs.getUpdateList();
//        DeleteList deleteList = qs.getDeleteList();
//        byte[] buf = new byte[length];
//        curBuffer.get(buf);
//        curBuffer.clear();
//        synchronized (readBufferList) {
//            readBufferList.add(curBuffer);
//            readBufferList.notifyAll();
//        }
//        byte[][] insertRow = new byte[InsertList.fields.size()][];
//        int cur = 0;
//        int lastIndex = 0;
//
//        //InsertList 反序列化
//        int colIndex = 0;
//        while(true) {
//            if(buf[cur] == '|' || buf[cur] =='\t' ||buf[cur] =='\n') {
//                byte[] col = new byte[cur - lastIndex];
//                System.arraycopy(buf, lastIndex, col, 0, cur - lastIndex);
//                insertRow[colIndex++] = col;
//                lastIndex =  cur + 1;
//                if(buf[cur] == '\t' || buf[cur] == '\n') {
//                    insertList.put(insertRow[0],insertRow);
//                    if(buf[cur] == '\n') {
//                        cur++;
//                        break;
//                    }
//                }
//            }
//            cur++;
//        }
//        lastIndex = cur;
//
//        // updateList 主键更新反序列化
//        LinkedList<byte[]> keyUpdate = new LinkedList<>();
//        while(true) {
//            if(buf[cur] == '\t' || buf[cur] =='\n') {
//                byte[] key = new byte[cur - lastIndex];
//                System.arraycopy(buf, lastIndex, key, 0, cur - lastIndex);
//                keyUpdate.add(key);
//                lastIndex = cur + 1;
//                if(buf[cur] == '\n') {
//                    cur++;
//                    break;
//                }
//                cur++;
//            }
//        }
//        for(int i = 0; i < keyUpdate.size(); i++) {
//            updateList.add(keyUpdate.removeFirst(), keyUpdate.removeFirst());
//        }
//
//        // updateList 非主键更新反序列化
//        while(true) {
//            if(buf[cur] == '|') {
//                byte[] key = new byte[cur - lastIndex];
//                System.arraycopy(buf, lastIndex, key, 0, cur - lastIndex);
//                lastIndex =  cur + 1;
//            }
//            if(buf[cur] == '\t' || buf[cur] == '\n') {
//                byte colName = buf[cur + 1];
//                byte[] colValue = new byte[cur - lastIndex - 1];
//                System.arraycopy(buf, lastIndex + 1, colValue, 0, cur - lastIndex - 1);
//                lastIndex = cur + 1;
//                if(buf[cur] == '\n') {
//                    cur++;
//                    break;
//                }
//            }
//            cur++;
//        }
//        while(cur++ < length) {
//            if(buf[cur] == '\t') {
//                byte[] key = new byte[cur - lastIndex];
//                System.arraycopy(buf, lastIndex, key, 0, cur - lastIndex);
//                deleteList.add(key);
//                lastIndex =  cur + 1;
//            }
//        }
//        //DeleteList 反序列化
//
//
//        // 加入队列
//        //查找下一个邻接块
//        SeqSlice expect;
//        if(next == -1) {
//            expect = new SeqSlice(fileIndex + 1, 0);
//        } else {
//            expect = new SeqSlice(fileIndex, next);
//        }
//        synchronized (taskList) {
//            Map.Entry<SeqSlice, QuerySlice> nextSlice = taskList.higherEntry(curSlice);
//            if(nextSlice.getKey().equals(expect)) {
//                //merge Task
//
//            }
//        }
//
//        // 查找上一个邻接块
//        synchronized (taskList) {
//            //taskList.put(curSlice, qs);
//            Map.Entry<SeqSlice, QuerySlice> lastSlice = taskList.lowerEntry(curSlice);
//            QuerySlice querySlice = lastSlice.getValue();
//            if(fileIndex == querySlice.getFileIndex()) {
//                if(next == querySlice.getCur()) {
//                    taskList.remove(lastSlice);
//                    //merge Task
//                }
//            } else if(fileIndex == querySlice.getFileIndex() + 1){
//                if(last == -1 && querySlice.getNext() == -1) {
//                    taskList.remove(lastSlice.getKey());
//                    //merge Task
//                }
//            }
//        }
//
////        int lastIndex  = 0;
////        byte[] remain = null;
////        while(curBuffer.get(buf).remaining() > 0) {
////            int i = 0;
////            for(i = 0; i < buf.length; i++) {
////                if(buf[i] == '|') {
////                    if(remain != null){
////                        byte[] col = new byte[i - lastIndex - 1 + remain.length];
////                        System.arraycopy(remain, 0, col, 0, remain.length);
////                        System.arraycopy(buf, lastIndex, col, remain.length, i - lastIndex);
////                        insertRow.add(col);
////                        lastIndex = i + 1;
////                        remain = null;
////                    } else {
////                        byte[] col = new byte[i - lastIndex - 1];
////                        System.arraycopy(buf, lastIndex, col, 0, i - lastIndex);
////                        insertRow.add(col);
////                        lastIndex = i + 1;
////                    }
////                }
////                if(buf[i] == '\t') {
////                    if(remain != null){
////                        byte[] col = new byte[i - lastIndex - 1 + remain.length];
////                        System.arraycopy(remain, 0, col, 0, remain.length);
////                        System.arraycopy(buf, lastIndex, col, remain.length, i - lastIndex);
////                        insertRow.add(col);
////                        lastIndex = i + 1;
////                        remain = null;
////                    } else {
////                        byte[] col = new byte[i - lastIndex - 1];
////                        System.arraycopy(buf, lastIndex, col, 0, i - lastIndex);
////                        insertList.add(insertRow);
////                        lastIndex = i + 1;
////                    }
////                }
////                if(buf[i] == '\n') {
////                    break;
////                }
////            }
////            if(i == buf.length) {
////                remain = new byte[buf.length - lastIndex];
////                System.arraycopy(buf, lastIndex, remain, 0, i - lastIndex);
////            }
//
//
////        }
//    }
//}
