//package com.alibaba.middleware.race.sync.struct;
//
//import com.alibaba.middleware.race.sync.Config;
//
///**
// * Created by mst on 2017/6/14.
// */
//public class SeqSlice {
//    int fileIndex;
//    int cur;
//    boolean finished = false;
//
//    public SeqSlice(int fileIndex, int cur) {
//        this.fileIndex = fileIndex;
//        this.cur = cur;
//    }
//    public void setEnd(){
//        this.finished = true;
//    }
//    public int getIndex() {
//        return fileIndex << 16 + cur;
//    }
//}
////    public SeqSlice(int fileIndex, int current, boolean finished) {
////        this.fileIndex = fileIndex;
////        this.cur = current;
////        this.finished = finished;
////    }
////    public void setQuerySlice(QuerySlice querySlice) {
////        qs = querySlice;
////    }
////    public QuerySlice getQuerySlice() {
////        return qs;
////    }
////    public SeqSlice getNextSlice() {
////        if(next != -1) {
////            return new SeqSlice(fileIndex, next, next + 1);
////        } else {
////            if(fileIndex < Config.MAX_FILE_INDEX)
////                return new SeqSlice(fileIndex + 1, -1 , 1);
////            else
////                return null;
////        }
////    }
////    public SeqSlice getLastSlice() {
////        if(last != -1) {
////            return new SeqSlice(fileIndex, last - 1, next + 1);
////        } else {
////            if(fileIndex > Config.MIN_FILE_INDEX)
////                // 我的上一块是上一个文件的最后一个块，通过上一个文件的块结束标识识别
////                return new SeqSlice(fileIndex - 1, Integer.MAX_VALUE, -1);
////            else
////                return null;
////        }
////    }
////    @Override
////    public int hashCode() {
////        return fileIndex<<20 + next<<10 + last;
////    }
////}
