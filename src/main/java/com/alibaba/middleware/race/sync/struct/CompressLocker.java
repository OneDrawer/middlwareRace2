//package com.alibaba.middleware.race.sync.struct;
//
///**
// * 状态锁
// * curUsed 锁对应的第一个结果片
// * nextUsed 锁对应的第二个结果片
// * 状态 : true 当前结果片正在被下一级合并器使用
// *        false 当前结果片空闲，可存放上一级合并器的结果
// * Created by mst on 2017/6/19.
// */
//public class CompressLocker {
//    boolean curUsed;
//    boolean nextUsed;
//    public CompressLocker(boolean cur, boolean next) {
//        this.curUsed = cur;
//        this.nextUsed = next;
//    }
//
//    public boolean getSeqState(int seq) {
//        return seq % 2 == 0 ? curUsed: nextUsed;
//    }
//
//    public void  setSeqState(int seq, boolean state) {
//        if(seq % 2 == 0)
//            curUsed = true;
//        else
//            nextUsed = true;
//    }
//    public void free() {
//        this.curUsed = false;
//        this.nextUsed = false;
//    }
//    public boolean joinable() {
//        return curUsed && nextUsed;
//    }
//}
