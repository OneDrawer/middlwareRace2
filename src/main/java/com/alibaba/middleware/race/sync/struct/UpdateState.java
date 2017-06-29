//package com.alibaba.middleware.race.sync.struct;
//
//import java.util.HashMap;
///**
// * Created by mst on 2017/6/12.
// */
//public class UpdateState {
//    byte[] oldKey;
//    byte[] newKey;
//    HashMap<byte[],byte[]> hm =new HashMap<>();
//    public UpdateState(HashMap<byte[], byte[]> hm, byte[] oldKey , byte[] newKey){
//        this.oldKey = oldKey;
//        this.newKey = newKey;
//        this.hm = hm;
//    }
//    public byte[] getOldKey() {
//        return oldKey;
//    }
//    public byte[] getNewKey() {
//        return newKey;
//    }
//    public HashMap<byte[],byte[]> getColChanged() {
//        return  hm;
//    }
//}
