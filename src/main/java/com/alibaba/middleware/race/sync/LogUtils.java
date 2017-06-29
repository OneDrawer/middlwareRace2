package com.alibaba.middleware.race.sync;

/**
 * Created by mst on 2017/6/19.
 */
public class LogUtils {
    static boolean compare(byte[] src, byte[] dest) {
        int len = src.length;
        if(len != dest.length) {
            return false;
        }
        int i = 0;
        while(i < len) {
            if(src[i] != dest[i]) {
                return false;
            }
            i++;
        }
        return true;
    }
    static public boolean inQueryRange(long src) {
        return src > ReadDisk.START_INDEX && src < ReadDisk.END_INDEX;
    }
//    static boolean inQueryRange(byte[] src, byte[] low, byte[] high) {
//        int len = src.length;
//        int down = low.length;
//        if(len == down ) {
//            int i = 0;
//            while (i < len) {
//                if (src[i] != low[i]) {
//                    if(src[i] > low[i]) {
//                        break;
//                    } else {
//                        return false;
//                    }
//                }
//                i++;
//                if(i == len)
//                    return false;
//            }
//        } else if(len > down){
//
//        } else {
//            return false;
//        }
//
//        if(len == high.length){
//            int i = 0;
//            while (i < len) {
//                if (src[i] != high[i]) {
//                    if(src[i] < high[i]) {
//                        return true;
//                    } else {
//                        return false;
//                    }
//                }
//                i++;
//            }
//            return false;
//        } else if(len > high.length){
//            return false;
//        } else {
//            return true;
//        }
//    }

}
