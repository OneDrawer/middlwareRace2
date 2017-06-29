package com.alibaba.middleware.race.sync;

import java.net.InetSocketAddress;
import java.util.Comparator;
import java.util.HashMap;
import java.util.TreeMap;

/**
 * Created by mst on 2017/6/8.
 */
public class MainTest {
    static boolean isBetween(byte[] src, byte[] low, byte[] high) {
        int len = src.length;
        int down = low.length;
        if(len == down ) {
            int i = 0;
            while (i < len) {
                if (src[i] != low[i]) {
                    if(src[i] > low[i]) {
                        break;
                    } else {
                        return false;
                    }
                }
                i++;
                if(i == len)
                  return false;
            }
        } else if(len > down){

        } else {
            return false;
        }

        if(len == high.length){
            int i = 0;
            while (i < len) {
                if (src[i] != high[i]) {
                    if(src[i] < high[i]) {
                        return true;
                    } else {
                        return false;
                    }
                }
                i++;
            }
            return false;
        } else if(len > high.length){
            return false;
        } else {
            return true;
        }

    }
    public static void main(String[] agrs) {
        byte[] ch = new byte[5];
        byte[] ch2 = new byte[5];
        byte a = '3';
        System.out.println(a);

    }
}
