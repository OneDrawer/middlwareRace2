package com.alibaba.middleware.race.sync.struct;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by mst on 2017/6/20.
 */
public class ResultSlice {
    ConcurrentHashMap<Long, byte[][]> insert = new ConcurrentHashMap<>();
    ConcurrentHashMap<Long, HashMap<Byte, byte[]>> update = new ConcurrentHashMap<>();
    ConcurrentHashMap<Long, Long> primaryUpdate = new ConcurrentHashMap<>();
    ArrayList<Long> delete = new ArrayList<>();
    int seq;
    Object has;
    public ResultSlice(int seq) {
        this.seq = seq;
    }

    public int getSeq() {
        return seq;
    }

    public ConcurrentHashMap<Long, byte[][]> getInsert() {
        return insert;
    }

    public ConcurrentHashMap<Long, HashMap<Byte, byte[]>> getUpdate() {
        return update;
    }

    public ConcurrentHashMap<Long, Long> getPrimaryChange() {
        return primaryUpdate;
    }

    public ArrayList<Long> getDelete() {
        return delete;
    }

    public void clear() {
        insert = null;
        update = null;
        primaryUpdate = null;
        delete = null;

    }
 }
