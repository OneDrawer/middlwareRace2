package com.alibaba.middleware.race.sync.struct;

/**
 * Created by mst on 2017/6/22.
 */
public class InsertRow {
    long key;
    byte[][] body;
    public InsertRow(long key, byte[][] body) {
        this.key = key;
        this.body= body;
    }

    public long getKey() {
        return key;
    }

    public byte[][] getBody() {
        return body;
    }
}
