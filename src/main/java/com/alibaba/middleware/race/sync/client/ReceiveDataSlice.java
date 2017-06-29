package com.alibaba.middleware.race.sync.client;

import java.nio.ByteBuffer;

/**
 * Created by mst on 2017/6/22.
 */
public class ReceiveDataSlice {
    int seq;
    ByteBuffer byteBuffer;
    public ReceiveDataSlice(int seq, ByteBuffer byteBuffer) {
        this.seq =seq;
        this.byteBuffer = byteBuffer;
    }

    public int getSeq() {
        return seq;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }
}
