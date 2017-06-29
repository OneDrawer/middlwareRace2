package com.alibaba.middleware.race.sync.struct;

import java.nio.ByteBuffer;

/**
 * Created by mst on 2017/6/19.
 */
public class BufferSlice {
    ByteBuffer body = ByteBuffer.allocate(32 * 1024 * 1024);
    ByteBuffer tail = ByteBuffer.allocate(512 * 1024);
    int index;
    public ByteBuffer getBody () {
        return body;
    }

    public ByteBuffer getTail() {
        return tail;
    }

    public int getIndex() {
        return index;
    }
    public void setIndex(int index) {
        this.index = index;
    }
}
