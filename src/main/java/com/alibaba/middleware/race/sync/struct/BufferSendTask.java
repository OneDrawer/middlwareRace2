package com.alibaba.middleware.race.sync.struct;

import java.nio.ByteBuffer;

/**
 * Created by mst on 2017/6/22.
 */
public class BufferSendTask {
    int location;
    ByteBuffer byteBuffer;
    public BufferSendTask(int loc, ByteBuffer byteBuffer) {
        this.location = loc;
        this.byteBuffer = byteBuffer;
    }
    public ByteBuffer getBuffer () {
        return byteBuffer;
    }
    public int getLocation() {
        return location;
    }

}
