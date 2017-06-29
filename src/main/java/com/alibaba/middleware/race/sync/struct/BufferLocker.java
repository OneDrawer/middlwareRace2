package com.alibaba.middleware.race.sync.struct;

/**
 * state -> -1 正在使用
 * state -> 0 空闲
 * state -> 1 已满
 * Created by mst on 2017/6/19.
 */
public class BufferLocker {
    int state;

    public BufferLocker(int state) {
        this.state = state;
    }

    public boolean isFilled() {
        return state == 1;
    }

    public boolean isAvailable() {
        return state == 0;
    }

    public void setState(int state) {
        this.state = state;
    }
}
