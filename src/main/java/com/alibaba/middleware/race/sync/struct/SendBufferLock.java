package com.alibaba.middleware.race.sync.struct;

/**
 * Created by mst on 2017/6/21.
 */
public class SendBufferLock {
    boolean one;  // position = i
    boolean onePlus; // position = i + 8
    public SendBufferLock() {
        this.one = true;
        this.onePlus = true;
    }

    public void freePosition(int pos) {
        if(pos < 8) {
            one = true;
        } else {
            onePlus = true;
        }
    }



    public int getFreeBufferIndex(){
        if(one) {
            one = false;
            return 0;
        }
        if(onePlus){
            onePlus = false;
            return 1;
        }
        //无空闲buffer
        return -1;
    }
}
