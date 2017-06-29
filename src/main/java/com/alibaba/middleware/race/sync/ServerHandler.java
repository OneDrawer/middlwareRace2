package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.struct.BufferSendTask;
//import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static com.alibaba.middleware.race.sync.Constants.DATA_HOME;
import static com.alibaba.middleware.race.sync.ReadDisk.sendBuffer;
import static com.alibaba.middleware.race.sync.ReadDisk.sendBufferList;
import static com.alibaba.middleware.race.sync.ReadDisk.sendBufferLocker;
import static com.alibaba.middleware.race.sync.Server.*;

/*
 * Created by mst on 2017/6/8.
 */
public class ServerHandler extends Thread{
    SocketChannel sc;
    ServerHandler(SocketChannel socket) {
        this.sc = socket;
    }
    @Override
    public void run() {
        ByteBuffer curSendBuffer;
        BufferSendTask bst;
        while(true) {
            synchronized (sendBufferList) {
                while (sendBufferList.size() == 0) {
                    try {
                        sendBufferList.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                bst = sendBufferList.getFirst();
            }
            curSendBuffer = bst.getBuffer();
            int pos = bst.getLocation();
            try {
                while(curSendBuffer.hasRemaining()) {
                    sc.write(curSendBuffer);
                }

            } catch (IOException e) {
                e.printStackTrace();
            }

            synchronized (sendBufferLocker[pos % 8]) {
                sendBufferLocker[pos % 8].freePosition(pos);
                sendBufferLocker[pos % 8].notifyAll();
            }
        }
    }
}
