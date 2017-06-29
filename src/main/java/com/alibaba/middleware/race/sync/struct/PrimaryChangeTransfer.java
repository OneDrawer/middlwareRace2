package com.alibaba.middleware.race.sync.struct;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.middleware.race.sync.ReadDisk.*;
/**
 * Created by mst on 2017/6/22.
 */
public class PrimaryChangeTransfer extends Thread {
    SendBufferLock sbl = new SendBufferLock();
    static byte separator = '|';
    static byte br = '\n';
    @Override
    public void run() {
        ConcurrentHashMap<Integer, LinkedList<InsertRow>> chm;
        synchronized (lockTransferData) {
            while(transferData.size() == 0) {
                try {
                    transferData.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            chm = transferData;
            transferData = new ConcurrentHashMap<>();
        }

        ByteBuffer curSendBuffer;
        int pointer = -1; // 索引位置标识
        synchronized (sbl) {
            while((pointer = sbl.getFreeBufferIndex()) == -1) {
                try {
                    sbl.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        // 轮流使用
        curSendBuffer = sendBuffer[pointer];
        curSendBuffer.putInt(0);
        byte flag = '1';
        curSendBuffer.put(flag);
        for(Map.Entry<Integer, LinkedList<InsertRow>> entry : chm.entrySet()) {
            curSendBuffer.putInt(entry.getKey());
            LinkedList<InsertRow> insertRows = entry.getValue();
            int len = insertRows.size();
            for(int i = 0; i < len; i++) {
                InsertRow insert = insertRows.removeFirst();
                // 序列化
                curSendBuffer.putLong(insert.getKey());
                byte[][]body = insert.getBody();
                for(int k = 0; k < body.length; k++) {
                    curSendBuffer.put(body[k]);
                    curSendBuffer.put(separator);
                }
                curSendBuffer.position(curSendBuffer.position() - 1);
                curSendBuffer.put(br);
            }
        }
        curSendBuffer.flip();
        synchronized (sendBufferList) {
            sendBufferList.addFirst(curSendBuffer);
        }
    }
}
