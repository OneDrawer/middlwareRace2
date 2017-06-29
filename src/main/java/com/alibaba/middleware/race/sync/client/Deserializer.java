package com.alibaba.middleware.race.sync.client;

import com.alibaba.middleware.race.sync.LogUtils;
import com.alibaba.middleware.race.sync.struct.ResultSlice;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import static com.alibaba.middleware.race.sync.Client.TABLE_FIELD_SIZE;
import static com.alibaba.middleware.race.sync.Client.taskList;

/**
 * Created by mst on 2017/6/22.
 */
public class Deserializer extends Thread {
    int queueID;
    LinkedList<ReceiveDataSlice> processData;
    public Deserializer(int seq, LinkedList<ReceiveDataSlice> processData) {
        this.queueID = seq;
        this.processData = processData;
    }

    @Override
    public void run() {
        ReceiveDataSlice rds;
        synchronized (taskList[queueID]) {
            while(taskList[queueID].size() == 0) {
                try {
                    taskList[queueID].wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            rds = taskList[queueID].removeFirst();
        }

        // 解析rds
        ResultSlice curResult = new ResultSlice(rds.getSeq());
        ByteBuffer curByteBuffer = rds.getByteBuffer();

        ConcurrentHashMap<Long, byte[][]> curInsert = curResult.getInsert();
        ConcurrentHashMap<Long, HashMap<Byte, byte[]>> curUpdate = curResult.getUpdate();
        ConcurrentHashMap<Long, Long> curPrimaryUpdate = curResult.getPrimaryChange();
        ArrayList<Long> curDelete = curResult.getDelete();
        byte type = curByteBuffer.get();
        long key = curByteBuffer.getLong();
        byte[] buf = new byte[1024];
        int inc;
        if(type == 'I') {
            byte[][] body = new byte[TABLE_FIELD_SIZE][];
            for(int colIndex = 1; colIndex < TABLE_FIELD_SIZE; colIndex++) {
                inc = 0;
                while ((buf[inc] = curByteBuffer.get()) != '|') {
                    inc++;
                }
                body[colIndex] = new byte[inc];
                System.arraycopy(buf, 0, body[colIndex], 0, inc);
            }
            curInsert.put(key, body);
        } else if(type == 'U') {
            long newKey = curByteBuffer.getLong();
            // 非主键变更
            // 更新内容
            HashMap<Byte, byte[]> hm = new HashMap<>();
            while(true) {
                byte colIndex = curByteBuffer.get();
                if(colIndex == '\n')
                    break;
                inc = 0;
                while ((buf[inc] = curByteBuffer.get()) == '|') {
                    inc++;
                }
                byte[] colValue = new byte[inc];
                System.arraycopy(buf, 0, colValue, 0, inc);
                hm.put(colIndex, colValue);
            }
            // 应用更新
            if(newKey == key) {
                // 使用旧键值更新数据
                if(curPrimaryUpdate.get(key) != null) {
                    key = curPrimaryUpdate.get(key);
                }
                if(curInsert.containsKey(key)) {
                    byte[][] body = curInsert.get(key);
                    for(Map.Entry<Byte, byte[]> entry : hm.entrySet()) {
                        body[entry.getKey() - 1] = entry.getValue();
                    }
                } else {
                    if(curUpdate.containsKey(key)) {
                        curUpdate.get(key).putAll(hm);
                    } else {
                        curUpdate.put(key, hm);
                    }
                }
            } else {
                // 题目只提到主键可能由范围外变更为范围内，因此
                // 暂不考虑主键首先变更为查询范围外,然后再变更为查询范围内的情况
                // 考虑情况如下 1)查询范围内变更为查询外，2)查询范围内变更为查询范围内

                // 变更后主键在查询范围内
                if (LogUtils.inQueryRange(newKey)) {
                    if (curPrimaryUpdate.containsKey(key)) {
                        curPrimaryUpdate.put(newKey, curPrimaryUpdate.remove(key));
                        key = curPrimaryUpdate.get(newKey);
                    }

                    // 把当前应用更新到update
                    if (curInsert.get(key) == null) {
                        if (curUpdate.get(key) == null) {
                            curUpdate.put(key, hm);
                        } else {
                            curUpdate.get(key).putAll(hm);
                        }
                    } else {
                        //直接将更新应用到InsertList
                        byte[][] body = curInsert.get(key);
                        //HashMap<Byte, byte[]> hm = curUpdate.get(key);
                        for (Map.Entry<Byte, byte[]> e : hm.entrySet()) {
                            byte index = e.getKey();
                            // 主键不在body中，应去掉其索引位置
                            body[index - 1] = e.getValue();
                        }
                    }
                } else {
                    // 主键由范围内变更为范围外, 直接删除, 此时将不考虑再次由范围外变更为范围内
                    if(curInsert.containsKey(key)) {
                        curInsert.remove(key);
                    } else {
                        if(curPrimaryUpdate.containsKey(key)) {
                            key = curPrimaryUpdate.remove(key);
                        }
                        if(curUpdate.containsKey(key)) {
                            curUpdate.remove(key);
                        }
                        curDelete.add(key);
                    }
                }
            }
        } else if(type == 'D'){
            if(curInsert.containsKey(key)) {
                curInsert.remove(key);
            } else {
                if(curPrimaryUpdate.containsKey(key)) {
                    key = curPrimaryUpdate.remove(key);
                    if(curUpdate.containsKey(key)) {
                        curUpdate.remove(key);
                    }
                } else {
                    if(curUpdate.containsKey(key)) {
                        curUpdate.remove(key);
                    }
                }
                curDelete.add(key);
            }
        }

    }
}
