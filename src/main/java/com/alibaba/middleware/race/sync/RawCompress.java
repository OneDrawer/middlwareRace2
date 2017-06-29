package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.struct.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.middleware.race.sync.ReadDisk.*;
/**
 * Created by mst on 2017/6/19.
 */
public class RawCompress extends Thread {
    static byte separator = '|';
    static byte br = '\n';
    SendBufferLock sendLocker;
    int sequence;
    RawCompress(int seq, SendBufferLock locker) {
        this.sequence = seq;
        this.sendLocker = locker;
    }

    @Override
    public void run() {
        while (true) {

            synchronized (bufferLocker[sequence]) {
                while (!bufferLocker[sequence].isFilled()) {
                    try {
                        bufferLocker[sequence].wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            //获取当前发送buffer
            ByteBuffer curSendBuffer ;
            int pointer = -1; // 索引位置标识
            synchronized (sendLocker) {
                while((pointer = sendLocker.getFreeBufferIndex()) == -1) {
                    try {
                        sendLocker.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            // 轮流使用
            int location  = sequence + 8;
            if(pointer == 0) {
                location = sequence;
            }
            curSendBuffer = sendBuffer[location];




            BufferSlice curBufferSlice = readbufferList[sequence];
            ByteBuffer curBuffer = curBufferSlice.getBody();
            // 获取结果存放位置
            ResultSlice curResult = new ResultSlice(curBufferSlice.getIndex());

            // 为分片方便，在此预先写入一个字段,当数据写完后会填充该位置为SendBuffer的长度
            curSendBuffer.putInt(0);
            curSendBuffer.putInt(curResult.getSeq());
            // 放入标识字段
            // 0 -> 当前Buffer为非主键变更字段
            // 1 -> 当前Buffer为主键变更字段
            byte flag = '0';
            curSendBuffer.put(flag);


            ConcurrentHashMap<Long, byte[][]> curInsert = curResult.getInsert();
            ConcurrentHashMap<Long, HashMap<Byte, byte[]>> curUpdate = curResult.getUpdate();
            ConcurrentHashMap<Long, Long> curPrimaryUpdate = curResult.getPrimaryChange();
            ArrayList<Long> curDelete = curResult.getDelete();
            byte[] buf = new byte[LINE_BUFFER_SIZE];

            int count = 0;
            boolean hasJoint = false;
            while (curBuffer.remaining() > 0) {
                // 将最初的拼接方式修改为 将tailbuffer追加到curBuffer, 减少代码量
                if(curBuffer.remaining() < LINE_BUFFER_SIZE && !hasJoint) {
                    curBuffer.compact();
                    curBuffer.put(curBufferSlice.getTail());
                    curBuffer.flip();
                    hasJoint = true;
                }
                // 处理一行数据

                // 跳过前四个元素
                while (true) {
                    if (curBuffer.get() == '|') {
                        count++;
                        if (count == 4)
                            break;
                    }
                }
                byte type = curBuffer.get();
                // 跳过‘|’
                curBuffer.get();
                int inc = 0;
                if (type == 'I') {
                    long primary = 0;
                    while (curBuffer.get() != '|') {
                    }
                    while (curBuffer.get() != '|') {
                    }
                    while ((buf[inc] = curBuffer.get()) != '|') {
                        primary = primary *10 + buf[inc] - 48;
                        inc++;
                    }
                    // 加入发送Buffer
                    if (LogUtils.inQueryRange(primary)) {
                        curSendBuffer.put(type);
                        curSendBuffer.putLong(primary);
                        while(curBuffer.get() != '\n') {
                            while (curBuffer.get() != '|') {
                            }
                            while (curBuffer.get() != '|') {
                            }
                            byte temp;
                            while ((temp = curBuffer.get()) != '|') {
                                curSendBuffer.put(temp);
                            }
                        }
                        curSendBuffer.put(br);
                    } else {
                        // 处理本行记录剩下部分
                        byte[][] body = new byte[TABLE_FIELD_SIZE][];
                        int colIndex = 0;
                        while (curBuffer.get() != '\n') {
                            inc = 0;
                            while (curBuffer.get() != '|') {
                            }
                            while (curBuffer.get() != '|') {
                            }
                            while ((buf[inc] = curBuffer.get()) != '|') {

                            }
                            body[colIndex] = new byte[inc];
                            System.arraycopy(buf, 0, body[colIndex], 0, inc);
                            colIndex++;
                        }
                        curInsert.put(primary, body);
                    }

                } else if(type == 'U') {
                    long oldKey = 0;
                    long newKey = 0;
                    while (curBuffer.get() != '|') {
                    }
                    inc = 0;
                    while ((buf[inc] = curBuffer.get()) != '|') {
                        oldKey = oldKey * 10 + buf[inc] - 48;
                        inc++;
                    }
                    inc = 0;
                    while ((buf[inc] = curBuffer.get()) != '|') {
                        newKey = newKey * 10 + buf[inc] - 48;
                        inc++;
                    }
                    // 旧键值在查询范围内，发送给客户端处理
                    if (LogUtils.inQueryRange(oldKey)) {
                        curSendBuffer.put(type);
                        curSendBuffer.putLong(oldKey);
                        curSendBuffer.putLong(newKey);
                        byte temp;
                        do {
                            inc = 0;
                            buf[inc] = curBuffer.get();
                            if(buf[inc] == '\n')
                                break;
                            inc++;
                            while ((buf[inc] = curBuffer.get()) != ':') {
                                inc++;
                            }
                            curSendBuffer.put(fieldToIndex.get(new String(buf, 0, inc)));

                            while (curBuffer.get() != '|') {
                            }
                            while (curBuffer.get() != '|') {
                            }

                            while ((temp = curBuffer.get()) != '|') {
                                curSendBuffer.put(temp);
                            }
                            curSendBuffer.put(separator);
                        } while(buf[inc] != '\n');
                        curSendBuffer.put(br);
                    } else {
                        // 解析本条日志内容
                        HashMap<Byte, byte[]> curUpdateContent = new HashMap<>();
                        while (curBuffer.get() != '\n') {
                            while ((buf[inc] = curBuffer.get()) != ':') {
                                inc++;
                            }
                            byte colIndex = fieldToIndex.get(new String(buf, 0, inc));
                            while (curBuffer.get() != '|') {
                            }
                            while (curBuffer.get() != '|') {
                            }
                            inc = 0;
                            while ((buf[inc] = curBuffer.get()) != '|') {
                                inc ++;
                            }
                            byte[] colValue = new byte[inc];
                            System.arraycopy(buf, 0, colValue, 0, inc);
                            curUpdateContent.put(colIndex, colValue);
                        }

                        // 旧键值不在查询范围内，此时该数据由服务端处理，分两种情况处理
                        // 1) 新键值在查询范围内,旧键值不在查询范围内
                        if(LogUtils.inQueryRange(newKey)) {
                            // 需要将数据发送给客户端
                            // 将本条日志非主键变化更新到Insert然后加入发送队列
                            if(curInsert.contains(oldKey)) {
                                // 先应用本条更新再加入发送队列
                                byte[][] body = curInsert.get(oldKey);
                                HashMap<Byte, byte[]> hm = curUpdate.get(oldKey);
                                for(Map.Entry<Byte, byte[]> e : hm.entrySet()) {
                                    byte index = e.getKey();
                                    // 主键不在body中，应去掉其索引位置
                                    body[index - 1] = e.getValue();
                                }
                                int curSeq = curResult.getSeq();
                                LinkedList<InsertRow> insertRows = transferData.get(curSeq);
                                if(insertRows == null) {
                                    insertRows = new LinkedList<>();
                                    transferData.put(curSeq, insertRows);
                                }
                                insertRows.add(new InsertRow(newKey, body));
                            } else {
                                curPrimaryUpdate.put(newKey, oldKey);
                                // 加入待传送键值列表，表示暂时无法获得完整的信息，需要合并后获得
                                int index = curBufferSlice.getIndex();
                                LinkedList<Long> trans = transferKeyList.get(index);
                                if (trans == null) {
                                    LinkedList<Long> transfer = new LinkedList<>();
                                    transferKeyList.put(index, transfer);
                                    trans = transfer;
                                }
                                trans.add(oldKey);
                                // 更新本条消息的其他字段
                                if (curUpdate.get(oldKey) == null) {
                                    curUpdate.put(oldKey, curUpdateContent);
                                } else {
                                    HashMap<Byte, byte[]> hm = curUpdate.get(oldKey);
                                    hm.putAll(curUpdateContent);
                                }

                            }
                        } else {
                            // 2)新键值不在查询范围内，旧键值也不在查询范围内，直接在服务端更新
                            if(curPrimaryUpdate.containsKey(oldKey)) {
                                curPrimaryUpdate.put(newKey, curPrimaryUpdate.remove(oldKey));
                            }

                            Long key = curPrimaryUpdate.get(newKey);
                            // 把当前应用更新到update
                            if(curInsert.get(key) == null) {
                                if (curUpdate.get(key) == null) {
                                    curUpdate.put(key, curUpdateContent);
                                } else {
                                    HashMap<Byte, byte[]> hm = curUpdate.get(key);
                                    hm.putAll(curUpdateContent);
                                }
                            } else {
                                //直接将更新应用到InsertList
                                byte[][]body = curInsert.get(key);
                                HashMap<Byte, byte[]> hm = curUpdate.get(key);
                                for(Map.Entry<Byte, byte[]> e : hm.entrySet()) {
                                    byte index = e.getKey();
                                    // 主键不在body中，应去掉其索引位置
                                    body[index - 1] = e.getValue();
                                }
                            }

                        }



                        while (curBuffer.get() != '\n') {
                            while ((buf[inc] = curBuffer.get()) != ':') {
                                inc++;
                            }
                            byte colIndex = fieldToIndex.get(new String(buf,0,inc));
                            inc = 0;
                            while (curBuffer.get() != '|') {
                            }
                            while (curBuffer.get() != '|') {
                            }
                            inc = 0;
                            while ((buf[inc] = curBuffer.get()) != '|') {
                                inc++;
                            }
                            byte[] colValue = new byte[inc];
                            System.arraycopy(buf, 0, colValue, 0, inc);

                        }

                    }
                } else if(type == 'D') {
                    long primary = 0;
                    while (curBuffer.get() != '|') {
                    }
                    while (curBuffer.get() != '|') {
                    }
                    while ((buf[inc] = curBuffer.get()) != '|') {
                        primary = primary * 10 + buf[inc] - 48;
                        inc++;

                    }
                    if(LogUtils.inQueryRange(primary)) {
                        curSendBuffer.put(type);
                        curSendBuffer.put(separator);
                        curSendBuffer.putLong(primary);
                        curSendBuffer.put(br);
                    } else {
                        // 本地处理Delete日志
                        if(curInsert.get(primary) == null) {
                            // 上一块发生了主键变更
                            if(curPrimaryUpdate.containsKey(primary)) {
                                // 删除 主键变更条目，更新记录，并添加 变更前主键的删除记录
                                long oldKey = curPrimaryUpdate.get(primary);
                                // 删除变更主键的记录，并将主键变更所在块内关于此条记录的所有更新删除
                                curPrimaryUpdate.remove(primary);
                                curUpdate.remove(oldKey);
                                // 删除变更主键之前的所有关于此记录的信息
                                curDelete.add(oldKey);
                            } else {
                                // 上一块未发生主键变更
                                if(curUpdate.containsKey(primary)) {
                                    curUpdate.remove(primary);
                                }
                                curDelete.add(primary);
                            }
                        } else {
                            //所有关于此条记录的信息被清除
                            curInsert.remove(primary);
                        }
                    }
                }

            }

            // 处理完成
            curBuffer.clear();

            // 释放当前ByteBuffer 允许继续读文件
            synchronized (bufferLocker[sequence]) {
                bufferLocker[sequence].setState(0);
                bufferLocker[sequence].notifyAll();
            }
            // 将当前结果加入到发送队列
            // 写入数据总量,去除开头的长度字段(Int)、序列号以及标识字段(byte)
            int dataSize = curSendBuffer.position() - 9;
            curSendBuffer.putInt(0, dataSize);
            curSendBuffer.flip();

            synchronized (sendBufferList) {
                sendBufferList.add(new BufferSendTask(location, curSendBuffer));

            }
            // 将结果加入结果集,通知合并线程开始合并操作
            synchronized (fourCombineResult[sequence /2]) {
                fourCombineResult[sequence /2].add(curResult);
                fourCombineResult[sequence /2].notifyAll();
            }

        }
    }
}
