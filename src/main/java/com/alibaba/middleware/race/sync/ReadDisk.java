package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.struct.*;

import static com.alibaba.middleware.race.sync.Constants.DATA_HOME;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * Created by mst on 2017/6/7.
 *
 */
public class ReadDisk extends Thread {
    public static int LINE_BUFFER_SIZE = 256 * 1024;
    public static long START_INDEX;
    public static long END_INDEX;
    public static int TABLE_FIELD_SIZE = 0;
    static FieldInfo fieldInfo = new FieldInfo();
    ArrayList<String> fields = fieldInfo.getFields();
    public static Map<String, Byte> fieldToIndex;
    public final static BufferSlice[] readbufferList = new BufferSlice[8];
    public static final LinkedList<ResultSlice>[] fourCombineResult = new LinkedList[4];
    public static final LinkedList<ResultSlice>[] twoCombineResult = new LinkedList[2];
    public static final LinkedList<ResultSlice> combineResult = new LinkedList<>();
    // 主键由查询范围外更替为查询范围内时，服务器将Insert日志加入此队列
    public static final ConcurrentHashMap<Integer, LinkedList<Long>> transferKeyList = new ConcurrentHashMap<>();
    public static final Object lockTransferData = new Object(); // 更改TransferData数据时使用该锁
    public static ConcurrentHashMap<Integer, LinkedList<InsertRow>> transferData = new ConcurrentHashMap<>();
//    public static final CompressLocker[] lockFour = new CompressLocker[4];
//    public static final CompressLocker[] lockTwo = new CompressLocker[2];
//    public static final CompressLocker lockOne = new CompressLocker(false, false);
    public static final ByteBuffer[] sendBuffer = new ByteBuffer[16];
    static final ByteBuffer[] primarySendBuffer = new ByteBuffer[2];
    static final BufferLocker[] bufferLocker = new BufferLocker[8];
    static final SendBufferLock[] sendBufferLocker = new SendBufferLock[8];
    public static final LinkedList<BufferSendTask> sendBufferList = new LinkedList<>();
    //public final static ResultSlice[] midResult = new ResultSlice[8];
    static boolean hasGenerateField = false;
    static {
        for(int i = 0; i < 8; i++) {
            readbufferList[i] = new BufferSlice();
            bufferLocker[i] = new BufferLocker(0);
            sendBuffer[i] = ByteBuffer.allocateDirect(12 *1024 *1024);
            sendBuffer[i + 8] = ByteBuffer.allocateDirect(12 *1024 *1024);
            sendBufferLocker[i] = new SendBufferLock();
        }
        for(int i = 0; i < 4; i++) {
            fourCombineResult[i] = new LinkedList<>();
        }
        for(int i = 0; i < 2; i++) {
            twoCombineResult[i] = new LinkedList<>();
        }
        // 主键变更ByteBuffer
        for(int i = 0; i < 2; i++) {
            primarySendBuffer[0] = ByteBuffer.allocateDirect(2 * 1024 * 1024);
            primarySendBuffer[1] = ByteBuffer.allocateDirect(2 * 1024 * 1024);
        }
        // 启动压缩和合并线程
        for(int i = 0; i < 8; i++) {
            new RawCompress(i, sendBufferLocker[i]).start();
        }
        for(int i = 0; i < 4; i++) {
            new Combiner(fourCombineResult[i], twoCombineResult[i/2]).start();
        }

        for(int i = 0; i < 2; i++) {
            new Combiner(twoCombineResult[i], combineResult).start();
        }
    }

    public void getField(byte[] buf) {
        int k  = 0;
        int count = 0;
        byte colIndex = 0;

        while(true) {
            if(buf[k] == '|') {
                count++;
                if(count >= 6) {
                    break;
                }
            }
            k++;
        }
        k++;
//        int lastIndex = k;
        //开始解析列名
        while(buf[k] != '\n') {
            int lastIndex = k;
            while (buf[k] != ':') {
                k++;
            }
            String colName = new String(buf, lastIndex, k -lastIndex);
            fields.add(colName);
            fieldToIndex.put(colName, colIndex);
            // 定位到下一个列名字段结束
            while(buf[k] != '|') {
                k++;
            }

            if(buf[k - 1] == '1') {
                fieldInfo.setPrimaryIndex(colIndex);
            }

            k++;
            while(buf[k++] != '|') {}
            while(buf[k++] != '|') {}
            colIndex ++;
        }

        TABLE_FIELD_SIZE = colIndex;
//        fieldToIndex = fieldInfo.getFieldToIndex();
    }
    @Override
    public void run() {
        // 初始化
        ByteBuffer tailBuffer = ByteBuffer.allocate(LINE_BUFFER_SIZE);
        int cur ;
        int iter = 0;
        byte[] buf = new byte[LINE_BUFFER_SIZE];
        byte[] lastBlockRemainData = null;
        BufferSlice curBufferSlice = null;
        for(int i = 1; i <= 10; i++) {
            try {
                File f = new File(DATA_HOME + "/" + i + ".txt");
                FileInputStream fis = new FileInputStream(f);
                FileChannel fc = fis.getChannel();
                while(true) {
                    int curIter = iter % 8;
                    synchronized (bufferLocker[curIter]) {
                        while (bufferLocker[curIter].isAvailable()){
                            try {
                                bufferLocker[curIter].wait();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        bufferLocker[curIter].setState(-1);
                        curBufferSlice = readbufferList[iter];

                    }

                    ByteBuffer body = curBufferSlice.getBody();
                    ByteBuffer tail = curBufferSlice.getTail();
                    curBufferSlice.setIndex(iter);
                    if (lastBlockRemainData != null) {
                        body.put(lastBlockRemainData);
                        lastBlockRemainData = null;
                    }

                    // 解析字段名
                    if (!hasGenerateField) {
                        fc.read(tailBuffer);
                        tailBuffer.flip();
                        cur = 0;
                        do {
                            buf[cur] = tailBuffer.get();
                        } while (buf[cur++] != '\n');
                        //解析行字段
                        getField(buf);

                        // 暂时复用最后一个Buffer发送第一行数据
                        sendBuffer[15].putInt(cur);
                        sendBuffer[15].put(buf);
                        sendBuffer[15].flip();
                        synchronized (sendBufferList) {
                            sendBufferList.add(new BufferSendTask(15, sendBuffer[15]));
                            sendBufferList.notifyAll();
                        }
                        tailBuffer.position(0);
                        body.put(tailBuffer);
                        hasGenerateField = true;
                    }

                    if (fc.read(body) == -1) {
                        body.flip();
                        tail.clear();
                        break;
                    }
                    // 下一步读作准备
                    body.flip();
                    //读取尾部直到换行
                    tailBuffer.clear();
                    cur = 0;
                    //读取尾部时文件结束
                    if (fc.read(tailBuffer) == -1) {
                        tail.put(tailBuffer);
                        break;
                    }
                    do {
                        buf[cur] = tailBuffer.get();
                    } while (buf[cur++] != '\n');

                    tail.put(buf, 0, cur - 1);
                    int remainLength = LINE_BUFFER_SIZE - cur;
                    lastBlockRemainData = new byte[remainLength];
                    tailBuffer.get(lastBlockRemainData);
                    tailBuffer.clear();
                    synchronized (bufferLocker[curIter]) {
                        bufferLocker[curIter].setState(1);
                        bufferLocker[curIter].notifyAll();
                    }
                    iter++;
                }

                // 文件结束

                tailBuffer.clear();
                fc.close();
                lastBlockRemainData = null;
            } catch (IOException e) {
                e.printStackTrace();
            } finally {

            }


        }
//        while (true) {
//            String logPath = null;
//            int fileIdentifier;
//            // 从未处理的日志文件中取出一个文件以处理；
//            if (Server.logFiles.size() > 0) {
//                    FileIndex fi = Server.logFiles.removeFirst();
//                    logPath = fi.getPath();
//                    fileIdentifier = fi.getIndex();
//            } else {
//                return; //已处理完成，退出线程；
//            }
//            try {
//                File log = new File(logPath);
//
//                FileInputStream fin = new FileInputStream(log);
//                FileChannel fs = fin.getChannel();
//                //获取文件长度
//                long lenFile = log.length();
//
//                //获取分块数
//                long count = lenFile / (64*1024*1024);
//
//                //获取剩余文件字节
//                long remain = lenFile % (64*1024*1024);
//                //处理分块
//                byte slice = 0;
//                MappedByteBuffer mbp;
//                for(slice = 0; slice < count; slice++) {
//                     mbp = fs.map(FileChannel.MapMode.READ_ONLY, slice * 16 * 1024 * 1024, 16 * 1024 * 1024);
//                    // 流处理逻辑...
//                    // 表内变更日志：
//                    //  当变更类型为I，插入到InsertList;
//                    //  当变更类型为U, 判断InsertList是否存在该条日志主键，不包含，则更新到
//                    //      UpdateList,包含直接应用到InsertList；
//                    //  当变更类型为D，判断InsertList是否存在该条日志主键，不包含，则更新到
//                    //      DeleteList,包含则将InsertList对应的条目删除。
//
//                    // 块处理后生成的最后数据；
//                    //
//                    QuerySlice qs = new QuerySlice(fileIdentifier, slice, false);
//                    InsertList insertList = qs.getInsertList();
//                    UpdateList updateList = qs.getUpdateList();
//                    DeleteList deleteList = qs.getDeleteList();
//                    byte[] buf = new byte[1024];
//                    while(mbp.remaining() > 0) {
//                        //跳过行首分隔符和前两个字段
//                        mbp.get();
//                        while(mbp.get() != '|'){}
//                        while(mbp.get() != '|'){}
//                        int cur = 0;
//                        byte[] curSchema;
//                        byte[] curTable;
//                        do {
//                            buf[cur] = mbp.get();
//                            if(buf[cur] == '|') {
//                                curSchema = new byte[cur];
//                                System.arraycopy(buf, 0, curSchema, 0, cur);
//                                cur = 0;
//                                break;
//                            }
//                            cur++;
//                        } while(true);
//
//
//                        do {
//                            buf[cur] = mbp.get();
//                            if(buf[cur] == '|') {
//                                curTable = new byte[cur];
//                                System.arraycopy(buf, 0, curTable, 0, cur);
//                                break;
//                            }
//                            cur++;
//
//                        } while(true);
//
//                        // 跳过分隔符 body格式 [id:1:1|102|102|score:1:0|98|95|]
//                        mbp.get();
//                        byte type = mbp.get();
//                        byte[] body = new byte[512];
//                        int index = 0;
//                        do {
//                            body[index] = mbp.get();
//                        } while(body[index ++] != '\n');
//
//                         if(type == 'I') {
//                            LogHandler.putInsert(insertList, body, index);
//                        } else if(type == 'U') {
//                             // updateList 会先将所有的更新暂时放在旧Key里，然后在最后添加一个主键变更记录
//                             UpdateState us = LogHandler.getUpdateState(body, index);
//                             byte[] curKey =  us.getOldKey();
//                             if(updateList.getPrimaryKeyChanges().containsKey(us.getOldKey())) {
//                                curKey = updateList.getPrimaryKeyChanges().get(us.getOldKey());
//                             }
//                             // insertList 不存在此条更新的Insert信息
//                             if(insertList.getData(curKey) == null) {
//                                 //在UpdateList是否存在此主键的更新,存在则合并更新,此时
//                                 if (updateList.getUpdate().get(curKey) == null) {
//                                     updateList.getUpdate().put(curKey, us.getColChanged());
//                                 } else {
//                                    HashMap<byte[], byte[]> hm = updateList.getUpdate().get(curKey);
//                                    hm.putAll(us.getColChanged());
//                                 }
//                                 if(!compare(curKey, us.getNewKey())) {
//                                    updateList.getPrimaryKeyChanges().put(us.getNewKey(), curKey);
//                                 }
//                             } else {
//                                //执行更新InsertList
//                                 LogHandler.updateBody(insertList, us);
//                             }
//
//
//
//                        } else if(type == 'D') {
//                            byte[] primary = LogHandler.getPrimary(body, index);
//                            if(insertList.getData(primary) == null) {
//                                deleteList.add(primary);
//                            }
//                            if (!updateList.getPrimaryKeyChanges().containsValue(primary)) {
//                                deleteList.add(primary);
//                            }
//                        }
//
//                    }
//
//
//                    // 顺序处理查询表的日志，加入qs
//
//                    // 单线程运行此代码，先获得查询表的第一条Insert记录，更新字段到InsertLIst的fields;
//                    // 然后开启多线程处理数据，此处是为了避免同步，后期可改进。
//                    //解析到第一条Insert数据，即发现当前日志记录是查询表的第一条INSERT记录，将字段名按序加入到field，
//                    //field.put(columnName, columnIndex++);
//
//                    //生成最后数据 demo
//                    // 注意updateList和InsertList更新方式的不同
//                    // updateList 更新时会整合同一个主键的所有列更新信息，因此单独更新每一个列项目
//                    // e.g. updateList.getUpdate.put(key, colName, colValue);
//                    //InsertList 会将字段整体放入一个List里在插入列表。
//                    LinkedList ll = new LinkedList();
//                    ll.add(InsertList.fields.get("解析到的列名"),"解析到的值"); // 将解析的数据更新到对应位置，可能会抛出IndexOutOfBoundsException
//                    byte[] key = null; //主键
//                    //insertList.put(key, ll);
//                    //...
//                    //...
//
//
//                    //写入ByteBuffer
//                    ByteBuffer curByteBuffer;
//                    synchronized (writeByteBuffer) {
//                        while(writeByteBuffer.size() == 0) {
//                            try {
//                                writeByteBuffer.wait();
//                            } catch(InterruptedException e) {
//                                e.printStackTrace();
//                            }
//                        }
//                        curByteBuffer = writeByteBuffer.removeFirst();
//                    }
//                    curByteBuffer.flip();
//
//                    //写入数据
//                    QuerySlice.writeObject(curByteBuffer, qs);
//
//                    synchronized (fullByteBuffer) {
//                        fullByteBuffer.add(curByteBuffer);
//                        fullByteBuffer.notifyAll();
//                    }
//
//                }
//                //处理最后一块数据,注意，最后一块数据长度可能为0
//                mbp = fs.map(FileChannel.MapMode.READ_ONLY, slice * 16 * 1024 * 1024, remain);
//
//               // QuerySlice qs = new QuerySlice(logPath, slice -1, -1);
//                // 处理逻辑...同上
//
//
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
    }
}
