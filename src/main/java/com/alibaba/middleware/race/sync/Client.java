package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.client.ReceiveDataSlice;
import com.alibaba.middleware.race.sync.struct.FieldInfo;
//import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;

import static com.alibaba.middleware.race.sync.Server.*;

/**
 * Created by wanshao on 2017/5/25.
 */
public class Client {

    private final static int port = Constants.SERVER_PORT;
    // idle时间
    private static String ip;
    public static final LinkedList<ByteBuffer> readBufferList = new LinkedList<>();
    public static final LinkedList<ReceiveDataSlice> taskList[] = new LinkedList[8];
    public static int TABLE_FIELD_SIZE = 0;
    static FieldInfo fieldInfo = new FieldInfo();
    ArrayList<String> fields = fieldInfo.getFields();
    public static Map<Byte, String> indexToField;
    static {
        for (int i = 0; i < 16; i++) {
            readBufferList.add(ByteBuffer.allocateDirect(12 * 1024 * 1024));
        }
        for(int i = 0; i < 8; i++) {
            taskList[i] = new LinkedList<>();
        }

    }
    public static void main(String[] args) throws Exception {
        initProperties();
        Logger logger = LoggerFactory.getLogger(Client.class);
        logger.info("Welcome");
        // 从args获取server端的ip
        //ip = args[0];
        ip = "127.0.0.1";
        Client client = new Client();
        client.connect(ip, port);
    }

    /**
     * 初始化系统属性
     */
    private static void initProperties() {
        System.setProperty("middleware.test.home", Constants.TESTER_HOME);
        System.setProperty("middleware.teamcode", Constants.TEAMCODE);
        System.setProperty("app.logging.level", Constants.LOG_LEVEL);
    }

    /**
     * 连接服务端
     *
     * @param host
     * @param port
     * @throws Exception
     */
    public void connect(String host, int port) throws Exception {
        SocketChannel sc = SocketChannel.open();
        sc.connect(new InetSocketAddress(host, port));
        System.out.println("start Client");
        ByteBuffer buffer = ByteBuffer.allocateDirect(9);
        ByteBuffer curReadBuffer;
        sc.configureBlocking(true);
        int size = 0;
        boolean isHead = true;
        int leftDataSize  = 0;
        int dataLength = 0;
        int seq = 0;
        boolean isPrimaryUpdate;
        int sum = 0;
        byte[] unProcessedData = null;
        // 服务端向客户端发送第一行数据
        // 客户端接受服务器发送的第一行数据
        while(sc.read(buffer) != -1) {
            while(buffer.hasRemaining()) {
                sc.read(buffer);
            }
            buffer.flip();
            int rowLength = buffer.getInt();
            ByteBuffer lineBuffer = ByteBuffer.allocateDirect(rowLength);
            while(lineBuffer.hasRemaining()) {
                sc.read(lineBuffer);
            }
            byte[] buf = new byte[rowLength];
            lineBuffer.get(buf);
            getField(buf);

        }
        while((sc.read(buffer))!= -1) {
            //直到读取一个完整头部
            while(buffer.hasRemaining()) {
                sc.read(buffer);
            }
            buffer.flip();
            leftDataSize = buffer.getInt();
            seq = buffer.getInt();

            if(buffer.get() == '1') {
                isPrimaryUpdate = true;
            } else {
                isPrimaryUpdate = false;
            }

            synchronized (readBufferList) {
                //取Buffer
                while(readBufferList.size() == 0)
                    readBufferList.wait();
                curReadBuffer = readBufferList.getFirst();
            }
            if(unProcessedData != null){
                curReadBuffer.put(unProcessedData);
                unProcessedData = null;
            }
            while(leftDataSize > 0) {
                size = sc.read(curReadBuffer);
                leftDataSize -= size;
            }

            // 处理粘包
            int overRead =  -leftDataSize;
            curReadBuffer.flip();
            curReadBuffer.position(dataLength);
            buffer.clear();
            if(overRead < 8) {
                // 下一数据包头部未被完全读取,此时结束本次循环，继续从通道读取数据到buffer里
                buffer.put(curReadBuffer);
            } else {
                // 下一数据包头部已被上一个ByteBuffer读取
                buffer.put(curReadBuffer);
                unProcessedData = new byte[curReadBuffer.remaining()];
                curReadBuffer.get(unProcessedData);
            }

            curReadBuffer.limit(dataLength);
            curReadBuffer.position(0);
            int queueId = seq % 8;
            // 加入不同线程的任务队列
            synchronized (taskList[queueId]) {
                taskList[queueId].add(new ReceiveDataSlice(seq, curReadBuffer));
                taskList[queueId].notifyAll();
            }
            logger.info("Size " + size);
        }
        //EventLoopGroup workerGroup = new NioEventLoopGroup();

//        try {
//            Bootstrap b = new Bootstrap();
//            b.group(workerGroup);
//            b.channel(NioSocketChannel.class);
//            b.option(ChannelOption.SO_KEEPALIVE, true);
//            b.handler(new ChannelInitializer<SocketChannel>() {
//
//                @Override
//                public void initChannel(SocketChannel ch) throws Exception {
//                    ch.pipeline().addLast(new IdleStateHandler(10, 0, 0));
//                    ch.pipeline().addLast(new ClientIdleEventHandler());
//                    ch.pipeline().addLast(new ClientDemoInHandler());
//                }
//            });
//
//            // Start the client.
//            ChannelFuture f = b.connect(host, port).sync();
//
//            // Wait until the connection is closed.
//            f.channel().closeFuture().sync();
//        } finally {
//            workerGroup.shutdownGracefully();
//        }

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
            indexToField.put(colIndex, colName);
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
        indexToField = fieldInfo.getIndexToField();
    }
}
