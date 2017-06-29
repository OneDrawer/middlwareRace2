package com.alibaba.middleware.race.sync;

import io.netty.buffer.ByteBuf;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;

/**
 * Created by Administrator on 2017/6/7.
 *
 * 读取日志文件 1.txt 2.txt 3.txt ...
 */
public class ReadFile {
    private static int currentMessPos = 0;//读取的文件当前位置
    private static int bufferSize = 2;//每次读一个数据块,注意：这里bufferSize值一旦小于一条数据的长度就会有异常。
    private static int messageSize = 1024;//message长度
    private static byte[] message = new byte[messageSize];//存储从buffer中解析的每行消息,必须为static:因为每个数据块末尾可能不足一条消息，需要和下一个数据块的开头部分就行合并为一个消息整体

    //映射文件filename
    public static MappedByteBuffer mapFile(String filename) throws IOException {
//        String path = Constants.DATA_HOME;
        String path = "data\\";
        //open the file
        String file = new StringBuilder(path).append(filename).toString();
        File f = new File(file);
        if (!(f.exists())) {
            System.out.println("No such file!");
            return null;
        }
        MappedByteBuffer mbb = null;
        try {//映射文件
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            long fileLength= raf.length();
            System.out.println(fileLength);
            mbb = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0L, fileLength);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return mbb;
    }

    //读取映射文件,每次读一个数据块
    @Nullable
    public static void read(MappedByteBuffer mbb) throws IOException {
        int i = 0;
        while (mbb.hasRemaining()) {
            byte[] buffer = new byte[bufferSize];//这里能不能申请一块固定的内存区？？？
            int readLength = Math.min(mbb.remaining(), bufferSize);
            mbb.get(buffer, 0, readLength);//get a data block
            parseBuffer(buffer);
//            readLine(buffer);
        }
    }

    //对数据块逐个字节处理
    public static void parseBuffer(byte[] buf) {
        int bufSize = buf.length;
        int curReadPos = 0;
        while(curReadPos < bufSize) {

        }
    }

    public static void readLine(byte[] bb) {
        int size = bb.length;
        int currentReadPos = 0;
        while(currentReadPos < size) {
            //没有读到块末尾并且当前字节不是换行符
            while( currentReadPos < size) {
                if (bb[currentReadPos] != '\n' && bb[currentReadPos] != '\r') {
                    message[currentMessPos++] = bb[currentReadPos++];
                } else break;
            }
            if(currentReadPos < size) {//上面的循环是换行导致退出，则加上换行
                message[currentMessPos] = bb[currentReadPos];//加上末尾换行
                byte[] ms = new byte[currentMessPos+1];
                System.arraycopy(message, 0, ms, 0, currentMessPos+1);//获得一条完整的数据
                System.out.print(new String(ms));//打印一条消息
                //接下来需要对message做进一步的分割
//                String[] mess = new String(ms).split("|");
                //处理完之后
                message = new byte[bufferSize];
                currentMessPos = 0;
//                if( i < 100) {
//                    System.out.println(new String(ms));
//                    i++;
//                }
            } else {
                //没有遇到完整的行则不作任何处理,在下一个块中继续读取
                break;
            }
            ++currentReadPos;
        }
    }

    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();
        String filename = "canal.txt";
        read(mapFile(filename));
        long end = System.currentTimeMillis();
        System.out.println("read file time:" + (end - start)*1.0/1000 + "s");
    }
}
