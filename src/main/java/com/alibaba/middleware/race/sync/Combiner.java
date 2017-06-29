package com.alibaba.middleware.race.sync;
import com.alibaba.middleware.race.sync.struct.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import static com.alibaba.middleware.race.sync.ReadDisk.transferData;
import static com.alibaba.middleware.race.sync.ReadDisk.transferKeyList;

/**
 * Created by mst on 2017/6/20.
 */
public class Combiner extends Thread{
    LinkedList<ResultSlice> queue;
    LinkedList<ResultSlice> result;
    public static Logger logger = LoggerFactory.getLogger(Combiner.class);
    public Combiner(LinkedList<ResultSlice> src, LinkedList<ResultSlice> res) {
        this.queue = src;
        this.result = res;
    }

    @Override
    public void run() {
        // 取出待压缩的两个结果片
        ResultSlice cur;
        ResultSlice next;
        synchronized (queue) {
            while(queue.size() < 2) {
                try {
                    queue.wait();
                } catch(InterruptedException e) {
                    e.printStackTrace();
                }
            }
            cur = queue.removeFirst();
            next = queue.removeFirst();
        }

        if(cur.getSeq() > next.getSeq()) {
            ResultSlice temp;
            temp = cur;
            cur = next;
            next = temp;
        }

        // 低概率事件，暂不添加处理逻辑
        if(cur.getSeq() != next.getSeq() - 1){
            logger.error("Sequence wrong! you should add codes to handle this situation!");
        }
        // 执行压缩,将数据压缩到cur
        mergeResult(cur, next);
        synchronized (result) {
            result.add(cur);
        }
    }
    public static ResultSlice mergeResult(ResultSlice cur, ResultSlice next) {

        Map<Long, byte[][]> curInsert = cur.getInsert();
        Map<Long, HashMap<Byte,byte[]>> curUpdate = cur.getUpdate();
        Map<Long, Long> curPrimaryChange = cur.getPrimaryChange();
        ArrayList<Long> curDelete = cur.getDelete();

        // 将deleteList 更新到上一块
        Map<Long, byte[][]> nextInsert = next.getInsert();
        Map<Long, HashMap<Byte,byte[]>> nextUpdate = next.getUpdate();
        Map<Long, Long> nextPrimaryChange = next.getPrimaryChange();
        ArrayList<Long> nextDelete = next.getDelete();

        int len = nextDelete.size();
        for(int i = 0; i < len; i++) {
            long key = nextDelete.get(i);
            if(curInsert.get(key) == null) {
                // 上一块发生了主键变更
                if(curPrimaryChange.containsKey(key)) {
                    // 删除 主键变更条目，更新记录，并添加 变更前主键的删除记录
                    long oldKey = nextPrimaryChange.get(key);
                    // 删除变更主键的记录，并将主键变更所在块内关于此条记录的所有更新删除
                    curPrimaryChange.remove(key);
                    curUpdate.remove(oldKey);
                    // 删除变更主键之前的所有关于此记录的信息
                    curDelete.add(oldKey);
                } else {
                    // 上一块未发生主键变更
                    if(curUpdate.containsKey(key)) {
                        curUpdate.remove(key);
                    }
                    curDelete.add(key);
                }
            } else {
                //所有关于此条记录的信息被清除
                curInsert.remove(key);
            }
        }

        // 在发生主键变更时，我们并不直接更新，除非合并时发现更新前主键的Insert记录，
        // 当发生主键变更时:
        //1)在InsertList中找到了变更前主键 -> 直接在Insert记录中更新
        //2)未在InsertList中找到变更前主键 -> 在原主键(oldKey)上更新字段，
        //  然后在主键变更列表(primaryUpdateList)添加一条变更记录，合并时首先要判断主键
        //  是否包含在主键变更列表中，如果包含，则应使用最初的主键(oldKey)更新列

        for(Map.Entry<Long, HashMap<Byte,byte[]>> entry : nextUpdate.entrySet()) {
            long key = entry.getKey();
            // 若之前发生过主键变更，则将新的键值替换成旧的键值以执行更新
            if(curPrimaryChange.containsKey(key)){
                key = curPrimaryChange.get(key); // 替换为oldkey
            }

            if(curInsert.get(key) == null) {
                if (curUpdate.get(key) == null) {
                    curUpdate.put(key, entry.getValue());
                } else {
                    HashMap<Byte, byte[]> hm = curUpdate.get(key);
                    hm.putAll(entry.getValue());
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

        // 更新主键变更记录
        for(Map.Entry<Long, Long> entry : nextPrimaryChange.entrySet()) {
            long oldKey = entry.getValue();
            long newKey = entry.getKey();
            if(curInsert.containsKey(oldKey)) {
                curInsert.put(newKey, curInsert.remove(oldKey));
            } else if(curPrimaryChange.containsKey(oldKey)){
                curPrimaryChange.put(newKey, curPrimaryChange.get(newKey));
            } else {
                curPrimaryChange.put(newKey, oldKey);
            }
        }
        // 更新Insert
        curInsert.putAll(nextInsert);

        // 判断是否有主键变更数据传送
        int curSeq = cur.getSeq();
        int nextSeq = next.getSeq();

        LinkedList<Long> nextTransferList = transferKeyList.get(nextSeq);
        // 为空则说明下一块没有要传输的主键变更
        if(nextTransferList == null) {
            return cur;
        }

        // 下一块存在需要传输的主键变更
        LinkedList<Long> curTransferList = transferKeyList.get(curSeq);
        if(curTransferList == null) {
            curTransferList = new LinkedList<>();
            transferKeyList.put(curSeq, curTransferList);
        }

        int transLen = nextTransferList.size();
        // 如果在Insert发现需要传输的键值，则加入传输任务列表，并将该条数据从Insert中移除，
        // 否则 加入上一级的传输任务列表
        for(int i = 0; i < transLen; i++) {
            long key  = nextTransferList.getFirst();
            if(curInsert.containsKey(key)) {
                LinkedList<InsertRow> insertRows = transferData.get(cur.getSeq());
                if(insertRows == null) {
                    insertRows = new LinkedList<>();
                    transferData.put(curSeq, insertRows);
                }
                insertRows.add(new InsertRow(key, curInsert.remove(key)));
            } else {
                curTransferList.add(key);
            }
        }
        return cur;
    }
}
