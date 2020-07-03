package com.sankuai.inf.leaf.segment.dao;

import com.sankuai.inf.leaf.segment.model.LeafWorkerIdAlloc;

/**
 * @author yangjunhui
 * @date 2020/4/30 4:54 下午
 */
public interface WorkerIdAllocDao {

    LeafWorkerIdAlloc getOrCreateLeafWorkerId(String tableName,String ip, String port, String ipPort);

    void updateMaxTimestamp(String tableName,Integer workerId, Long maxTimestamp);
    void deleteNoUseWorkerId(String tableName, Long maxTimestamp);

    void init(String tableName);
}
