package com.sankuai.inf.leaf.segment.dao;

import com.sankuai.inf.leaf.segment.model.LeafAlloc;

import java.util.List;

public interface IDAllocDao {
     List<LeafAlloc> getAllLeafAllocs(String tableName);
     LeafAlloc updateMaxIdAndGetLeafAlloc(String tableName,String tag);
     LeafAlloc updateMaxIdByCustomStepAndGetLeafAlloc(String tableName,LeafAlloc leafAlloc);
     List<String> getAllTags(String tableName);
}
