package com.sankuai.inf.leaf.segment.dao;

import com.sankuai.inf.leaf.common.TableVO;
import com.sankuai.inf.leaf.segment.model.LeafAlloc;
import org.apache.ibatis.annotations.*;

import java.util.List;

public interface IDAllocMapper {

    @Select("SELECT biz_tag, max_id, step, update_time FROM ${tableName}")
    @Results(value = {
            @Result(column = "biz_tag", property = "key"),
            @Result(column = "max_id", property = "maxId"),
            @Result(column = "step", property = "step"),
            @Result(column = "update_time", property = "updateTime")
    })
    List<LeafAlloc> getAllLeafAllocs(@Param("tableVO") TableVO tableVO);

    @Select("SELECT biz_tag FROM ${tableName}")
    List<String> getAllTags(@Param("tableVO") TableVO tableVO);



    @Select("SELECT biz_tag, max_id, step FROM ${tableName} WHERE biz_tag = #{tag}")
    @Results(value = {
            @Result(column = "biz_tag", property = "key"),
            @Result(column = "max_id", property = "maxId"),
            @Result(column = "step", property = "step")
    })
    LeafAlloc getLeafAlloc(@Param("tableVO") TableVO tableVO);

    @Update("UPDATE ${tableName} SET max_id = max_id + step WHERE biz_tag = #{tag}")
    void updateMaxId(@Param("tableVO") TableVO tableVO);

    @Update("UPDATE ${tableName} SET max_id = max_id + #{step} WHERE biz_tag = #{key}")
    void updateMaxIdByCustomStep(@Param("leafAlloc") LeafAlloc leafAlloc);

}
