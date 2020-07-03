package com.sankuai.inf.leaf.segment.dao;

import com.sankuai.inf.leaf.segment.model.LeafWorkerIdAlloc;
import java.util.List;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

public interface WorkerIdAllocMapper {

    @Insert({
            "insert into ${tableName} (service_name,worker_id,ip_port,descriptions,last_update_time) ",
            "values (#{serviceName,jdbcType=VARCHAR},#{workerId,jdbcType=INTEGER},#{ipPort,jdbcType=VARCHAR},  ",
            "#{descriptions,jdbcType=VARCHAR}, #{lastUpdateTime,jdbcType=NUMERIC})"
    })
    int insertNotUsedWorkerId(@Param("leafWorkerIdAlloc") LeafWorkerIdAlloc leafWorkerIdAlloc);

    @Select("SELECT * FROM ${tableName} WHERE service_name = #{serviceName,jdbcType=VARCHAR}")
    @Results(value = {
            @Result(column = "worker_id", property = "workerId"),
            @Result(column = "service_name", property = "serviceName"),
            @Result(column = "ip_port", property = "ipPort"),
            @Result(column = "last_update_time", property = "lastUpdateTime"),
            @Result(column = "descriptions", property = "descriptions")
    })
    List<LeafWorkerIdAlloc> getLeafWorkerIdAlloc(@Param("leafWorkerIdAlloc") LeafWorkerIdAlloc leafWorkerIdAlloc);

    @Update("UPDATE ${tableName} SET last_update_time = #{lastUpdateTime,jdbcType=NUMERIC} WHERE worker_id = #{workerId,jdbcType=INTEGER} ")
    void updateMaxTimestamp(@Param("leafWorkerIdAlloc") LeafWorkerIdAlloc leafWorkerIdAlloc);

    @Update("DELETE from  ${tableName} WHERE last_update_time < #{lastUpdateTime,jdbcType=NUMERIC} and service_name = #{serviceName,jdbcType=VARCHAR} ")
    void deleteNoUseWorkerId(@Param("leafWorkerIdAlloc") LeafWorkerIdAlloc leafWorkerIdAlloc);

}
