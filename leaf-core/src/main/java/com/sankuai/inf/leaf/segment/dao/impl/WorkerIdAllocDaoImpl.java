package com.sankuai.inf.leaf.segment.dao.impl;

import co.faao.plugin.starter.properties.PropertiesValue;
import com.mysql.jdbc.TimeUtil;
import com.sankuai.inf.leaf.common.TableVO;
import com.sankuai.inf.leaf.segment.dao.WorkerIdAllocDao;
import com.sankuai.inf.leaf.segment.dao.WorkerIdAllocMapper;
import com.sankuai.inf.leaf.segment.model.LeafWorkerIdAlloc;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
@Slf4j
public class WorkerIdAllocDaoImpl implements WorkerIdAllocDao {

    //workerid 最大的空闲时间，超过就回收   10分钟
    private  static  int maxIdlTime = 10 * 60 * 1000;
    private    String  tableName = "";
    private static final List<Integer> WORKERIDS = new ArrayList<>();

    SqlSessionFactory sqlSessionFactory;

    public WorkerIdAllocDaoImpl(DataSource dataSource,String tableName) {
        TransactionFactory transactionFactory = new JdbcTransactionFactory();
        Environment environment = new Environment("development", transactionFactory, dataSource);
        Configuration configuration = new Configuration(environment);
        configuration.addMapper(WorkerIdAllocMapper.class);
        sqlSessionFactory = new SqlSessionFactoryBuilder().build(configuration);
        this.tableName = tableName;
    }

    @Override
    public LeafWorkerIdAlloc getOrCreateLeafWorkerId(String tableName,String ip, String port, String ipPort) {
        SqlSession sqlSession = sqlSessionFactory.openSession(false);
        try {
            this.init(tableName);
            //先查询已经存在的workerId
            LeafWorkerIdAlloc param = new LeafWorkerIdAlloc();
            param.setServiceName(PropertiesValue.servicenameurl);
            param.setTableName(tableName);
            List<LeafWorkerIdAlloc> workIdAllocList = sqlSession.selectList("com.sankuai.inf.leaf.segment.dao.WorkerIdAllocMapper.getLeafWorkerIdAlloc", param);
            List<Integer> useeWorkerIds = workIdAllocList.stream().map(leafWorkerIdAlloc -> leafWorkerIdAlloc.getWorkerId()).collect(Collectors.toList());
            WORKERIDS.removeAll(useeWorkerIds);

            //从未使用的wokerid中选一个
            LeafWorkerIdAlloc workerIdAlloc = new LeafWorkerIdAlloc();
            workerIdAlloc.setWorkerId(WORKERIDS.get(new Random().nextInt(WORKERIDS.size())));
            workerIdAlloc.setServiceName(PropertiesValue.servicenameurl);
            workerIdAlloc.setIpPort(ipPort);
            workerIdAlloc.setTableName(tableName);
            workerIdAlloc.setDescriptions("分布式ID");
            workerIdAlloc.setLastUpdateTime(System.currentTimeMillis());
            int status = sqlSession.insert("com.sankuai.inf.leaf.segment.dao.WorkerIdAllocMapper.insertNotUsedWorkerId", workerIdAlloc);

            sqlSession.commit();
            log.error("初始化workerId成功！");
            return workerIdAlloc;
        } catch (Exception e) {
            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e1) {
            }
            log.error("初始化workerId异常："+e.getMessage());
           return this.getOrCreateLeafWorkerId(tableName,ip,port,ipPort);

        } finally {
            sqlSession.close();
        }
    }

    @Override
    public void updateMaxTimestamp(String tableName,Integer workerId, Long maxTimestamp) {
        SqlSession sqlSession = sqlSessionFactory.openSession(true);
        try {
            LeafWorkerIdAlloc workerIdAlloc = new LeafWorkerIdAlloc();
            workerIdAlloc.setWorkerId(workerId);
            workerIdAlloc.setTableName(tableName);
            workerIdAlloc.setLastUpdateTime(System.currentTimeMillis());
            sqlSession.update("com.sankuai.inf.leaf.segment.dao.WorkerIdAllocMapper.updateMaxTimestamp", workerIdAlloc);
        } finally {
            sqlSession.close();
        }
    }
    @Override
    public void deleteNoUseWorkerId(String tableName, Long lastTimestamp) {
        SqlSession sqlSession = sqlSessionFactory.openSession(true);
        try {
            LeafWorkerIdAlloc workerIdAlloc = new LeafWorkerIdAlloc();
            workerIdAlloc.setTableName(tableName);
            workerIdAlloc.setLastUpdateTime(lastTimestamp);
            workerIdAlloc.setServiceName(PropertiesValue.servicenameurl);
            sqlSession.update("com.sankuai.inf.leaf.segment.dao.WorkerIdAllocMapper.deleteNoUseWorkerId", workerIdAlloc);
        } finally {
            sqlSession.close();
        }
    }
    @Override
    public void init(String p_tableName) {
        tableName = p_tableName;
        //初始化所有 workerid
        for(int i = 0;i<1024;i++ ) {
            WORKERIDS.add(i);
        }
        //启动定时任务删除不用的workerid
        Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "schedule-delete-workerId");
                thread.setDaemon(true);
                return thread;
            }
        }).scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                WorkerIdAllocDaoImpl.this.deleteNoUseWorkerId(tableName,System.currentTimeMillis() - maxIdlTime);
            }
        }, 60L, 3600L, TimeUnit.SECONDS);//每1小时清除一次
    }

}
