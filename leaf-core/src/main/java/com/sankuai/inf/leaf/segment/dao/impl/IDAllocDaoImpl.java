package com.sankuai.inf.leaf.segment.dao.impl;

import com.sankuai.inf.leaf.common.TableVO;
import com.sankuai.inf.leaf.segment.dao.IDAllocDao;
import com.sankuai.inf.leaf.segment.dao.IDAllocMapper;
import com.sankuai.inf.leaf.segment.model.LeafAlloc;
import java.util.HashMap;
import java.util.Map;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;

import javax.sql.DataSource;
import java.util.List;

public class IDAllocDaoImpl implements IDAllocDao {

    SqlSessionFactory sqlSessionFactory;

    public IDAllocDaoImpl(DataSource dataSource) {
        TransactionFactory transactionFactory = new JdbcTransactionFactory();
        Environment environment = new Environment("development", transactionFactory, dataSource);
        Configuration configuration = new Configuration(environment);
        configuration.addMapper(IDAllocMapper.class);
        sqlSessionFactory = new SqlSessionFactoryBuilder().build(configuration);
    }

    @Override
    public List<LeafAlloc> getAllLeafAllocs(String tableName) {
        SqlSession sqlSession = sqlSessionFactory.openSession(false);
        try {
            TableVO tableVO = new TableVO();
            tableVO.setTableName(tableName);
            return sqlSession.selectList("com.sankuai.inf.leaf.segment.dao.IDAllocMapper.getAllLeafAllocs",tableVO);
        } finally {
            sqlSession.close();
        }
    }


    @Override
    public List<String> getAllTags(String tableName) {
        SqlSession sqlSession = sqlSessionFactory.openSession(false);
        TableVO tableVO = new TableVO();
        tableVO.setTableName(tableName);
        try {
            return sqlSession.selectList("com.sankuai.inf.leaf.segment.dao.IDAllocMapper.getAllTags",tableVO);
        } finally {
            sqlSession.close();
        }
    }


    @Override
    public LeafAlloc updateMaxIdAndGetLeafAlloc(String tableName,String tag) {
        SqlSession sqlSession = sqlSessionFactory.openSession();
        try {
            TableVO tableVO = new TableVO();
            tableVO.setTableName(tableName);
            tableVO.setTag(tag);

            sqlSession.update("com.sankuai.inf.leaf.segment.dao.IDAllocMapper.updateMaxId", tableVO);

            LeafAlloc result = sqlSession.selectOne("com.sankuai.inf.leaf.segment.dao.IDAllocMapper.getLeafAlloc", tableVO);
            sqlSession.commit();
            return result;
        } finally {
            sqlSession.close();
        }
    }

    @Override
    public LeafAlloc updateMaxIdByCustomStepAndGetLeafAlloc(String tableName,LeafAlloc leafAlloc) {
        SqlSession sqlSession = sqlSessionFactory.openSession();
        try {


            leafAlloc.setTableName(tableName);
            sqlSession.update("com.sankuai.inf.leaf.segment.dao.IDAllocMapper.updateMaxIdByCustomStep", leafAlloc);

            TableVO tableVO = new TableVO();
            tableVO.setTableName(tableName);
            tableVO.setTag(leafAlloc.getKey());
            LeafAlloc result = sqlSession.selectOne("com.sankuai.inf.leaf.segment.dao.IDAllocMapper.getLeafAlloc", tableVO);
            sqlSession.commit();
            return result;
        } finally {
            sqlSession.close();
        }
    }

}
