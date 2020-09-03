package com.dajiangtai.dbus.model;

import java.io.Serializable;

/**
 * @Author: 🐟lifei🐟
 * @Date: 2019/3/6 下午11:03
 * 在生产上一般通过配置中心来管理
 */
public class GlobalConfig implements Serializable {
    /**
     * 数据库driver class
     */
    public static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";
    /**
     * 数据库jdbc url
     */
    public static final String DB_URL = "jdbc:mysql://127.0.0.1:3306/test?useUnicode=true&characterEncoding=utf8";
    /**
     * 数据库user name
     */
    public static final String USER_MAME = "root";
    /**
     * 数据库password
     */
    public static final String PASSWORD = "root%123";
    /**
     * 批量提交size
     */
    public static final int BATCH_SIZE = 2;

    //HBase相关配置
    public static final String HBASE_ZOOKEEPER_QUORUM = "slave01,slave02,slave03";
    public static final String HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "2181";
    public static final String ZOOKEEPER_ZNODE_PARENT = "/hbase-unsecure";



}
