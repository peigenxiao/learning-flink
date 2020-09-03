package com.dajiangtai.dbus.model;

import com.dajiangtai.dbus.enums.FlowStatusEnum;
import com.dajiangtai.dbus.enums.HBaseStorageModeEnum;
import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

/**
 * @Author: 🐟lifei🐟
 * @Date: 2019/3/6 下午6:07
 * 每张表一个Flow
 */

//
//{
//        mode: STRING                      # HBase中的存储类型, 默认统一存为String, 可选: #PHOENIX  #NATIVE   #STRING
//        # NATIVE: 以java类型为主, PHOENIX: 将类型转换为Phoenix对应的类型
//        destination: example              # 对应 canal destination/MQ topic 名称
//        database: mytest                  # 数据库名/schema名
//        table: person                     # 表名
//        hbaseTable: MYTEST.PERSON         # HBase表名
//        family: CF                        # 默认统一Column Family名称
//        uppercaseQualifier: true          # 字段名转大写, 默认为true
//        commitBatch: 3000                 # 批量提交的大小, ETL中用到
//        #rowKey: id,type                  # 复合字段rowKey不能和columns中的rowKey并存
//        # 复合rowKey会以 '|' 分隔
//        columns:                          # 字段映射, 如果不配置将自动映射所有字段,
//        # 并取第一个字段为rowKey, HBase字段名以mysql字段名为主
//        id: ROWKE
//        name: CF:NAME
//        email: EMAIL                    # 如果column family为默认CF, 则可以省略
//        type:                           # 如果HBase字段和mysql字段名一致, 则可以省略
//        c_time:
//        birthday:
//}
@Data
@ToString
public class Flow implements Serializable {
    private Integer flowId;
    /**
     * HBase中的存储类型, 默认统一存为String,
     * 可选: #PHOENIX  #NATIVE   #STRING
     * # NATIVE: 以java类型为主, PHOENIX: 将类型转换为Phoenix对应的类型
     */
    private int mode= HBaseStorageModeEnum.STRING.getCode();
    /**
     * 数据库名/schema名
     */
    private String databaseName;
    /**
     * 表名
     */
    private String tableName;
    /**
     * 表名
     */
    private String hbaseTable;
    /**
     * 默认统一Column Family名称
     */
    private String family;
    /**
     * 字段名转大写, 默认为true
     */
    private boolean uppercaseQualifier=true;
    /**
     * 批量提交的大小, ETL中用到
     */
    private int commitBatch;
    /**
     *  组成rowkey的字段名，必须用逗号分隔
     */
    private String rowKey;
    /**
     * 状态
     */
    private int status= FlowStatusEnum.FLOWSTATUS_INIT.getCode();
}



