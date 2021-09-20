package com.atguigu.gmall.realtime.bean;

import lombok.Data;

/**
 * @author wang
 * @create 2021-09-20 11:57
 */
@Data
public class TableProcess {
    //动态分流Sink常量
    public static final String SINK_TYPE_HBASE = "hbase";
    public static final String SINK_TYPE_KAFKA = "kafka";
    public static final String SINK_TYPE_CK = "clickhouse";
    //来源表
    String sourceTable;
    //操作类型 insert,update,delete
    String operateType;
    //输出类型 hbase kafka
    String sinkType;
    //输出表(主题)
    String sinkTable;
    //输出字段
    String sinkColumns;
    //主键字段    phoenix建表的主键
    String sinkPk;
    //建表扩展
    String sinkExtend;
}