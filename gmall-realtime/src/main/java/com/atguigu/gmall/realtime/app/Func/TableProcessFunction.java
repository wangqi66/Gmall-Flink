package com.atguigu.gmall.realtime.app.Func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * 将数据封装成javabean，并根据bean中数据确定相应的操作（存储（kafka，hbase），（建表/键主题））
 *
 * @author wang
 * @create 2021-09-20 20:11
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    //广播需求
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    //phonix连接
    private Connection connection;

    //侧输出流
    private OutputTag<JSONObject> hbaseTag;

    public TableProcessFunction() {
    }

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor, OutputTag<JSONObject> hbaseTag) {
        this.mapStateDescriptor = mapStateDescriptor;
        this.hbaseTag = hbaseTag;
    }

    /**
     * 在此方法创建hbase的连接(phonex)
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //对广播流进行处理（数据封装javabean，并验证表的存在性，新建表之类的）
    //value:{"datebase":"","tablename":"","data":{"sourceTable":"",...},"before":{},"type":"insert"}
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        //获取数据 数据转json对象
        JSONObject jsonObj = JSONObject.parseObject(value);
        String data = jsonObj.getString("data");
        //将data封装成javabean
        TableProcess tableProcess = JSONObject.parseObject(data, TableProcess.class);

        //验证表是否存在(insert,且插入hbse才要创建表)（广播流存的是建表和分表（主题）文件数据）
        String type = jsonObj.getString("type");
        if ("insert".equals(type) && TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            //创建表
            checkTable(tableProcess.getSinkTable(),//表名
                    tableProcess.getSinkColumns(),//列
                    tableProcess.getSinkPk(),//主键
                    tableProcess.getSinkExtend());//补充字段（innodb等）
        }

//        //将流数据写入状态，并广播出去
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
//        String key = jsonObj.getString("tableName") + "_" + jsonObj.getString("type");
        String key=tableProcess.getSourceTable()+"_"+tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);

        //3.写入状态,广播出去
//        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
//        String key = tableProcess.getSourceTable() + "_" + tableProcess.getOperateType();
//        broadcastState.put(key, tableProcess);

    }


    //对主流，也就是读取的数据流进行处理
    //获取状态数据，对数据进行对应，写入对应的hbase表或者kafka主题(传递到下游)
    //value:{"database":"","tableName":"","data":{"id":"","tm_name":"","logo_url":""},"before":{},"type":"insert"}
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

//        //获取广播状态
//        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
//        String key = value.getString("tableName") + "_" + value.getString("type");
//        TableProcess tableProcess = broadcastState.get(key);

        //1.获取广播状态数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = value.getString("tableName") + "_" + value.getString("type");
        TableProcess tableProcess = broadcastState.get(key);

        //分流
        //对数据进行选取，因为数据存储到kafka/hbabe可能并不需要这么多字段

        if (tableProcess != null) {
            //过滤数据
            filterColumn(value.getJSONObject("data"), tableProcess.getSinkColumns());
            //数据写入hbase(传递到下游（侧输出流）)
            //写入hbase的数据需要目标表名，故需要写出去
            value.put("sinkTable",tableProcess.getSinkTable());

            if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                ctx.output(hbaseTag,value);
            } else if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())) {
                //数据写入kafka（传递到下游，主流）
                out.collect(value);
            }

        } else {
            System.out.println(key + "不存在，可能是广播流传输错字段的问题");
        }


    }


    /**
     * 创建hbase表的方法 create table  if not exists db.t(id varchar....) extend....
     *
     * @param sinkTable
     * @param sinkColumns
     * @param sinkPk
     * @param sinkExtend
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        //提前处理列值 主键默认为id，sinextend有可能为空，指定“”
        if (sinkPk == null) {
            sinkPk = "id";
        }
        if (sinkExtend == null) {
            sinkExtend = "";
        }


        try {
            //在open中声名了连接，在此处写出创建sql的语句
            StringBuilder createTableSql = new StringBuilder("create table if not exists ");
            createTableSql.append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");
            //列值
            String[] split = sinkColumns.split(",");
            for (int i = 0; i < split.length; i++) {
                //主键
                if (split[i].equals(sinkPk)) {
                    createTableSql.append(split[i]).append(" varchar primary key");
                } else {
                    createTableSql.append(split[i]).append(" VARCHAR");
                }
                //最后一列不添加  ","
                if (i < split.length - 1) {
                    createTableSql.append(",");
                }
            }
            createTableSql.append(")").append(sinkExtend);
            System.out.println(createTableSql);

            //创建连接对象
            PreparedStatement statement = connection.prepareStatement(createTableSql.toString());
            //执行   提交
            statement.execute();
            connection.commit();
        } catch (SQLException throwables) {
            //创建失败，抛异常提示
            throw new RuntimeException("创建表" + sinkTable + "失败！！！！！！！");
        }

    }

    /**
     * 对数据（cdc读取的业务数据，广播流中的配置表数据）进行数据字段（创建表）过滤
     *
     * @param data
     * @param sinkColumns
     */
    private void filterColumn(JSONObject data, String sinkColumns) {
        String[] cloumn = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(cloumn);

        //遍历data内的数据
        Set<Map.Entry<String, Object>> entrySet = data.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entrySet.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> next = iterator.next();
            if (!columnList.contains(next.getKey())) {
                iterator.remove();
            }
        }

    }

}
