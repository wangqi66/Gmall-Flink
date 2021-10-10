package com.atguigu.gmall.realtime.app.Test2.Func22;


import com.alibaba.fastjson.JSON;
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

import java.sql.*;
import java.util.*;

/**
 * @author wang
 * @create 2021-09-23 18:43
 */
public class tableProcessFun extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private Connection connection;
    private PreparedStatement statement = null;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    //侧输出流
    private OutputTag<JSONObject> hbaseTag;
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_SERVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_DRIVER);
    }

    public tableProcessFun() {
    }

    public tableProcessFun(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    public tableProcessFun(MapStateDescriptor<String, TableProcess> mapStateDescriptor, OutputTag<JSONObject> hbaseTag) {
        this.mapStateDescriptor = mapStateDescriptor;
        this.hbaseTag = hbaseTag;
    }

    //主流
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

        //获取广播出来的流状态数据
        String key = value.getString("tableName") + "_" + "type";
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        TableProcess tableProcess = broadcastState.get(key);

        //在主流中过滤需要的字段
        JSONObject data = value.getJSONObject("data");
        String sinkColumns = tableProcess.getSinkColumns();
        if (tableProcess != null) {
            filterDate(data, sinkColumns);

            value.put("sinkTable",tableProcess.getSinkTable());
            //分流  hbase 入侧输出流，kafka入主流
            if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())){
                ctx.output(hbaseTag,value);
            }else if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())){
                out.collect(value);
            }

        } else {
            System.out.println(key + "不存在！！！");
        }


    }

    /**
     * ]
     * 过滤需要的数据
     *
     * @param data
     * @param sinkColumns
     */
    private void filterDate(JSONObject data, String sinkColumns) {
        String[] columns = sinkColumns.split(",");
        List<String> list = Arrays.asList(columns);

        Set<Map.Entry<String, Object>> entries = data.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        while (iterator.hasNext()){
            if (!list.contains(iterator.next())){
                iterator.remove();
            }
        }


    }

    //广播流
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

        String data = JSON.parseObject(value).getString("data");

        TableProcess tableProcess = JSON.parseObject(data, TableProcess.class);

        //广播流中进行创建phoenix的表
        String type = tableProcess.getOperateType();
        if ("insert".equals(type) && TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            createTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend());

        }

        //将广播流广播出去
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        broadcastState.put(tableProcess.getSourceTable() + "_" + tableProcess.getOperateType(), tableProcess);


    }

    private void createTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        //对字段预处理
        if (sinkPk == null) {
            sinkPk = "id";
        }
        if (sinkExtend == null) {
            sinkExtend = "";
        }

        StringBuilder sql = new StringBuilder("create table if not exists ");
        sql.append(GmallConfig.HBASE_SCHEMA).append(".").append(sinkTable).append("(");
        String[] columnList = sinkColumns.split(",");


        for (int i = 0; i < columnList.length; i++) {
            if (columnList[i].equals(sinkPk)) {
                sql.append(columnList[i]).append(" varchar primary key");
            } else {
                sql.append(columnList[i]).append(" varchar");
            }

            if (i < columnList.length - 1) {
                sql.append(",");
            }
        }
        sql.append(")").append(sinkExtend);

        try {
            statement = connection.prepareStatement(sql.toString());

            statement.execute();

            connection.commit();

        } catch (SQLException throwables) {
            throw new RuntimeException("创建表" + sinkTable + "失败！！！");
        } finally {
            try {
                statement.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }

    }
}
