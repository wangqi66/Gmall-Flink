package com.atguigu.gmall.realtime.app.Func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.utils.DimUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * @author wang
 * @create 2021-09-21 17:04
 */
//将数据导入到hbase
public class DimSinkFun extends RichSinkFunction<JSONObject> {

    private Connection connection;

    //初始化
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //每条数据的执行语句
    //value:{"database":"","tableName":"base_trademark","data":{"id":"","tm_name":"","logo_url":""},"before":{},"type":"insert","sinkTable":"dim_base_trademark"}
    private PreparedStatement statement = null;

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        try {
            //将数据通过sql语句的方式将数据写入到hbase
            String sql = upsertSql(value.getString("sinkTable"), value.getJSONObject("data"));
            System.out.println("sql执行语句"+sql);
            statement = connection.prepareStatement(sql);

            //判断当前如果是更新数据，则此时将redis内的数据进行删除[phoenix内数据发生变更（更改），redis内数据直接删除]

            if ("update".equals(value.getString("type"))){
                String redisKey="DIM:"+value.getString("sinkTable").toUpperCase()
                        +":"+value.getJSONObject("data").getString("id");
                DimUtil.deleteRedisData(redisKey);
            }


            statement.execute();
            connection.commit();
        } catch (SQLException throwables) {
            System.out.println("插入数据" + value + "失败！！！！！！！！");
        } finally {
            //连接关闭
            if (statement != null) {
                statement.close();
            }
        }

    }

    /**
     * 创建插入hbase的sql语句 upsert into 库+表 （字段） values（？？？？）
     *
     * @param sinkTable
     * @param data
     */
    private String upsertSql(String sinkTable, JSONObject data) {

        //获取字段列表
        Set<String> keySet = data.keySet();
        //获取值
        Collection<Object> values = data.values();
        //返回sql语句
        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" + StringUtils.join(keySet, ",")
                +")"+ " values('" + StringUtils.join(values, "','") + "')";
    }
}
