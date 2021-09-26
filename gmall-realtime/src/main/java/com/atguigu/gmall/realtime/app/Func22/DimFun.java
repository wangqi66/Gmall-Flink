package com.atguigu.gmall.realtime.app.Func22;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * 在这里进行匹配hbase的表及数据，上传到hbase的相关位置
 *
 * @author wang
 * @create 2021-09-23 19:58
 */
public class DimFun extends RichSinkFunction<JSONObject> {
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    PreparedStatement statement = null;

    //upsert into 库表（字段） values （值）
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {

        String sql = upsertHbae(value.getString("sink_table"), value.getJSONObject("data"));
        System.out.println("插入hbase的sql：" + sql);

        try {
            statement = connection.prepareStatement(sql);
            statement.execute();
            connection.commit();
        } catch (SQLException throwables) {
            System.out.println("插入数据失败");
        } finally {
            if (statement != null) {
                statement.close();
            }
        }

    }

    private String upsertHbae(String sink_table, JSONObject data) {
        Set<String> columns = data.keySet();
        Collection<Object> values = data.values();
        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sink_table + "('" +
                StringUtils.join(columns, "','") + ") " + "values(" +
                StringUtils.join(values, "','") + ")";
    }
}
