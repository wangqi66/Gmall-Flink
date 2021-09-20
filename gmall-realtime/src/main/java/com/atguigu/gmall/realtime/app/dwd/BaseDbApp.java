package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.gmall.realtime.app.ods.FlinkCDCApp;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author wang
 * @create 2021-09-19 22:47
 */
public class BaseDbApp {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //checkPoint
//        environment.enableCheckpointing(5000L);
//        environment.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        environment.getCheckpointConfig().setCheckpointTimeout(10000L);
//        environment.setStateBackend(new FsStateBackend(""));

        //kafka获取ods_base_db的数据
        String db = "ods_base_db";
        DataStreamSource<String> dataStreamSource = environment.addSource(MyKafkaUtil.getKafkaSource(db, "dbGroup"));

        //将数据转换成jsonobject（主流）并进行清洗数据(删除数据)
        SingleOutputStreamOperator<JSONObject> jsonDstream = dataStreamSource.map(jsonobj -> JSONObject.parseObject(jsonobj)).filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return !"delete".equals(value.getString("type"));
            }
        });

        //通过flinkCDC来读取配置信息表，并封装为广播流，实时形成动态并传递给各并行度
        DebeziumSourceFunction<String> tableProcessSourceFun = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-realtime")
                .tableList("gmall-realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new FlinkCDCApp.MySerializer())
                .build();

        //分装成广播流
        DataStreamSource<String> tableProcessDS = environment.addSource(tableProcessSourceFun);
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);

        tableProcessDS.broadcast();


    }
}
