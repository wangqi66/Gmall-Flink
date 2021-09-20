package com.atguigu.gmall.realtime.app.ods;

import com.alibaba.fastjson.JSONObject;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;

import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;


import java.util.List;


/**
 * 从mysql中监控并读取数据，保存在kafka中ods_base_db主题
 *
 * @author wang
 * @create 2021-09-18 9:02
 */
public class FlinkCDCApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //创建flink-mysql-cdc的连接
        DebeziumSourceFunction<String> mysqlSource = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-flink")
                .startupOptions(StartupOptions.latest())
                .deserializer(new MySerializer()).build();


        //将连接放入flink的source 读取数据
        DataStreamSource<String> streamSource = environment.addSource(mysqlSource);

        //获取kafka的sink
        streamSource.addSink(MyKafkaUtil.getKafkaSink("ods_base_db"));
        //程序执行
        environment.execute();

    }


    public static class MySerializer implements DebeziumDeserializationSchema<String> {

        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
            //创建一个json对象用来返回需要的数据格式、
            //数据库名，表名，数据，before，type
            JSONObject result = new JSONObject();
            String topic =  sourceRecord.topic();
            String[] split = topic.split("\\.");
            //库名和表名
            String dataBase = split[1];
            String tableName = split[2];


            Struct value = (Struct) sourceRecord.value();
            //这个是数据体
            JSONObject data = new JSONObject();
            Struct after = value.getStruct("after");
            if (after!=null){
                //当数据体不为空时，将数据进行保存存储到一个结果对象中
                Schema schema = after.schema();//结果对象
                //获取key的值列表
                List<Field> fieldList = schema.fields();
                for (int i = 0; i < fieldList.size(); i++) {
                    Field field = fieldList.get(i);
                    Object dataValue = after.get(field);
                    //放入对象
                    data.put(field.name(),dataValue);
                }
            }

            //这个是数据体
            Struct before = value.getStruct("before");
            //创建对象存储before对象数据
            JSONObject beforeData = new JSONObject();
            if (before!=null){
                //当数据体不为空时，将数据进行保存存储到一个结果对象中
                Schema schema = before.schema();//结果对象
                //获取key的值列表
                List<Field> fieldList = schema.fields();
                for (int i = 0; i < fieldList.size(); i++) {
                    Field field = fieldList.get(i);
                    Object dataValue = before.get(field);
                    //放入对象
                    beforeData.put(field.name(),dataValue);
                }
            }

            //获取type类型 CREATE UPDATE DELETE
            Envelope.Operation operation = Envelope.operationFor(sourceRecord);
            //将create进行更改为insert   转小写
            String lowerCase = operation.toString().toLowerCase();
            if (lowerCase.equals("create")){
                lowerCase="insert";
            }

            //将数据真正整合到对象
            result.put("dataBase",dataBase);
            result.put("tableName",tableName);
            result.put("data",data);
            result.put("beforeData",beforeData);
            result.put("Type",lowerCase);

            //收集并返回需要的对象格式
            collector.collect(result.toJSONString());
        }

        @Override
        public TypeInformation<java.lang.String> getProducedType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }


    }
}
