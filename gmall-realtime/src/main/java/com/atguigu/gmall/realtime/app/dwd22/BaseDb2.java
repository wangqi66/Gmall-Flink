package com.atguigu.gmall.realtime.app.dwd22;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.gmall.realtime.app.Func.DimSinkFun;
import com.atguigu.gmall.realtime.app.Func.MyStringDeserializationSchema;
import com.atguigu.gmall.realtime.app.Func22.tableProcessFun;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.utils22.KU;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;


/**
 * 业务数据的处理
 * @author wang
 * @create 2021-09-23 18:26
 */
public class BaseDb2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //获取业务数据
        DataStreamSource<String> dataStreamSource = environment.addSource(MyKafkaUtil.getKafkaSource("ods_base_db", "db_consumer"));

        //将数据进行过滤处理，去除类型为delete的数据，转换成jsonobject
        SingleOutputStreamOperator<JSONObject> jsonobjDS = dataStreamSource.map(obj -> JSON.parseObject(obj)).filter(new RichFilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return "delete".equals(value.getString("type"));
            }
        });

        //获取flinkcdc监控的配置文件流
        DebeziumSourceFunction<String> cdcdate = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-realtime")
                .tableList("gmall-realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyStringDeserializationSchema())
                .build();

        //将数据进行装换成广播流
        DataStreamSource<String> cdcDS = environment.addSource(cdcdate);
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastDS = cdcDS.broadcast(mapStateDescriptor);



        //流的合并,并对两条流进行处理
        BroadcastConnectedStream<JSONObject, String> broadcastConnectedStream = jsonobjDS.connect(broadcastDS);
        OutputTag<JSONObject> hbaseTage = new OutputTag<JSONObject>("habsTag") {
        };
        SingleOutputStreamOperator<JSONObject> mainDS = broadcastConnectedStream.process(new tableProcessFun(mapStateDescriptor, hbaseTage));


        //对流进行分流写入

        DataStream<JSONObject> hbaseDS = mainDS.getSideOutput(hbaseTage);

        mainDS.print("kafka》》》》》》》");
        hbaseDS.print("hbase>>>>>>>>>>>");


        //将数据写入相应的sink
        //hbase
        hbaseDS.addSink(new DimSinkFun());
        //kafka
        mainDS.addSink(KU.getKafkaConsumerSink(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                return new ProducerRecord<>(element.getString("sinkTable"),
                        element.getString("data").getBytes());
            }
        }));


        //程序启动执行
        environment.execute();



    }
}
