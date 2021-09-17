import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import scala.annotation.meta.field;

import java.util.List;


/**
 * @author wang
 * @create 2021-09-16 20:47
 */
public class flinkCDCDstrem_custom {
    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DebeziumSourceFunction<String> mysqlSource = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-flink")
                .tableList("gmall-flink.base_trademark") //可选配置项,如果不指定该参数,则会读取上一个配置下的所有表的数据，注意：指定的时候需要使用"db.table"的方式
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDeserializationSchema())
                .build();

        //4.使用CDC Source从MySQL读取数据
        DataStreamSource<String> mysqlDS = env.addSource(mysqlSource);

        //5.打印数据
        mysqlDS.print();

        //6.执行任务
        env.execute();

    }

    //自定义反序列化器
    public static class MyDeserializationSchema implements DebeziumDeserializationSchema<String> {
        //结果值进行返回，返回成一个json，因为这是一个source，要读多个数据库和数据库表，故要对返回字段进行设定
        //此处StringDebeziumDeserializationSchema（）原本返回的字段较为复合，不易解析，故自定义一个反序列化器
        //{
        // "database"："",  "tablename":"",
        //数据： "data":{"";""  ,"" : "" ....}
        //数据： "before":{"";""  ,"" : "" ....}
        // "type":"update"
        //  "ts" : ""
        // }
        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
            JSONObject result = new JSONObject();

            //获取数据库名和表名
            String topic = sourceRecord.topic();
            String[] split = topic.split("\\.");

            String dataBase = split[1];
            String tableName = split[2];


            //获取数据
            Struct value = (Struct)sourceRecord.value();

            //获取after数据
            Struct after = value.getStruct("after");
            //创建一个json对象用来存储after的数据
            JSONObject afterData = new JSONObject();
            if (after!=null){
                //当插入时after为null，故需要判断一下
                Schema schema = after.schema();

                //获取after中数据名（类key）
                List<Field> fieldList = schema.fields();

                for (int i = 0; i < fieldList.size(); i++) {
                    //获取对应的值
                    Field feild = fieldList.get(i);
                    Object fieldValue = after.get(feild);
                    //将数据存储到一个json对象中
                    afterData.put(feild.name(),fieldValue);

                }
            }

            //获取before数据
            Struct before = value.getStruct("before");
            //创建一个json对象用来存储before的数据
            JSONObject beforeData = new JSONObject();

            if (before!=null){
                //当插入时before为null，故需要判断一下
                Schema schema = before.schema();

                //获取before中数据名（类key）
                List<Field> fieldList = schema.fields();

                for (int i = 0; i < fieldList.size(); i++) {
                    //获取对应的值
                    Field feild = fieldList.get(i);
                    Object fieldValue = before.get(feild);

                    //将数据存储到一个json对象中
                    beforeData.put(feild.name(),fieldValue);

                }
            }



            //获取type类型 CREATE UPDATE DELETE
            Envelope.Operation operation = Envelope.operationFor(sourceRecord);
            //将create进行更改为insert   转小写
            String lowerCase = operation.toString().toLowerCase();
            if ("create".equals(lowerCase)){
                lowerCase="insert";
            }


            //将数据封装到resultjson对象中进行返回

            result.put("dataBase",dataBase);
            result.put("tableName",tableName);
            result.put("data",afterData);
            result.put("before",beforeData);
            result.put("type",lowerCase);
            result.put("ts",System.currentTimeMillis());

            collector.collect(result.toJSONString());
        }

        //返回值类型的方法
        @Override
        public TypeInformation<String> getProducedType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }

}
