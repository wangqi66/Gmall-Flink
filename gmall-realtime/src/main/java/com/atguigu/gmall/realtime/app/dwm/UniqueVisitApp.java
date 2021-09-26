package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

/**
 * 用户访问uv计算（第一次打开页面则last_page_id为null，为用户访问）
 * 因为一个用户可能在一天内会多次进入，故需要在一天内进行过滤操作，涉及状态编程，要根据时间戳
 *
 * @author wang
 * @create 2021-09-22 13:05
 */

//数据流：web/app -》 nginx ——》springboot --》mysql——》flinkcdc -》kafka（ods）
//        -》flinkapp ->kafka(dwd)/phonix(dim) -》flinkapp-》kafka

//程序：mock-》mysql->flinkcdc ->kafka(ods) ->baserlogapp->kafka(log三张表)——》basedbLog
//     -》kafka（各主题的topic）/phonix（hbase的各个表）-》UniqueVisitApp -》kafka
public class UniqueVisitApp {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //设置checkpoint
//        environment.enableCheckpointing(5000L);
//        environment.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        environment.getCheckpointConfig().setCheckpointTimeout(10000L);
//        environment.setStateBackend(new FsStateBackend(""));


        //获取ikafka内的dwd_page_log 日志数据
        DataStreamSource<String> dataStreamSource = environment.addSource(MyKafkaUtil.getKafkaSource("dwd_page_log", "uv"));

        //将数据转换成jsonobj
        SingleOutputStreamOperator<JSONObject> map = dataStreamSource.map(jsonobj -> JSON.parseObject(jsonobj));

        //将数据进行分组
        KeyedStream<JSONObject, String> keyedStream = map.keyBy(jsonobj -> jsonobj.getJSONObject("common").getString("mid"));


        //对数据进行过滤，获取第一次的登录的数据（last_page_id==null）
        SingleOutputStreamOperator<JSONObject> filter = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            //封装状态数据
            private ValueState<String> valueState;

            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("value-state", String.class);
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.days(1)).build();
                valueStateDescriptor.enableTimeToLive(stateTtlConfig);
                valueState = getRuntimeContext().getState(valueStateDescriptor);
                sdf = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                //判断第一个页面，此时数据时keyby后的数据，故获取的是第一条数据
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                if (lastPageId == null) {
                    //获取valuestate状态中的数据
                    String visitDate = valueState.value();
                    String currDate = sdf.format(value.getLong("ts"));
                    //获取状态中的数据，如果有则进行更新
                    if (visitDate == null || !visitDate.equals(currDate)) {
                        valueState.update(currDate);
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        });

        //将数据写入到kafka
        filter.print("filterDS>>>>>>>>");

        filter.map(js -> JSON.toJSONString(js)).addSink(MyKafkaUtil.getKafkaSink("dwm_unique_visit"));

        //程序执行
        environment.execute();

    }
}
