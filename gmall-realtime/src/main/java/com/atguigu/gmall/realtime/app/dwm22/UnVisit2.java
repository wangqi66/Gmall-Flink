package com.atguigu.gmall.realtime.app.dwm22;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

/**
 * uv获取用户活跃访问（单日）
 *
 * @author wang
 * @create 2021-09-24 15:28
 */
public class UnVisit2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        DataStreamSource<String> dataStreamSource = environment.addSource(MyKafkaUtil.getKafkaSource("dwd_page_log", "aa"));

        //装换数据格式并keyby分组
        KeyedStream<JSONObject, String> keyedStream = dataStreamSource.map(js -> JSON.parseObject(js))
                .keyBy(js -> js.getJSONObject("common").getString("mid"));


        //对数据进行过滤
        SingleOutputStreamOperator<JSONObject> streamOperator = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> valueState;
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<String>("value", String.class);
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.days(1)).build();
                valueStateDescriptor.enableTimeToLive(stateTtlConfig);

                valueState = getRuntimeContext().getState(valueStateDescriptor);
                sdf = new SimpleDateFormat("yyyy-MM-dd");
            }

            //uv获取用户活跃访问
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                String state = valueState.value();
                if (lastPageId == null) {
                    //符合条件的数据,获取当前时间，获取valuestate
                    Long ts = value.getLong("ts");
                    String currdate = sdf.format(ts);
                    //当装态中没有数据，表明是首次访问，当状态中并没有今天的数据，证明也是今天的数据
                    if (state == null || !state.equals(currdate)) {
                        valueState.update(currdate);
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        });


        streamOperator.print("过滤出的数据");

        //将数据写入到kafka中
        streamOperator.map(js -> js.toJSONString(js)).addSink(MyKafkaUtil.getKafkaSink("dwm_uservisit_log"));

        environment.execute();


    }
}
