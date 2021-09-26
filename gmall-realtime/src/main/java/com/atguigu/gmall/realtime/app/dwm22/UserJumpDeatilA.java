package com.atguigu.gmall.realtime.app.dwm22;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 页面跳出
 * @author wang
 * @create 2021-09-25 10:24
 */
public class UserJumpDeatilA {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //获取数据源
        DataStreamSource<String> kafkaDS = environment.addSource(MyKafkaUtil.getKafkaSource("dwd_page_log", "aa"));

        //将数据源转换成jsonobj
        SingleOutputStreamOperator<JSONObject> mapDS = kafkaDS.map(JSON::parseObject).assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }));

        //对数据进行分组
        KeyedStream<JSONObject, String> keyedStream = mapDS.keyBy(js -> js.getJSONObject("common").getString("mid"));

        //定义模式序列

        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") == null;
            }
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") == null;
            }
        }).within(Time.seconds(10));

        //将模式作用于流
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        //提取超时时间和符合条件的事件
        OutputTag<String> outputTag = new OutputTag<String>("timeout") {
        };
        SingleOutputStreamOperator<String> streamOperator = patternStream.select(outputTag, new PatternTimeoutFunction<JSONObject, String>() {
            @Override
            public String timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                return map.get("start").get(0).toJSONString();
            }
        }, new PatternSelectFunction<JSONObject, String>() {
            @Override
            public String select(Map<String, List<JSONObject>> map) throws Exception {
                return map.get("start").get(0).toJSONString();
            }
        });

        //将超时事件和符合条件的事件来union
        DataStream<String> timeoutDS = streamOperator.getSideOutput(outputTag);
        DataStream<String> dataStream = streamOperator.union(timeoutDS);

        //打印
        dataStream.print("join");

        //将数据写入到kakfa
        dataStream.addSink(MyKafkaUtil.getKafkaSink("dwm_user_jump_detail"));

        //程序执行
        environment.execute();

    }
}
