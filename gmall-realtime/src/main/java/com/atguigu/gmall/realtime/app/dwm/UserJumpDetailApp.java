package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
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
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 页面单跳转化率
 * （当用户登录后就跳出视为一次单跳）
 * 在需求中，可以根据会话 使用会话窗口
 * 也可以认定上一跳为null，连续两次null则为单跳，
 * 其中还要考虑超时时间
 *
 * @author wang
 * @create 2021-09-23 20:55
 */
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //读取kafak的 dwd_page_log 主题数据
        DataStreamSource<String> dataStreamSource = environment.addSource(MyKafkaUtil.getKafkaSource("dwd_page_log", "page_dantiao"));

        //转换为hjsonobject并分组
        SingleOutputStreamOperator<JSONObject> jsonobjDS = dataStreamSource.map(js -> JSONObject.parseObject(js));
        //定义watermark
        KeyedStream<JSONObject, String> keyedStream = jsonobjDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                })).keyBy(kb -> kb.getJSONObject("common").getString("mid"));

        //定义模式序列  //匹配上一跳为null
        Pattern<JSONObject, JSONObject> jsonObjectPattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") == null;
            }
        }).next("next_page")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getJSONObject("page").getString("last_page_id") == null;
                    }
                }).within(Time.seconds(10));


        //将模式序列作用到流
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, jsonObjectPattern);

        //提取事件（包含匹配上的及超时事件）
        //超时时间放入侧输出流
        OutputTag<JSONObject> timeOutTag = new OutputTag<JSONObject>("timeout") {
        };

        SingleOutputStreamOperator<JSONObject> outputStreamOperator = patternStream.select(timeOutTag, new PatternTimeoutFunction<JSONObject, JSONObject>() {
            //超时
            @Override
            public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                return map.get("start").get(0);
            }
        }, new PatternSelectFunction<JSONObject, JSONObject>() {
            //正常匹配
            @Override
            public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                return map.get("start").get(0);
            }
        });


        //获取侧输出流数据与主流数据进行union
        //超时测输出流
        DataStream<JSONObject> sideOutputDS = outputStreamOperator.getSideOutput(timeOutTag);

        DataStream<JSONObject> unionDS = outputStreamOperator.union(sideOutputDS);

        //将数据写入kafka
        unionDS.print("数据》》》》");
        unionDS.map(js->js.toJSONString()).addSink(MyKafkaUtil.getKafkaSink("dwm_user_jump_detail"));

        //启动任务
        environment.execute();



    }
}
