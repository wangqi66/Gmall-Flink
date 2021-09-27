package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.VisitorStats;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


import java.time.Duration;
import java.util.Date;

/**
 * @author wang
 * @create 2021-09-27 10:55
 */
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        //checkpoint

        String groupId = "visitor_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";

        DataStreamSource<String> pageViewSource = environment.addSource(MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId));
        DataStreamSource<String> uniqueVisitSource = environment.addSource(MyKafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> userJumpDetailSource = environment.addSource(MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId));

        //将数据封装成相对应的javabean
        //页面访问数
        SingleOutputStreamOperator<VisitorStats> pageViewDS = pageViewSource.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                Long sv = 0L;
                if (jsonObject.getJSONObject("page").getString("last_page_id") == null) {
                    sv = 1L;
                }
                return new VisitorStats(
                        "", "",
                        jsonObject.getJSONObject("common").getString("vc"),
                        jsonObject.getJSONObject("common").getString("ch"),
                        jsonObject.getJSONObject("common").getString("ar"),
                        jsonObject.getJSONObject("common").getString("is_new"),
                        0L,
                        1L,
                        sv,
                        0L,
                        jsonObject.getJSONObject("page").getLong("during_time"),
                        jsonObject.getLong("ts")
                );
            }
        });
        //用户访问数
        SingleOutputStreamOperator<VisitorStats> uniqueVisitViewDS = uniqueVisitSource.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                return new VisitorStats(
                        "", "",
                        jsonObject.getJSONObject("common").getString("vc"),
                        jsonObject.getJSONObject("common").getString("ch"),
                        jsonObject.getJSONObject("common").getString("ar"),
                        jsonObject.getJSONObject("common").getString("is_new"),
                        1L,
                        0L,
                        0L,
                        0L,
                        0L,
                        jsonObject.getLong("ts")
                );
            }
        });
        //跳出数
        SingleOutputStreamOperator<VisitorStats> userJumpDetailDS = userJumpDetailSource.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String value) throws Exception {
                //1.将数据转换为JSONObject
                JSONObject jsonObject = JSONObject.parseObject(value);
                //2.提取公共字段
                JSONObject common = jsonObject.getJSONObject("common");
                return new VisitorStats("", "",
                        common.getString("vc"),
                        common.getString("ch"),
                        common.getString("ar"),
                        common.getString("is_new"),
                        0L,
                        0L,
                        0L,
                        1L,
                        0L,
                        jsonObject.getLong("ts"));
            }
        });


        //将数据union到一起
        DataStream<VisitorStats> unionDS = pageViewDS.union(uniqueVisitViewDS, userJumpDetailDS);

        //设置watermark
        SingleOutputStreamOperator<VisitorStats> unionAndWaterMarkDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        //分组。开窗。聚合
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = unionAndWaterMarkDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                String ar = value.getAr();
                String ch = value.getCh();
                String is_new = value.getIs_new();
                String vc = value.getVc();
                return Tuple4.of(ar, ch, is_new, vc);
            }
        });
        //开窗
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        //聚合
        SingleOutputStreamOperator<VisitorStats> reduceDS = window.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());
                return value1;
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {
                //取出聚合后的数据
                VisitorStats visitorStats = input.iterator().next();

                String stt = DateTimeUtil.toYMDhms(new Date(window.getStart()));
                String edt = DateTimeUtil.toYMDhms(new Date(window.getEnd()));

                visitorStats.setStt(stt);
                visitorStats.setEdt(edt);

                //输出数据
                out.collect(visitorStats);

            }
        });


        reduceDS.print("reduce>>>>");


        //将数据写入clickhouse




        //启动任务
        environment.execute();

    }
}
