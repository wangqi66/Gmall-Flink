package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.VisitorStats;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import com.atguigu.gmall.realtime.utils22.ClickUT;
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


import java.text.SimpleDateFormat;
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

        //??????????????????????????????javabean
        //???????????????
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
        //???????????????
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
        //?????????
        SingleOutputStreamOperator<VisitorStats> userJumpDetailDS = userJumpDetailSource.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String value) throws Exception {
                //1.??????????????????JSONObject
                JSONObject jsonObject = JSONObject.parseObject(value);
                //2.??????????????????
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


        //?????????union?????????
        DataStream<VisitorStats> unionDS = pageViewDS.union(uniqueVisitViewDS, userJumpDetailDS);

        //??????watermark
        SingleOutputStreamOperator<VisitorStats> unionAndWaterMarkDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        //????????????????????????
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
        //??????
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        //??????
        SingleOutputStreamOperator<VisitorStats> reduceDS = window.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());
                value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                return value1;
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {
                //????????????????????????
                VisitorStats visitorStats = input.iterator().next();

                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                String stt = sdf.format(window.getStart());
                String edt = sdf.format(window.getEnd());
                visitorStats.setStt(stt);
                visitorStats.setEdt(edt);
                //????????????
                out.collect(visitorStats);

            }
        });


        reduceDS.print("reduce>>>>");


        //???????????????clickhouse
        reduceDS.addSink(ClickHouseUtil.getSink("insert into visitor_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        //????????????
        environment.execute();

    }
}
