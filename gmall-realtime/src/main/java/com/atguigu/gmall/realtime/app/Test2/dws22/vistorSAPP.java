package com.atguigu.gmall.realtime.app.Test2.dws22;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.VisitorStats;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
 * @create 2021-09-28 15:36
 */
public class vistorSAPP {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        String groupId = "visitor_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";

        DataStreamSource<String> pgDS = environment.addSource(MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId));
        DataStreamSource<String> uvDS = environment.addSource(MyKafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> ujDS = environment.addSource(MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId));

        //装换javabean
        SingleOutputStreamOperator<VisitorStats> uvmapDS = uvDS.map(res -> {
            JSONObject jsonObject = JSON.parseObject(res);
            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L,
                    0L,
                    0L,
                    0L,
                    0l,
                    jsonObject.getLong("ts"));
        });
        SingleOutputStreamOperator<VisitorStats> ujmapDS = ujDS.map(res -> {
            JSONObject jsonObject = JSON.parseObject(res);
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
                    0l,
                    jsonObject.getLong("ts"));
        });

        SingleOutputStreamOperator<VisitorStats> pgmapDS = pgDS.map(res -> {
            JSONObject jsonObject = JSON.parseObject(res);
            JSONObject common = jsonObject.getJSONObject("common");
            JSONObject page = jsonObject.getJSONObject("page");
            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    1L,
                    page.getString("last_page_id") == null ? 1L : 0L,
                    0L,
                    page.getLong("during_time"),
                    jsonObject.getLong("ts"));
        });

        //将流union
        DataStream<VisitorStats> unionDS = pgmapDS.union(uvmapDS, ujmapDS);

        //获取watermark
        SingleOutputStreamOperator<VisitorStats> unionWithWmDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(12))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        //分组
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = unionWithWmDS.keyBy(key -> Tuple4.of(key.getAr(), key.getCh(), key.getIs_new(), key.getVc()));

        //开窗
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        //聚合
        SingleOutputStreamOperator<VisitorStats> reduceDS = window.reduce(new ReduceFunction<VisitorStats>() {
            //数据聚合处理
            @Override
            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                return value1;
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            //补全字段
            @Override
            public void apply(Tuple4<String, String, String, String> key, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {
                long start = window.getStart();
                long end = window.getEnd();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String stt = sdf.format(new Date(start));
                String edt = sdf.format(new Date(end));
                VisitorStats value = input.iterator().next();
                value.setStt(stt);
                value.setEdt(edt);
                out.collect(value);
            }
        });

        reduceDS.print();




        //程序执行
        environment.execute();

    }
}
