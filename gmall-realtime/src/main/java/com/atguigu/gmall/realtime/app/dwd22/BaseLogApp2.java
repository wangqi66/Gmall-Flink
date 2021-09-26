package com.atguigu.gmall.realtime.app.dwd22;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 判断新老用户，并将数据过滤写入kafka
 *
 * @author wang
 * @create 2021-09-23 10:40
 */
public class BaseLogApp2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        //ck

        //获取ods_base_log的数据
        DataStreamSource<String> dataStreamSource = environment.addSource(MyKafkaUtil.getKafkaSource("ods_base_log", "dwd_log"));

        //将数据转换成jsonobj并清洗脏数据
        OutputTag<String> dirtyTag = new OutputTag<String>("dirty") {
        };
        SingleOutputStreamOperator<JSONObject> process = dataStreamSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                //将数据转换成jsonobj
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirtyTag, value);
                }
            }
        });

        process.getSideOutput(dirtyTag).print("dirty>>>>>>");

        //对数据根据mid进行分组
        KeyedStream<JSONObject, String> keyedStream = process.keyBy(js -> js.getJSONObject("common").getString("mid"));

        //对数据进行判断，获取数据的is_new
        SingleOutputStreamOperator<JSONObject> streamOperator = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {


            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                String isnew = value.getJSONObject("common").getString("is_new");
                if ("1".equals(isnew)) {
                    //对数据进行判断，当状态内有数据时，则对数据进行判断
                    String state = valueState.value();
                    if (state != null && state.length() > 0) {
                        //此时证明是老用户，进行修改数据
                        value.getJSONObject("common").put("is_new", "0");
                    } else {
                        //更新状态
                        valueState.update("0");
                    }
                }
                return value;
            }

        });


        //将数据区分为启动，页面，曝光
        OutputTag<String> startTag = new OutputTag<String>("startTag") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {
        };
        SingleOutputStreamOperator<String> mainDS = streamOperator.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                String start = value.getString("start");
                if (start != null && start.length() > 0) {
                    //启动页面
                    ctx.output(startTag, value.toJSONString());
                } else {
                    //是页面
                    out.collect(value.toJSONString());
                    //判断是否是曝光页面
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        //将数据拆分写入侧输出流、
                        String pageId = value.getJSONObject("page").getString("page_id");
                        for (int i = 0; i < displays.size(); i++) {
                            //将页面page_id写入
                            JSONObject displaysJSONObject = displays.getJSONObject(i);
                            displaysJSONObject.put("page_id", pageId);
                            ctx.output(displayTag, displaysJSONObject.toJSONString());
                        }
                    }

                }
            }
        });

        mainDS.print("页面数据流》》》》》");
        DataStream<String> startDS = mainDS.getSideOutput(startTag);
        startDS.print("start>>>>>>>");
        DataStream<String> dispalyDS = mainDS.getSideOutput(displayTag);
        dispalyDS.print("dispaly>>>>>>>");

        //将数据写入对应的kafka分区
        mainDS.addSink(MyKafkaUtil.getKafkaSink("dwd_page_log"));
        startDS.addSink(MyKafkaUtil.getKafkaSink("dwd_start_log"));
        dispalyDS.addSink(MyKafkaUtil.getKafkaSink("dwd_display_log"));

        //启动任务
        environment.execute();
    }
}
