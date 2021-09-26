package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Collector;

/**
 *
 * * dwd层数据，存放于 kafka，(从kafka的ods_base_db中读取用户行为数据)
 * 对数据进行过滤，分为页面数据（主流），启动数据（侧输出流），曝光数据（侧输出流）
 * 将数据进行过滤出脏数据，存放到测输出流
 *
 *
 * 识别新老用户，当数据中的is_new为1时要进行判断，
 * 因为每当卸载重装，对于前端埋点都为1，
 * 此时通过状态编程来判断该用户是否为新用户
 * @author wang
 * @create 2021-09-18 23:15
 */


//数据流  web/app -》 ngix  -》 springboot（日志数据） ---》 kafka（ods） -》BaseLogApp -》kafka{dwd层}

//程序 mock -》nginx -》logger -》kafka（ods） -》baselog （消费并判断）-》kafka（dwd）
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);//并行度要和kafka的分区数相同，，【watermark，多了会导致watermark一直为integer的最小值】

        //设置checkpoint
//        environment.enableCheckpointing(5000L);
//        environment.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        environment.getCheckpointConfig().setCheckpointTimeout(10000L);
//        environment.setStateBackend(new FsStateBackend(""));


        //从kafka(Kafka ods_base_log)获取数据

        String topic = "ods_base_log";//主题
        String groupId = "gmall0426";//消费者组
        DataStreamSource<String> streamSource = environment.addSource(MyKafkaUtil.getKafkaSource(topic, groupId));

        //对主数据进行封装成jsonobject,此时还要对数据进行过滤，清洗数据，将脏数据（json解析出现错误的数据）写入测输出流
        //定义一个测输出流对象
        OutputTag<String> outputTag = new OutputTag<String>("dirtyDate") {
        };
        //因为map，flatmap只能进行解析封装，无法对脏数据进行保留，故使用process来对数据进行处理
        SingleOutputStreamOperator<JSONObject> processDstream = streamSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    //解析成功
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    //解析失败，写入测输出流
                    context.output(outputTag, s);
                }
            }
        });

        //打印脏数据
        processDstream.getSideOutput(new OutputTag<String>("dirtyDate") {
        }).print("dirtyDate>>>>>>");

        //将数据进行分组
        KeyedStream<JSONObject, String> keyedStream = processDstream.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //判断此时的is_new，如果是“1”就进行考察是否新用户(状态编程存储数据)
        SingleOutputStreamOperator<JSONObject> mapDstream = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {

            private ValueState<String> valueState;

            @Override
            public void open(Configuration conf) throws Exception {
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("new_state", String.class);
                //ttl设置过期时间，来将前一天的额数据进行清空，因为本需求是根据每天的数据来进行获取的，故前一天的数据已不需要
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.hours(24))
                        //设置个更新模式，当又发生变动后，就进行将改数据继续保留24小时
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();

                stateDescriptor.enableTimeToLive(stateTtlConfig);


                valueState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                //当is_new 为1才去判断是否新老用户
                if ("1".equals(value.getJSONObject("common").getString("is_new"))) {
                    //获取状态
                    String state = valueState.value();
                    if (state != null) {
                        //老用户，更改数据
                        value.getJSONObject("common").put("is_new", "0");

                    } else {
                        //因为valuestate 是没有数据，故是新用户，随意设置数据即可
                        valueState.update("0");
                    }
                }
                return value;
            }
        });


        //分流，将数据进行拆分为启动数据，页面数据，曝光数据
        OutputTag<JSONObject> startTag = new OutputTag<JSONObject>("start") {
        };
        OutputTag<JSONObject> displaysTag = new OutputTag<JSONObject>("displays") {
        };

        SingleOutputStreamOperator<JSONObject> process = mapDstream.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<JSONObject> collector) throws Exception {
                //获取启动页面
                String start = jsonObject.getString("start");
                //如果启动页面存在
                if (start != null && start.length() > 0) {
                    context.output(startTag, jsonObject);
                } else {
                    //不是启动页面，故是页面数据
                    collector.collect(jsonObject);

                    //判断是否为曝光页面
                    JSONArray displays = jsonObject.getJSONArray("displays");

                    if (displays != null && displays.size() > 0) {
                        //获取page_id
                        String page_id = jsonObject.getJSONObject("page").getString("page_id");
                        //是曝光页面，此时写入page_id并写入测输出流
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject displaysJSONObject = displays.getJSONObject(i);
                            displaysJSONObject.put("page_id", page_id);

                            context.output(displaysTag, displaysJSONObject);
                        }
                    }
                }
            }
        });

        //将输出流打印出来，并写入对应的kafka主题
        DataStream<JSONObject> startDS = process.getSideOutput(startTag);
        startDS.print("start>>>>>>>");
        DataStream<JSONObject> displayDS = process.getSideOutput(displaysTag);
        displayDS.print("displaysTag>>>>>>>");
        process.print("页面数据》》》》》》》");

        String startTopic="dwd_start_log";
        String pageTopic="dwd_page_log";
        String displayTopic="dwd_display_log";

        //写入kafka
        startDS.map(JSObj->JSObj.toJSONString()).addSink(MyKafkaUtil.getKafkaSink(startTopic));
        process.map(JSObj->JSObj.toJSONString()).addSink(MyKafkaUtil.getKafkaSink(pageTopic));
        displayDS.map(JSObj->JSObj.toJSONString()).addSink(MyKafkaUtil.getKafkaSink(displayTopic));

        //执行程序
        environment.execute();

    }
}
