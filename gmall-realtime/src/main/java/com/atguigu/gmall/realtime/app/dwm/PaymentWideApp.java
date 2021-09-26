package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.bean.PaymentInfo;
import com.atguigu.gmall.realtime.bean.PaymentWide;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import io.debezium.data.Json;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * @author wang
 * @create 2021-09-26 16:04
 */
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        //checkpoint
        //>......
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";

        //获取订单宽表，和支付流数据表
        DataStreamSource<String> orderWideStreamSource = environment.addSource(MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId));
        DataStreamSource<String> paymentStreamSource = environment.addSource(MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId));

        //转换javabean，并获取watermark
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideStreamSource.map(js -> JSON.parseObject(js, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                            @Override
                            public long extractTimestamp(OrderWide element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(element.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    throw new RuntimeException("时间格式错误！！！");
                                }
                            }
                        }));

        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentStreamSource.map(js -> JSON.parseObject(js, PaymentInfo.class)).assignTimestampsAndWatermarks(WatermarkStrategy
                .<PaymentInfo>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                    @Override
                    public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        try {
                            return sdf.parse(element.getCreate_time()).getTime();
                        } catch (ParseException e) {
                            throw new RuntimeException("时间格式错误！！！");
                        }
                    }
                }));


        //keyby并双流join
        SingleOutputStreamOperator<PaymentWide> joinDS = paymentInfoDS.keyBy(key -> key.getOrder_id())
                .intervalJoin(orderWideDS.keyBy(key -> key.getOrder_id()))
                .between(Time.minutes(-15), Time.minutes(2))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo left, OrderWide right, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(left, right));
                    }
                });

        joinDS.print("支付》》》》");


        //将数据写入kafka
        joinDS.map(JSON::toJSONString).addSink(MyKafkaUtil.getKafkaSink(paymentWideSinkTopic));


        //启动任务
        environment.execute();


    }

}
