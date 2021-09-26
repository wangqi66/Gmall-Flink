package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.Func.DimAsyncFunction;
import com.atguigu.gmall.realtime.bean.OrderDetail;
import com.atguigu.gmall.realtime.bean.OrderInfo;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * 订单宽表
 * 双流join
 *
 * @author wang
 * @create 2021-09-24 16:29
 */
public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group0426";

        //获取数据源
        DataStreamSource<String> orderinfoData = environment.addSource(MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, "orderinfo"));
        DataStreamSource<String> orderDetailData = environment.addSource(MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, "orderDetail"));

        //将数据封装成javabean
        //对数据进行分组
        KeyedStream<OrderInfo, Long> orderinfoDS = orderinfoData.map(new MapFunction<String, OrderInfo>() {
            @Override
            public OrderInfo map(String value) throws Exception {
                OrderInfo orderInfo = JSON.parseObject(value, OrderInfo.class);
                String create_time = orderInfo.getCreate_time();

                String[] date = create_time.split(" ");

                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                long ts = sdf.parse(create_time).getTime();

                orderInfo.setCreate_date(date[0]);
                orderInfo.setCreate_hour(date[1].split(":")[0]);
                orderInfo.setCreate_ts(ts);

                return orderInfo;
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
            @Override
            public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                return element.getCreate_ts();
            }
        })).keyBy(js -> js.getId());


        KeyedStream<OrderDetail, Long> orderDetailDS = orderDetailData.map(new MapFunction<String, OrderDetail>() {
            @Override
            public OrderDetail map(String value) throws Exception {
                OrderDetail Orderdetail = JSON.parseObject(value, OrderDetail.class);
                String create_time = Orderdetail.getCreate_time();

                String[] date = create_time.split(" ");

                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                long ts = sdf.parse(create_time).getTime();

                Orderdetail.setCreate_ts(ts);

                return Orderdetail;
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
            @Override
            public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                return element.getCreate_ts();
            }
        })).keyBy(js -> js.getOrder_id());


        //双流join，使得订单表和订单详情表join起来

        SingleOutputStreamOperator<OrderWide> joinDS = orderinfoDS.intervalJoin(orderDetailDS)
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left, OrderDetail right, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left, right));
                    }
                });
        joinDS.print("双流join>>");

        //关联维度信息（本质上是将join的orderwide数据中为null的数据进行填补，来获取一张宽表）

        //关联用户维度信息
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(joinDS,
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {

                    //将id传入，以获取相对应的phoenix内的数据信息用来补全维度信息
                    @Override
                    public String getId(OrderWide orderWide) {
                        Long user_id = orderWide.getUser_id();
                        return user_id.toString();
                    }

                    //将获取到的维度信息进行补充到主流
                    @Override
                    public void Join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        //获取性别
                        String gender = dimInfo.getString("GENDER");
                        orderWide.setUser_gender(gender);
                        //获取年龄，因为内部是生日，故需要计算
                        String birthday = dimInfo.getString("BIRTHDAY");
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

                        long time = sdf.parse(birthday).getTime();
                        //获取当前时间
                        long timeMillis = System.currentTimeMillis();
                        Long ageLong = (timeMillis - time) / (1000L * 60 * 60 * 24 * 365);
                        orderWide.setUser_age(ageLong.intValue());
                    }
                },
                60,
                TimeUnit.SECONDS);

        //关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideWithUserDS, new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
            @Override
            public String getId(OrderWide orderWide) {
                return orderWide.getProvince_id().toString();
            }

            @Override
            public void Join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                String province_name = dimInfo.getString("NAME");
                String area_code = dimInfo.getString("AREA_CODE");
                String iso_code = dimInfo.getString("ISO_CODE");
                String iso31662 = dimInfo.getString("ISO_3166_2");

                orderWide.setProvince_name(province_name);
                orderWide.setProvince_area_code(area_code);
                orderWide.setProvince_iso_code(iso_code);
                orderWide.setProvince_3166_2_code(iso31662);
            }
        }, 60, TimeUnit.SECONDS);

        //5.3 关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDS, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public String getId(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSku_id());
                    }

                    @Override
                    public void Join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        orderWide.setSku_name(dimInfo.getString("SKU_NAME"));
                        orderWide.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(dimInfo.getLong("SPU_ID"));
                        orderWide.setTm_id(dimInfo.getLong("TM_ID"));
                    }
                }, 60, TimeUnit.SECONDS);

        //5.4 关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(
                orderWideWithSkuDS, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public void Join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }

                    @Override
                    public String getId(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //5.5 关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void Join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }

                    @Override
                    public String getId(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //5.6 关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithTmDS, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void Join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }

                    @Override
                    public String getId(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);

        orderWideWithCategory3DS.print("Result>>>>>>>>>>");

        //将数据写入kafka

        orderWideWithCategory3DS.map(JSONObject::toJSONString).addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic));

        environment.execute();
    }
}