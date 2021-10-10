package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.Func.DimAsyncFunction;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.bean.PaymentWide;
import com.atguigu.gmall.realtime.bean.ProductStats;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.text.ParseException;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @author wang
 * @create 2021-09-28 19:59
 */
public class ProductStatsApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 1.从Kafka中获取数据流
        String groupId = "product_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        DataStreamSource<String> pageViewSource = environment.addSource(MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId));
        DataStreamSource<String> orderWideSource = environment.addSource(MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId));
        DataStreamSource<String> paymentWideSource = environment.addSource(MyKafkaUtil.getKafkaSource(paymentWideSourceTopic, groupId));
        DataStreamSource<String> cartInfoSource = environment.addSource(MyKafkaUtil.getKafkaSource(cartInfoSourceTopic, groupId));
        DataStreamSource<String> favorInfoSource = environment.addSource(MyKafkaUtil.getKafkaSource(favorInfoSourceTopic, groupId));
        DataStreamSource<String> refundInfoSource = environment.addSource(MyKafkaUtil.getKafkaSource(refundInfoSourceTopic, groupId));
        DataStreamSource<String> commentInfoSource = environment.addSource(MyKafkaUtil.getKafkaSource(commentInfoSourceTopic, groupId));


        //将流转换成javabean
        //点击 /曝光
        SingleOutputStreamOperator<ProductStats> productStatsWithClickAndDisplaysDS = pageViewSource.flatMap(new FlatMapFunction<String, ProductStats>() {
            @Override
            public void flatMap(String value, Collector<ProductStats> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);

                JSONObject page = jsonObject.getJSONObject("page");
                String item_type = page.getString("item_type");
                String page_id = page.getString("page_id");
                //判断是否为点击数据写入点击数据
                if ("good_detail".equals(page_id) && "sku_id".equals(item_type)) {
                    ProductStats productStats = ProductStats.builder()
                            .sku_id(page.getLong("item"))
                            .click_ct(1L)
                            .ts(jsonObject.getLong("ts"))
                            .build();
                    out.collect(productStats);
                }


                //判断是否为曝光数据，并将曝光写入数据 displays 是集合
                JSONArray displays = jsonObject.getJSONArray("displays");
                if (displays != null && displays.size() > 0) {
                    for (int i = 0; i < displays.size(); i++) {
                        JSONObject displayxiao = displays.getJSONObject(i);
                        if ("sku_id".equals(displayxiao.getString("item_type"))) {
                            out.collect(ProductStats.builder()
                                    .display_ct(1L)
                                    .sku_id(displayxiao.getLong("item"))
                                    .ts(jsonObject.getLong("ts"))
                                    .build());
                        }
                    }
                }
            }
        });

        //以下的表都是从dwddwm中拿取数据，已封装好的数据类型，故不再取日志中的信息，需要关注数据表类型字段
        //收藏
        SingleOutputStreamOperator<ProductStats> productStatsWithFavoDS = favorInfoSource.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                return ProductStats.builder()
                        .sku_id(jsonObject.getLong("sku_id"))
                        .favor_ct(1L)
                        .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                        .build();
            }
        });


        //加入购物车
        SingleOutputStreamOperator<ProductStats> productStatsWithCartDS = cartInfoSource.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                return ProductStats.builder()
                        .sku_id(jsonObject.getLong("sku_id"))
                        .cart_ct(1L)
                        .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                        .build();
            }
        });

        //下单，要考虑到一个订单中有多个数据【一个订单，多家店铺】

        SingleOutputStreamOperator<ProductStats> productStatsWithOrderWideDS = orderWideSource.map(new MapFunction<String, ProductStats>() {

            @Override
            public ProductStats map(String value) throws Exception {
                OrderWide orderWide = JSON.parseObject(value, OrderWide.class);

                HashSet<Long> orderIdset = new HashSet<>();
                orderIdset.add(orderWide.getOrder_id());
                return ProductStats.builder()
                        .sku_id(orderWide.getSku_id())
                        .order_sku_num(orderWide.getSku_num())
                        .order_amount(orderWide.getSplit_total_amount())
                        .orderIdSet(orderIdset)
                        .ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
                        .build();
            }
        });

        //支付订单

        SingleOutputStreamOperator<ProductStats> productStatsWithPayDS = paymentWideSource.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                PaymentWide paymentWide = JSON.parseObject(value, PaymentWide.class);
                HashSet<Long> payOrderIds = new HashSet<>();

                payOrderIds.add(paymentWide.getOrder_id());

                return ProductStats.builder()
                        .sku_id(paymentWide.getSku_id())
                        .payment_amount(paymentWide.getSplit_total_amount())
                        .paidOrderIdSet(payOrderIds)
                        .ts(DateTimeUtil.toTs(paymentWide.getPayment_create_time()))
                        .build();
            }
        });

        //退款

        SingleOutputStreamOperator<ProductStats> productStatsWithRefundInfoDS = refundInfoSource.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                HashSet<Long> hashSet = new HashSet<>();
                hashSet.add(jsonObject.getLong("order_id"));
                return ProductStats.builder()
                        .sku_id(jsonObject.getLong("sku_id"))
                        .refundOrderIdSet(hashSet)
                        .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                        .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                        .build();
            }
        });


        //评价
        SingleOutputStreamOperator<ProductStats> productStatsWithCommentInfoDS = commentInfoSource.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                return ProductStats.builder()
                        .sku_id(jsonObject.getLong("sku_id"))
                        .good_comment_ct(GmallConstant.APPRAISE_GOOD.equals(jsonObject.getString("appraise")) ? 1L : 0L)
                        .comment_ct(1L)
                        .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                        .build();
            }
        });


        //将7个流union

        DataStream<ProductStats> unionDS = productStatsWithCartDS.union(productStatsWithClickAndDisplaysDS, productStatsWithCommentInfoDS, productStatsWithFavoDS, productStatsWithOrderWideDS, productStatsWithPayDS, productStatsWithRefundInfoDS);

        //提取事件时间获取watermark

        SingleOutputStreamOperator<ProductStats> unionWithWmDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
            @Override
            public long extractTimestamp(ProductStats element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        //将数据进行分组
        KeyedStream<ProductStats, Long> keyedStream = unionWithWmDS.keyBy(key -> key.getSku_id());

        //开窗聚合
        WindowedStream<ProductStats, Long, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<ProductStats> reduceDS = window.reduce(new ReduceFunction<ProductStats>() {
            @Override
            public ProductStats reduce(ProductStats value1, ProductStats value2) throws Exception {
                value1.setDisplay_ct(value1.getDisplay_ct() + value2.getDisplay_ct());
                value1.setClick_ct(value1.getClick_ct() + value2.getClick_ct());
                value1.setFavor_ct(value1.getFavor_ct() + value2.getFavor_ct());
                value1.setCart_ct(value1.getCart_ct() + value2.getCart_ct());
                value1.setOrder_sku_num(value1.getOrder_sku_num() + value2.getOrder_sku_num());
                value1.setOrder_amount(value1.getOrder_amount().add(value2.getOrder_amount()));
                value1.setPayment_amount(value1.getPayment_amount().add(value2.getPayment_amount()));
                value1.setRefund_amount(value1.getRefund_amount().add(value2.getRefund_amount()));
                value1.setComment_ct(value1.getComment_ct() + value2.getComment_ct());
                value1.setGood_comment_ct(value1.getComment_ct() + value2.getGood_comment_ct());

                value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                value1.getPaidOrderIdSet().addAll(value2.getPaidOrderIdSet());
                value1.getRefundOrderIdSet().addAll(value2.getRefundOrderIdSet());

                return value1;
            }
        }, new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
            @Override
            public void apply(Long aLong, TimeWindow window, Iterable<ProductStats> input, Collector<ProductStats> out) throws Exception {
                ProductStats productStats = input.iterator().next();

                productStats.setOrder_ct(productStats.getOrderIdSet().size() + 0L);
                productStats.setPaid_order_ct(productStats.getPaidOrderIdSet().size() + 0L);
                productStats.setRefund_order_ct(productStats.getRefundOrderIdSet().size() + 0L);

                String stt = DateTimeUtil.toYMDhms(new Date(window.getStart()));
                String edt = DateTimeUtil.toYMDhms(new Date(window.getEnd()));


                productStats.setStt(stt);
                productStats.setEdt(edt);


                out.collect(productStats);
            }
        });

        //关联维度信息

        //关联sku_id的维度信息
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(reduceDS, new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
            @Override
            public String getId(ProductStats input) {
                return input.getSku_id().toString();
            }

            @Override
            public void Join(ProductStats productstats, JSONObject dimInfo) throws ParseException {
                productstats.setSku_name(dimInfo.getString("SKU_NAME"));
                productstats.setSku_price(dimInfo.getBigDecimal("PRICE"));
                productstats.setSpu_id(dimInfo.getLong("SPU_ID"));
                productstats.setTm_id(dimInfo.getLong("TM_ID"));
                productstats.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
            }
        }, 60, TimeUnit.SECONDS);

        //关联spu信息

        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS = AsyncDataStream.unorderedWait(productStatsWithSkuDS, new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
            @Override
            public String getId(ProductStats input) {
                return input.getSpu_id().toString();
            }

            @Override
            public void Join(ProductStats input, JSONObject dimInfo) throws ParseException {
                input.setSpu_name(dimInfo.getString("SPU_NAME"));
            }
        }, 60, TimeUnit.SECONDS);

        //关联品牌维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDS = AsyncDataStream.unorderedWait(productStatsWithSkuDS, new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
            @Override
            public String getId(ProductStats input) {
                return input.getTm_id().toString();
            }

            @Override
            public void Join(ProductStats input, JSONObject dimInfo) throws ParseException {
                input.setTm_name(dimInfo.getString("TM_NAME"));
            }
        }, 60, TimeUnit.SECONDS);

        //关联品类维度

        SingleOutputStreamOperator<ProductStats> productStatsWithCateDS = AsyncDataStream.unorderedWait(productStatsWithSkuDS, new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
            @Override
            public String getId(ProductStats input) {
                return input.getCategory3_id().toString();
            }

            @Override
            public void Join(ProductStats input, JSONObject dimInfo) throws ParseException {
                input.setCategory3_name(dimInfo.getString("CATEGORY3_NAME"));
            }
        }, 60, TimeUnit.SECONDS);



        productStatsWithCateDS.print("关联》》");


        //将数据写入clickHouse
        productStatsWithCateDS.addSink(ClickHouseUtil.getSink("insert into product_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));


        //启动任务
        environment.execute();
    }
}
