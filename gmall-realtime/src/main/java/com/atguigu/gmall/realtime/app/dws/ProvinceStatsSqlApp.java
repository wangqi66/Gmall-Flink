package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.bean.ProvinceStats;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author wang
 * @create 2021-10-01 17:24
 */
public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(4);
        //ck
        //      env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        //        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //        StateBackend fsStateBackend = new FsStateBackend(
        //                "hdfs://hadoop102:8020/gmall/flink/checkpoint/ProvinceStatsSqlApp");
        //        env.setStateBackend(fsStateBackend);
        //        System.setProperty("HADOOP_USER_NAME","atguigu");

        //创建表的执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        String groupId = "province_stats";
        String orderWideTopic = "dwm_order_wide";

        //用sql的方法读取kafka内的数据
        
        tableEnvironment.executeSql("CREATE TABLE order_wide (" +
                "  `province_id` BIGINT,   " +
                "  `province_name` STRING,   " +
                "  `province_area_code` STRING,   " +
                "  `province_iso_code` STRING,   " +
                "  `province_3166_2_code` STRING,   " +
                "  `order_id` BIGINT,   " +
                "  `split_total_amount` DECIMAL,   " +
                "  `create_time` STRING,   " +
                "   rt AS TO_TIMESTAMP(create_time),   " +
                "  WATERMARK FOR rt AS rt - INTERVAL '2' SECOND   " +
                ")"
                + MyKafkaUtil.getKafkaDDL(orderWideTopic,groupId));

        //测试打印
//        tableEnvironment.executeSql("select * from order_wide").print();

        //获取数据
        Table tableResult = tableEnvironment.sqlQuery("select  " +
                "    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') stt,  " +
                "    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') edt,  " +
                "    province_id,  " +
                "    province_name,  " +
                "    province_area_code,  " +
                "    province_iso_code,  " +
                "    province_3166_2_code,  " +
                "    sum(split_total_amount) order_amount,  " +
                "    count(distinct order_id) order_count,  " +
                "    UNIX_TIMESTAMP() AS ts " +
                "from order_wide  " +
                "group by  " +
                "    province_id,  " +
                "    province_name,  " +
                "    province_area_code,  " +
                "    province_iso_code,  " +
                "    province_3166_2_code,  " +
                "    TUMBLE(rt, INTERVAL '10' SECOND)");

        //测试查询到的流
//        tableResult.print();


        //TODO 4.将动态表转换为数据流
        DataStream<ProvinceStats> provinceStatsDataStream = tableEnvironment.toAppendStream(tableResult, ProvinceStats.class);

        //TODO 5.将数据写入ClickHouse
        provinceStatsDataStream.print(">>>>>>>>>>>>>");
        provinceStatsDataStream.addSink(ClickHouseUtil.getSink("insert into province_stats_21 values(?,?,?,?,?,?,?,?,?,?)"));

        //TODO 6.启动任务
        environment.execute();

    }
}
