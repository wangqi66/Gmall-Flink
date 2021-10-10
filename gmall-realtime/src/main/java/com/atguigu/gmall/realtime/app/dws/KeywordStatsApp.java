package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.app.Func.SplitFun;
import com.atguigu.gmall.realtime.bean.KeywordStats;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author wang
 * @create 2021-10-08 15:15
 */
public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生产环境,并行度设置与Kafka主题的分区数一致
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink-cdc-210426/ck"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(1000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        //TODO 2.使用DDL方式读取Kafka主题的数据创建表
        String groupId = "keyword_stats_app_210426";
        String pageViewSourceTopic = "dwd_page_log";

        tableEnv.executeSql("" +
                "CREATE TABLE page_view ( " +
                "  `common` MAP<STRING,STRING>, " +
                "  `page` MAP<STRING,STRING>, " +
                "  `ts` BIGINT, " +
                "  `rt` as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +
                "  WATERMARK FOR rt AS rt - INTERVAL '2' SECOND " +
                ")"+
                MyKafkaUtil.getKafkaDDL(pageViewSourceTopic, groupId)
                );

        //TODO 3.过滤数据  "item_type":"keyword"  "item":"苹果手机"
        Table filterTable = tableEnv.sqlQuery("" +
                "select " +
                "    page['item'] full_word, " +
                "    rt " +
                "from page_view " +
                "where  " +
                "    page['item_type'] = 'keyword' " +
                "and " +
                "    page['item'] is not null");
        tableEnv.createTemporaryView("filter_table", filterTable);

        //TODO 4.注册UDTF函数 并且做分词处理
        tableEnv.createTemporarySystemFunction("SplitFunc", SplitFun.class);
        Table splitTable = tableEnv.sqlQuery("" +
                "SELECT  " +
                "    word, " +
                "    rt " +
                "FROM filter_table,  " +
                "LATERAL TABLE(SplitFunc(full_word))");
        tableEnv.createTemporaryView("split_table", splitTable);

        //TODO 5.按照单词分组计算WordCount
        Table resultTable = tableEnv.sqlQuery(" " +
                "select " +
                "    'search' source, " +
                "    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                "    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +
                "    word keyword, " +
                "    count(*) ct, " +
                "    UNIX_TIMESTAMP() as ts " +
                "from split_table " +
                "group by word,TUMBLE(rt, INTERVAL '10' SECOND)");

        //TODO 6.将动态表转换为流
        DataStream<KeywordStats> keywordStatsDataStream = tableEnv.toAppendStream(resultTable, KeywordStats.class);
        keywordStatsDataStream.print();

        //TODO 7.写出数据到ClickHouse
        keywordStatsDataStream.addSink(ClickHouseUtil.getSink("insert into keyword_stats_2021(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)"));

        //TODO 8.启动任务
        env.execute("KeywordStatsApp");
    }
}
