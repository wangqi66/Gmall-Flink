package com.atguigu.gmall.realtime.app.Func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.utils.DimUtil;
import com.atguigu.gmall.realtime.utils.ThreadPoolUtil;
import com.sun.org.apache.regexp.internal.RE;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author wang
 * @create 2021-09-25 16:40
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {

    private Connection connection;
    private ThreadPoolExecutor threadPool;
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        threadPool = ThreadPoolUtil.getInstance();
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }



    //程序执行
    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        //通过线程池来减少io
        threadPool.submit(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                //获取维度信息的id
                String id = getId(input);
                //查询维度信息
                //内部调用工具类，获取表数据信息 反射+泛型
                JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, id);
                //将维度信息进行关联补充
                if (dimInfo!=null){
                    Join(input, dimInfo);
                }
                //将关联好的维度数据信息输入导流中
                resultFuture.complete(Collections.singletonList(input));
            }
        });
    }


    //超时时间
    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("TimeOut" + input);
    }
}
