package com.atguigu.gmall.realtime.utils22;

import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.utils.TransientSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author wang
 * @create 2021-09-30 10:27
 */
public class ClickUT {


    public static <T> SinkFunction<T> getSink(String sql) {

        return JdbcSink.<T>sink(sql, new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        //获取sql中的？来将值传入prstate

                        Class<?> aClass = t.getClass();

                        Field[] declaredFields = aClass.getDeclaredFields();

                        int offset = 0;
                        for (int i = 0; i < declaredFields.length; i++) {

                            try {
                                Field field = declaredFields[i];

                                TransientSink annotation = field.getAnnotation(TransientSink.class);
                                if (annotation != null) {
                                    offset++;
                                    continue;
                                }
                                field.setAccessible(true);
                                Object value = field.get(t);
                                preparedStatement.setObject(i + 1 - offset, value);
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }

                        }

                    }
                }, new JdbcExecutionOptions.Builder()
                        .withBatchSize(2).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER).build());


    }


}
