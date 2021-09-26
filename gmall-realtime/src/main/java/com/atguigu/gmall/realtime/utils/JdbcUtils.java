package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 用于查询数据
 * 对于业务维度数据而言，通用的较为繁琐，对于关联维度信息，一条sql的变动仅为 tablename和用户id，故另外封装一个类来调用工具类
 *
 * @author wang
 * @create 2021-09-24 19:07
 */
public class JdbcUtils {


    //因为是sql语句查询，故会是结果集，而不是单个结果，故需要用list
    public static <T> List<T> querySql(Connection connection, String sql, Class<T> clz, boolean toCamel) throws Exception {

        ArrayList<T> list = new ArrayList<>();
        PreparedStatement preparedStatement = null;

        //编译sql
        preparedStatement = connection.prepareStatement(sql);

        //执行sql,生成结果集
        ResultSet resultSet = preparedStatement.executeQuery();

        //获取执行结果的元数据信息（其中包含字段，值） [获取列名，值，将结果封装成对象并返回]
        ResultSetMetaData metaData = resultSet.getMetaData();
        //获取结果列的个数(查询一张表，故列的个数是固定)
        int columnCount = metaData.getColumnCount();

        //遍历结果
        while (resultSet.next()) {

            //此处创建泛型对象，用来封装成对象返回（是用户自身参数传进来的，故是 T 类型）【反射】
            T t = clz.newInstance();

            //此处拿到的是结果集中的一条数据

            for (int i = 1; i <= columnCount; i++) {
                //获取列名
                String columnName = metaData.getColumnName(i);
                //判断是否要进行驼峰命名转换、
                if (toCamel) {
                    //在phoenix中数据是全大写 UPPER_UNDERSCORE
                    columnName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                }

                //获取值
                String value = resultSet.getString(i);

                //将列名和结果封装到一个类中
                BeanUtils.setProperty(t, columnName, value);
            }
            //将泛型对象封装到list集合中

            list.add(t);
        }
        preparedStatement.close();
        resultSet.close();
        return list;
    }

//    public static void main(String[] args) throws Exception {
//        Class.forName(GmallConfig.PHOENIX_DRIVER);
//        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
//
//        long start = System.currentTimeMillis();
//        List<JSONObject> list = querySql(connection, "select * from GMALL_REALTIME.DIM_BASE_TRADEMARK", JSONObject.class, true);
//        long end = System.currentTimeMillis();
//
//        System.out.println(end - start);
//        System.out.println(list);
//    }

}



