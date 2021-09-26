package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.List;

/**
 * 对于业务维度数据而言，通用的较为繁琐，对于关联维度信息，
 * 一条sql的变动仅为 tablename和用户id，故另外封装一个类来调用本工具类
 * <p>
 * redis的 旁置缓存优化，将查询到数据进行插入到redis，做二级缓存，处理热点数据
 *
 * @author wang
 * @create 2021-09-24 19:49
 */
public class DimUtil {

    public static JSONObject getDimInfo(Connection connection, String tableName, String id)throws Exception  {

        //获取redis内数据
        String key = "DIM:" + tableName + ":" + id;
        Jedis jedis = RedisUtil.getJedis();
        String redisValue = jedis.get(key);
        if (redisValue != null) {
            JSONObject jsonObject = JSON.parseObject(redisValue);
            //设置过期时间
            jedis.expire(key, 24 * 60 * 60);
            jedis.close();
            //将结果进行直接返回调用者
            return jsonObject;
        }


        String sql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName
                + " where id='" + id + "'";

        System.out.println("查询的phoenix中数据sql"+sql);

        //执行sql
        List<JSONObject> list = JdbcUtils.querySql(connection, sql, JSONObject.class, false);


        //将查询到的数据写入到redis
        JSONObject jsonObject = list.get(0);
        jedis.set(key, jsonObject.toJSONString());
        //设置过期时间
        jedis.expire(key, 24 * 60 * 60);
        jedis.close();
        return jsonObject;
    }

    //删除redis中的数据，因为当phoenix中数据发生变更的时候，redis却并不会知道，故此时需要将redis内的数据进行删除
    public static void deleteRedisData(String redisKey){
        //获取链接
        Jedis jedis = RedisUtil.getJedis();

        jedis.del(redisKey);

        jedis.close();
    }


}
