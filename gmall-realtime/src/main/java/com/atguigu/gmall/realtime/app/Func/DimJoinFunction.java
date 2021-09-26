package com.atguigu.gmall.realtime.app.Func;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

/**
 * @author wang
 * @create 2021-09-25 23:14
 */
public interface DimJoinFunction<T> {
    //获取在本方法内实现不了的方法，则通过抽象类交由外部自己实现
     abstract String getId(T input);

    //定义抽象类，当谁调用此方法的时候自己去有针对性的实现这个方法
     abstract void Join(T input, JSONObject dimInfo) throws ParseException;
}
