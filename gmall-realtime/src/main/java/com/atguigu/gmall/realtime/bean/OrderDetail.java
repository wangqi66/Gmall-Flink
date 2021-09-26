package com.atguigu.gmall.realtime.bean;

import lombok.Data;
import java.math.BigDecimal;

/**
 * 订单详情表
 * @author wang
 * @create 2021-09-24 11:25
 */
@Data
public class OrderDetail {
    Long id;
    Long order_id;
    Long sku_id;
    BigDecimal order_price;
    Long sku_num;
    String sku_name;
    String create_time;
    BigDecimal split_total_amount;
    BigDecimal split_activity_amount;
    BigDecimal split_coupon_amount;
    Long create_ts;
}