package com.atguigu.gmall.realtime.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @author wang
 * @create 2021-09-26 16:01
 */
@Data
public class PaymentInfo {
    Long id;
    Long order_id;
    Long user_id;
    BigDecimal total_amount;
    String subject;
    String payment_type;
    String create_time;
    String callback_time;
}