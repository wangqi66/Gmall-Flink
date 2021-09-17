package com.atguigu.gmalllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * @author wang
 * @create 2021-09-15 22:54
 */
@RestController //表示返回普通对象而不是页面
@Slf4j
public class LoggerController {

    //@Slf4j  使用这个注解可以在程序中生成一个日志对象，等价于下面的创建
    //    Logger logger = LoggerFactory.getLogger(LoggerController.class);

    @Resource
    private KafkaTemplate<String,String> kafkaTemplate;



    @RequestMapping("/applog")
    public String getLogger(@RequestParam("param") String logStr) {

        //将日志数据打印控制台
        //System.out.println(logStr);
        log.info(logStr);

        //将数据发送到kafka
        kafkaTemplate.send("ods_base_log",logStr);

        return "success";
    }


}
