package com.atguigu.gmall.realtime.utils;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * @author wang
 * @create 2021-09-28 18:25
 */
@Target(ElementType.FIELD)
@Retention(RUNTIME)
public @interface TransientSink {
}
