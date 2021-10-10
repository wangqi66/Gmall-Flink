package com.atguigu.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author wang
 * @create 2021-10-08 15:59
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeywordStats {
    private String keyword;
    private Long ct;
    private String source;
    private String stt;
    private String edt;
    private Long ts;
}