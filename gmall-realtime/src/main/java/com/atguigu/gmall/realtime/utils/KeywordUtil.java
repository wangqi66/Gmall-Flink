package com.atguigu.gmall.realtime.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wang
 * @create 2021-10-01 18:27
 */
public class KeywordUtil {

    public static List<String> splitKryWord(String text){

        //创建集合存储分出来的词
        List<String> list = new ArrayList<>();

        //创建ik对象
        StringReader reader = new StringReader(text);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, false);

        try {
            Lexeme next = ikSegmenter.next();

            while (next!=null){
                //分词对象获取数据
                String lexemeText = next.getLexemeText();
                list.add(lexemeText);
                next=ikSegmenter.next();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

      return list;
    }

    public static void main(String[] args) {
        String text = "Apple iPhoneXSMax (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双待";
        System.out.println(KeywordUtil.splitKryWord(text));
    }

}
