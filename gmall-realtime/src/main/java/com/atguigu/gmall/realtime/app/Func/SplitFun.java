package com.atguigu.gmall.realtime.app.Func;

import com.atguigu.gmall.realtime.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @author wang
 * @create 2021-10-08 15:30
 */

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFun extends TableFunction<Row> {

    public void eval(String keyWord){

        List<String> splitKryWord = KeywordUtil.splitKryWord(keyWord);

        for (String s : splitKryWord) {
            collect(Row.of(s));
        }

    }


}
