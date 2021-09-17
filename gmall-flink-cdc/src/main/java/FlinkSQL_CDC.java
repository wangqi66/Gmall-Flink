import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author wang
 * @create 2021-09-16 22:56
 */
public class FlinkSQL_CDC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        //创建表的执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //创建flinkcdc的source
        TableResult tableResult = tableEnvironment.executeSql("CREATE TABLE base_trademark (" +    //创建动态临时表，表中数据从下面with里面的表中获取
                "  id INT," +
                "  tm_name STRING," +
                "  logo_url STRING" +
                ") WITH (" +
                "  'connector' = 'mysql-cdc'," +
                "  'hostname' = 'hadoop102'," +
                "  'port' = '3306'," +
                "  'username' = 'root'," +
                "  'password' = '123456'," +
                "  'database-name' = 'gmall-flink'," +
                "  'table-name' = 'base_trademark'" +
                ")");

        //查询数据并打印
        tableEnvironment.executeSql("select * from base_trademark").print();

        environment.execute();

    }
}
