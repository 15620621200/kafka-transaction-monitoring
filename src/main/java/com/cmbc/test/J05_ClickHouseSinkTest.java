package com.cmbc.test;/*
 * @Package com.cmbc.test
 * @author wang shuangli
 * @date 2022-05-14 1:18
 * @version V1.0
 * @Copyright © 2015-2021 北京海致星图科技有限公司
 */


import com.cmbc.domain.J_User;
import com.cmbc.util.J_MyClickHouseUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
    进入clickhouse-client
    use default;
    drop table if exists user_table;
    CREATE TABLE default.user_table(id UInt16, name String, age UInt16 ) ENGINE = TinyLog();
 */
public class J05_ClickHouseSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // source
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        // Transform 操作
        SingleOutputStreamOperator<J_User> dataStream = inputStream.map(new MapFunction<String, J_User>() {
            @Override
            public J_User map(String data) throws Exception {
                String[] split = data.split(",");
                return J_User.of(Integer.parseInt(split[0]),
                        split[1],
                        Integer.parseInt(split[2]));
            }
        });

        // sink
        String sql = "INSERT INTO default.user_table (id, name, age) VALUES (?,?,?)";
        J_MyClickHouseUtil jdbcSink = new J_MyClickHouseUtil(sql,"192.168.138.132",8123,"default","default","hadoop");
        dataStream.addSink(jdbcSink);
        dataStream.print();

        env.execute("clickhouse sink test");
    }
}
