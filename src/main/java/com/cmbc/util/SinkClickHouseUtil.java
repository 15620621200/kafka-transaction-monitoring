package com.cmbc.util;/*
 * @Package com.cmbc.util
 * @author wang shuangli
 * @date 2022-05-14 0:06
 * @version V1.0
 * @Copyright © 2015-2021 北京海致星图科技有限公司
 */

import com.cmbc.domain.FullMsg;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;


public class SinkClickHouseUtil extends RichSinkFunction<FullMsg> {
    Connection connection = null;

    String sql;
    String host;
    int port;
    String database;
    String username;
    String password;


    public SinkClickHouseUtil(String sql, String host, int port, String database, String username, String password) {
        this.sql = sql;
        this.host = host;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        connection = ClickHouseUtil.getConn(host, port, database, username, password);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    public void invoke(FullMsg fullMsg, Context context) throws Exception {
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, fullMsg.time);
        preparedStatement.setString(2, fullMsg.institution);
        preparedStatement.setString(3, fullMsg.system_code);
        preparedStatement.setString(4, fullMsg.healthy_biz);
        preparedStatement.setString(5, fullMsg.healthy_sys);
        preparedStatement.setInt(6, fullMsg.RV);
        preparedStatement.setDouble(7, fullMsg.RS);
        preparedStatement.setInt(8, fullMsg.RD);
        preparedStatement.setDouble(9, fullMsg.RR);
        preparedStatement.setString(10, fullMsg.faults);
        preparedStatement.addBatch();

//        long startTime = System.currentTimeMillis();
        int[] ints = preparedStatement.executeBatch();
        connection.commit();
//        long endTime = System.currentTimeMillis();
//        System.out.println("批量插入完毕用时：" + (endTime - startTime) + " -- 插入数据 = " + ints.length);
    }
}
