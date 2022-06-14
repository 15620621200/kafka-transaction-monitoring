package com.cmbc.util;/*
 * @Package com.cmbc.util
 * @author wang shuangli
 * @date 2022-05-14 1:16
 * @version V1.0
 * @Copyright © 2015-2021 北京海致星图科技有限公司
 */


import com.cmbc.domain.J_User;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;


public class J_MyClickHouseUtil extends RichSinkFunction<J_User> {
    Connection connection = null;

    String sql;
    String host;
    int port;
    String database;
    String username;
    String password;


    public J_MyClickHouseUtil(String sql, String host, int port, String database, String username, String password) {
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
    public void invoke(J_User user, Context context) throws Exception {
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setLong(1, user.id);
        preparedStatement.setString(2, user.name);
        preparedStatement.setLong(3, user.age);
        preparedStatement.addBatch();

        long startTime = System.currentTimeMillis();
        int[] ints = preparedStatement.executeBatch();
        connection.commit();
        long endTime = System.currentTimeMillis();
        System.out.println("批量插入完毕用时：" + (endTime - startTime) + " -- 插入数据 = " + ints.length);
    }
}