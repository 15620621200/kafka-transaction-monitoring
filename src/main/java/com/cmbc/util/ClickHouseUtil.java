package com.cmbc.util;/*
 * @Package com.cmbc.util
 * @author wang shuangli
 * @date 2022-05-14 0:03
 * @version V1.0
 * @Copyright © 2015-2021 北京海致星图科技有限公司
 */


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class ClickHouseUtil {
    private static Connection connection;

    public static Connection getConn(String host, int port, String database, String username, String password) throws SQLException, ClassNotFoundException {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        String address = "jdbc:clickhouse://" + host + ":" + port + "/" + database;
        System.out.println(address);
        connection = DriverManager.getConnection(address, username, password);
        return connection;
    }

    //    public static Connection getConn(String host, int port) throws SQLException, ClassNotFoundException {
//        return getConn(host,port,"default");
//    }
//    public static Connection getConn() throws SQLException, ClassNotFoundException {
//        return getConn("node2",8123);
//    }
    public void close() throws SQLException {
        connection.close();
    }
}
