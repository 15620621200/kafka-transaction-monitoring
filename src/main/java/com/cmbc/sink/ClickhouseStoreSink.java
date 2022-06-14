package com.cmbc.sink;/*
 * @Package com.cmbc.kafka.transaction.monitoring
 * @author wang shuangli
 * @date 2022-05-13 23:37
 * @version V1.0
 * @Copyright © 2015-2021 北京海致星图科技有限公司
 */

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Date;
import java.util.Map;

/**
 * clickhouse sink 初始化
 * @author yinlilan
 *
 */
public class ClickhouseStoreSink extends RichSinkFunction<Map<String, String>> {

    private static final long serialVersionUID = 5994842691528274078L;

    final private ClickhouseSink clickhouseSink;

    private Connection connection;

    public ClickhouseStoreSink(final ClickhouseSink clickhouseSink) {
        this.clickhouseSink = clickhouseSink;
    }


    @Override
    public void open(final Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        System.out.println(clickhouseSink.getUrl());
        connection = DriverManager.getConnection(clickhouseSink.getUrl(), clickhouseSink.getUsername(), clickhouseSink.getPassword());
        connection.setAutoCommit(false);
    }

    @Override
    public void invoke(Map<String, String> value, Context context) throws Exception {
        Statement statement = connection.createStatement();
        final StringBuffer sqlCol = new StringBuffer("INSERT INTO ");
        sqlCol.append(clickhouseSink.getIndex() + " (deviceid, metric, timestamp, value, type, error, create_time) VALUES ");
        final JSONArray datas = JSONArray.parseArray(value.get("value"));
        final StringBuffer sqlVal = new StringBuffer();
        for(int i=0; i<datas.size(); i++) {
            final JSONObject data = datas.getJSONObject(i);
            final JSONObject prop = data.getJSONObject("properties");
            for(String key : prop.keySet()) {
                final JSONObject metric = prop.getJSONObject(key);
                if(data.getString("subDeviceId").isEmpty()) {
                    sqlVal.append("('" + data.getString("deviceCode") + "','");
                } else {
                    sqlVal.append("('" + data.getString("deviceCode") + "@" + data.getString("subDeviceId") + "','");
                }
                sqlVal.append(key + "',");
                sqlVal.append(metric.getLong("time")/1000 + ",");
                sqlVal.append(metric.getDoubleValue("value") + ",'");
                sqlVal.append(metric.getString("type") + "','");
                sqlVal.append(metric.getString("error") + "',");
                sqlVal.append((new Date().getTime() / 1000) + "),");
            }
        }
        sqlVal.deleteCharAt(sqlVal.length() -1);
        final String sql = sqlCol.toString() + sqlVal.toString();
        statement.executeQuery(sql);
        connection.commit();
    }

    @Override
    public void close() throws Exception {
        super.close();
        connection.close();
    }
}


