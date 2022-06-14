package com.cmbc.sink;/*
 * @Package com.cmbc.kafka.transaction.monitoring
 * @author wang shuangli
 * @date 2022-05-13 23:38
 * @version V1.0
 * @Copyright © 2015-2021 北京海致星图科技有限公司
 */

import com.alibaba.fastjson.JSONObject;

import java.io.Serializable;

/**
 * clickhouse sink 参数实例
 * @author yinlilan
 *
 */
public class ClickhouseSink implements Serializable {

    private static final long serialVersionUID = -4410041701538783205L;

    private final String url;

    private final String index;

    private final String username;

    private final String password;

    public String getUrl() {
        return url;
    }

    public String getIndex() {
        return index;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public ClickhouseSink(Object obj) {
        final JSONObject json = JSONObject.parseObject(obj.toString());
        this.url = json.getString("url");
        this.index = json.getString("index");
        this.username = json.getString("username");
        this.password = json.getString("password");
    }

}

