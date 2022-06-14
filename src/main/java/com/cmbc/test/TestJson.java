package com.cmbc.test;/*
 * @Package com.cmbc.test
 * @author wang shuangli
 * @date 2022-05-12 20:34
 * @version V1.0
 * @Copyright © 2015-2021
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.junit.Test;

import java.text.ParseException;

public class TestJson {
    public static void main(String[] args) {
        System.out.println(args[0]+"          "+args[1]);

    }


    public JSONObject javaTojson(Person obj) {

        return (JSONObject) JSONObject.toJSON(obj);
    }

    public Person jsonTojava(String text, Class c) {
        return (Person) JSON.parseObject(text, c);
    }

    @Test
    public void test() throws ParseException {
        String filePath2="hdfs://192.168.138.131:9000/test/";
        int index = filePath2.lastIndexOf(":") + 5;
        String uri = filePath2.substring(0, index);
        System.out.println("uri:"+uri);
        /*User user = new User("张三", "男", 18);
        System.out.println(String.valueOf(1));;
        Person jane = new Person("jane", 13, "male", "shanghai");
        JSONObject s = javaTojson(jane);
        System.out.println("sssssssssssss:"+s);
        System.out.println(JSONObject.toJSON(jane));
        String a = "{\"address\":\"shanghai\",\"sex\":\"male\",\"name\":\"jane\",\"age\":13}";
        Person b = JSONObject.parseObject(a, Person.class);
        System.out.println(b.toString());
        String c = "{'tsDate': '20220513104300', 'spv': 'app89', 'intf': 'cap1', 'host': 'BS-224', 'trans_count_total': 9919.0, 'succ_count_total': 9912.0, 'resp_count_total': 9912.0, 'duration_avg': 0.14122354656, 'succ_p': 1.0, 'resp_p': 0.9931231323}";
        System.out.println(JSONObject.parseObject(c).getInteger("trans_count_total"));
        //设置时间格式
        SimpleDateFormat sdfSource = new SimpleDateFormat("yyyyMMddHHmmss");
        SimpleDateFormat sdfTarget = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        String dateSource = "20220513104300";
        String dateTarget = sdfTarget.format(sdfSource.parse(dateSource));
        System.out.println(dateTarget);

        String filePath = "hdfs://192.168.138.131:9000/test/sateudp.properties";
        String hdfsUser = "root";
        PropertiesUtil.initProperties(filePath, hdfsUser);
        int checkpoint_interval = PropertiesUtil.getInt("checkpoint_interval");
        int parallelism = PropertiesUtil.getInt("parallelism");
        String source_kafka_bootstrap_servers = PropertiesUtil.getString("source_kafka_bootstrap_servers");
        String source_kafka_group_id = PropertiesUtil.getString("source_kafka_group_id");
        String source_kafka_topic = PropertiesUtil.getString("source_kafka_topic");
        String source_kafka_offset = PropertiesUtil.getString("source_kafka_offset");
        String institution = PropertiesUtil.getString("institution");
        String system = PropertiesUtil.getString("system");
        String system_code = PropertiesUtil.getString("system_code");
        String system_host = PropertiesUtil.getString("system_host");
        String system_spv = PropertiesUtil.getString("system_spv");
        String trigger_condition = PropertiesUtil.getString("trigger_condition");
        String sink_kafka_bootstrap_servers = PropertiesUtil.getString("sink_kafka_bootstrap_servers");
        String sink_kafka_topic = PropertiesUtil.getString("sink_kafka_topic");
        String sink_clickhouse_address = PropertiesUtil.getString("sink_clickhouse_address");
        String sink_clickhouse_username = PropertiesUtil.getString("sink_clickhouse_username");
        String sink_clickhouse_password = PropertiesUtil.getString("sink_clickhouse_password");
        String sink_clickhouse_database = PropertiesUtil.getString("sink_clickhouse_database");
        String sink_clickhouse_table = PropertiesUtil.getString("sink_clickhouse_table");
        System.out.println((int) Math.round(0.2778212 * 1000));
        System.out.println(0.9931231323 * 100.00d);
        String str = "86.64566666";
        BigDecimal bd = new BigDecimal(Double.parseDouble(str));
        System.out.println(bd.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue());
        System.out.println("=================");
        DecimalFormat df = new DecimalFormat("#.00");
        System.out.println(df.format(Double.parseDouble(str)));
        System.out.println("=================");
        System.out.println(String.format("%.2f", Double.parseDouble(str)));
        System.out.println("=================");
        NumberFormat nf = NumberFormat.getNumberInstance();
        nf.setMaximumFractionDigits(2);
        System.out.println(nf.format(Double.parseDouble(str)));
        double f = 111231.5585;
        double f1 = new BigDecimal(f).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("a",1);
        System.out.println(jsonObject.isEmpty());

        Map<String,Object> map=new HashMap<String,Object>();*/



    }


}
