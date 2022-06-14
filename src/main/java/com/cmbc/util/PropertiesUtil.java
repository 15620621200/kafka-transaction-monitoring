package com.cmbc.util;/*
 * @Package com.cmbc.util
 * @author wang shuangli
 * @date 2022-05-13 17:28
 * @version V1.0
 * @Copyright © 2015-2021
 */

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class PropertiesUtil {

    public static Properties prop = new Properties();

    /**
     * 加载配置文件sateudp.properties
     */
    public static void initProperties(String filePath, String hdfsUser) {
//        String sateudp = System.getProperty("user.dir") + File.separator +"src\\main\\resources"+File.separator +"sateudp.properties";
//        String sateudp = "hdfs://192.168.138.131:9000/test/sateudp.properties";
        try {
//            "hdfs://192.168.138.131:9000/"
            int index = filePath.lastIndexOf(":") + 6;
            String uri = filePath.substring(0, index);
            FileSystem fileSystem = HdfsUtils.getHadoopFileSystem(uri, hdfsUser);
//            prop.load(new InputStreamReader(new FileInputStream(sateudp), StandardCharsets.UTF_8));
            prop.load(new InputStreamReader(fileSystem.open(new Path(filePath)), StandardCharsets.UTF_8));
        } catch (IOException | InterruptedException | URISyntaxException e) {
            e.printStackTrace();
        }
    }

    public static Object getProperty(String name) {
        return prop.get(name);
    }

    public static String getString(String name) {
        Object value = prop.get(name);
        System.out.println(name + ":" + value);
        if (value != null) {
            return value.toString();
        }
        return "";
    }

    public static int getInt(String name) {
        Object value = prop.get(name);
        System.out.println(name + ":" + value);
        if (value != null) {
            return Integer.parseInt(value.toString());
        }
        return 0;
    }

    public static double getDouble(String name) {
        Object value = prop.get(name);
        System.out.println(name + ":" + value);
        if (value != null) {
            return Double.parseDouble(value.toString());
        }
        return 0;
    }

    public static boolean getBoolean(String name) {
        Object value = prop.get(name);
        System.out.println(name + ":" + value);
        return "true".equals(value.toString());


    }
}

