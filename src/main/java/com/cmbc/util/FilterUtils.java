package com.cmbc.util;/*
 * @Package com.cmbc.util
 * @author wang shuangli
 * @date 2022-05-18 15:35
 * @version V1.0
 * @Copyright © 2015-2021
 */

import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;

import java.util.HashMap;
import java.util.Map;

/**
 * 公式计算
 * @author hgSuper
 * @date 2021-10-08
 */
public class FilterUtils {

    /**
     * 公式计算
     * @param jexlExp 计算公式
     * @param map 公式中需要替换的参数
     * @return Object
     */
    public static Object convertToCode(String jexlExp, Map<String, Object> map) {
        JexlEngine jexl = new JexlEngine();
        Expression expression = jexl.createExpression(jexlExp);
        JexlContext jc = new MapContext();
        for(String key:map.keySet()){
            jc.set(key,map.get(key));
        }
        if(null == expression.evaluate(jc)){
            return "";
        }
        return expression.evaluate(jc);


    }

    public static void main(String[] args) {
        HashMap<String, Object> map = new HashMap<>();
        map.put("money",2100);
        map.put("age",11);
        String expression = "money>=2000&&money<=4000&&age==11";
        boolean code = (boolean)convertToCode(expression,map);
        System.out.println(code);

    }
}
