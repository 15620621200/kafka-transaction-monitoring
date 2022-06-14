package com.cmbc.domain;/*
 * @Package com.cmbc.domain
 * @author wang shuangli
 * @date 2022-05-14 1:14
 * @version V1.0
 * @Copyright © 2015-2021 北京海致星图科技有限公司
 */


public class J_User {
    public int id;
    public String name;
    public int age;

    public J_User(int id, String name, int age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    public static J_User of(int id, String name, int age) {
        return new J_User(id, name, age);
    }
}
