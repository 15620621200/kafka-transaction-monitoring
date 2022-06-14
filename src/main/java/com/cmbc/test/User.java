package com.cmbc.test;/*
 * @Package com.cmbc.test
 * @author wang shuangli
 * @date 2022-05-12 20:56
 * @version V1.0
 * @Copyright © 2015-2021 北京海致星图科技有限公司
 */

import org.junit.Test;

public class User {
    private String name;
    private String sex;
    private int age;


    public User(String name, String sex, int age) {
        this.name = name;
        this.sex = sex;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public String getSex() {
        return sex;
    }

    public int getAge() {
        return age;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Test
    public void test() {

    }
}
