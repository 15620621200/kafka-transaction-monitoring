package com.cmbc.domain;/*
 * @Package com.cmbc.domain
 * @author wang shuangli
 * @date 2022-05-13 23:50
 * @version V1.0
 * @Copyright © 2015-2021 北京海致星图科技有限公司
 */

public class FullMsg {

    public String time;
    public String institution;
    public String system_code;
    public String healthy_biz;
    public String healthy_sys;
    public int RV;
    public double RS;
    public int RD;
    public double RR;
    public String faults;

    public FullMsg(String time, String institution, String system_code, String healthy_biz, String healthy_sys, int RV, double RS, int RD, double RR, String faults) {
        this.time = time;
        this.institution = institution;
        this.system_code = system_code;
        this.healthy_biz = healthy_biz;
        this.healthy_sys = healthy_sys;
        this.RV = RV;
        this.RS = RS;
        this.RD = RD;
        this.RR = RR;
        this.faults = faults;
    }

    public static FullMsg of(String time, String institution, String system_code, String healthy_biz, String healthy_sys, int RV, double RS, int RD, double RR, String faults) {
        return new FullMsg(time, institution, system_code, healthy_biz, healthy_sys, RV, RS, RD, RR, faults);
    }

}
