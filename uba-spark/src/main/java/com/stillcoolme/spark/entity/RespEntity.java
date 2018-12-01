package com.stillcoolme.spark.entity;

import java.io.Serializable;

/**
 * Created by zhangjianhua on 2018/10/31.
 */
public class RespEntity implements Cloneable, Serializable {
    public boolean success;
    public String reason;
    public String resData;

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public String getResData() {
        return resData;
    }

    public void setResData(String resData) {
        this.resData = resData;
    }
}
