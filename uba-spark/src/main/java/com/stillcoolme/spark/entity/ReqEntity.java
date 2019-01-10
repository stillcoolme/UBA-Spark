package com.stillcoolme.spark.entity;

/**
 * Created by zhangjianhua on 2018/10/31.
 */
public class ReqEntity {

    public String id;
    public String reqType;
    public String reqData;
    //public ServiceEndpoint resDest;

    public ReqEntity() {
    }

    public ReqEntity(String id, String reqType, String reqData) {
        this.id = id;
        this.reqType = reqType;
        this.reqData = reqData;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getReqType() {
        return reqType;
    }

    public void setReqType(String reqType) {
        this.reqType = reqType;
    }

    public String getReqData() {
        return reqData;
    }

    public void setReqData(String reqData) {
        this.reqData = reqData;
    }

}
