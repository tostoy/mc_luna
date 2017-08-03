package com.aliyun.odps.examples.udf;

import com.aliyun.odps.udf.UDF;

public class UDFtest extends UDF {
    // TODO define parameters and return type, e.g:  public String evaluate(String a, String b)
    public String evaluate(String s) {
        return "hello world:" + s;
    }
}