package myudf;

import com.aliyun.odps.udf.UDF;
import com.google.gson.Gson;


public class JsonParseTest extends UDF {
    // TODO define parameters and return type, e.g:  public String evaluate(String a, String b)
    public String evaluate(String json) {

        String path = "$.contact_list.[?(@.call_in_cnt - @.call_in_cnt >= 0 )].phone_num";
        // String path="$.资信提示信息.[?(@.项目 == \"01.最近六个月内贷款申请信息\")]";
        try {
            Object ret = com.jayway.jsonpath.JsonPath.read(json, path);
            Gson gson = new Gson();
//            path ="$.strmax()";
//            ret = com.jayway.jsonpath.JsonPath.read(gson.toJson(ret), path);
            return gson.toJson(ret);
        }catch(Exception e){
            return e.getMessage();
        }
    }

}