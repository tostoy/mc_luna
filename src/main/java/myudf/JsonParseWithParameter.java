package myudf;

import com.aliyun.odps.udf.UDF;
import com.google.gson.Gson;

public class JsonParseWithParameter extends UDF {
    // TODO define parameters and return type, e.g:  public String evaluate(String a, String b)
    public String evaluate(String json) {
        String  path ="$.tradedetails.tradedetails.[?(@.trade_time > {{1}})].amount";
        String param1 ="2016-02-2";
        // String path="$..call_len";
        path =path.replace("{{1}}","\""+param1+"\"");

        try {
            Object ret = com.jayway.jsonpath.JsonPath.read(json, path);
            Gson gson = new Gson();
            return gson.toJson(ret);
        }catch(Exception e){
            return null;
        }
    }
    public String evaluate(String json,String  path, String param1) {
        // String path="$..call_len";
        path =path.replace("{{1}}","\""+param1+"\"");
        try {
            Object ret = com.jayway.jsonpath.JsonPath.read(json, path);
            Gson gson = new Gson();
            return gson.toJson(ret);
        }catch(Exception e){
            return null;
        }
    }
    public String evaluate(String json,String  path, Long param1) {
        // String path="$..call_len";
        path =path.replace("{{1}}",param1.toString());
        try {
            Object ret = com.jayway.jsonpath.JsonPath.read(json, path);
            Gson gson = new Gson();
            return gson.toJson(ret);
        }catch(Exception e){
            return null;
        }
    }
}