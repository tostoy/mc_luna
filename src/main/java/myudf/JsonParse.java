package myudf;

import com.aliyun.odps.udf.UDF;
import com.google.gson.Gson;


public class JsonParse extends UDF {
    // TODO define parameters and return type, e.g:  public String evaluate(String a, String b)
    public String evaluate(String json,String  path) {
       // String path="$..call_len";
        try {
            Object ret = com.jayway.jsonpath.JsonPath.read(json, path);
            Gson gson = new Gson();
            return gson.toJson(ret);
        }catch(Exception e){
            return null;
        }
    }

}