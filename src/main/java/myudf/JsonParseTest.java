package myudf;

import com.aliyun.odps.udf.UDF;
import com.google.gson.Gson;


public class JsonParseTest extends UDF {
    // TODO define parameters and return type, e.g:  public String evaluate(String a, String b)
    public String evaluate(String json) {
//        json= " {\"资信提示信息\": {" +
//                "      \"资信提示\": {" +
//                "        \"项目\": \"01.最近六个月内贷款申请信息\"," +
//                "        \"提示内容\": \"申请笔数 = 4\"," +
//                "        \"提示时间\": \"2017.09.13\"" +
//                "      }" +
//                "    }}";
        String path = "$.个人身份信息.地址.地址明细";
        // String path="$.资信提示信息.[?(@.项目 == \"01.最近六个月内贷款申请信息\")]";
        try {
            Object ret = com.jayway.jsonpath.JsonPath.read(json, path);
            Gson gson = new Gson();
//            path ="$.length()";
//            ret = com.jayway.jsonpath.JsonPath.read(gson.toJson(ret), path);
            return gson.toJson(ret);
        }catch(Exception e){
            return null;
        }
    }

}