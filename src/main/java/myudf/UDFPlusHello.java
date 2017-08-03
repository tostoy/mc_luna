package myudf;

import com.aliyun.odps.udf.UDF;
import com.jayway.jsonpath.JsonPath;
import java.io.FileInputStream;

public class UDFPlusHello extends UDF {
    // TODO define parameters and return type, e.g:  public String evaluate(String a, String b)
    public String evaluate(Long a, String b ) {
        String c="";

        if(b !=null && !b.equals("null")){
            try {
                Object x = JsonPath.read(b, "$.card_list..card_no");
                c= x.toString();

            }catch(Exception e){
                c="eeeee";
            }

        }

        return "ss2s:" + a + "," + c;
    }
}