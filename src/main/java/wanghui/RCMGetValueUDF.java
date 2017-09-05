package wanghui;
import com.aliyun.odps.udf.UDF;
import com.jayway.jsonpath.*;
import com.jayway.jsonpath.ReadContext;
import java.util.*;

public class RCMGetValueUDF extends UDF {
    public String evaluate(String s, Long index) {
//        index = 33l;
        try {
            ReadContext rct = JsonPath.parse(s);
            return (rct.read("$."+index+".value").toString());
        }catch(Exception e) {
            return "";
        }
    }
}
