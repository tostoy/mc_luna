package wanghui;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;

//import java.io.IOException;
import java.util.*;

@Resolve({"string->string,string"})
public class CreditJXLDataUDTF extends UDTF {

    public void process(Object[] objects) throws UDFException {
        String report = (String) objects[0];
        ReadContext rct = JsonPath.parse(report);
//        LinkedHashMap<String, String> map = rct.read("$.contact_list");
        try {
            List<Object> lists = rct.read("$.contact_list");
            for (Object s : lists) {
                if (s != null && s instanceof LinkedHashMap) {
                    LinkedHashMap<String, String> map = (LinkedHashMap<String, String>) s;
//                System.out.println("s:" + map.keySet().toString());
                    if(map.get("contact_name").equals("未知")) continue;
                    forward(map.get("phone_num"), map.get("contact_name"));
                }
            }
        } catch(Exception e) {

        }
    }
}
