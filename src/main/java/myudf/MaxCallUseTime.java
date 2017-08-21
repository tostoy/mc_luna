package myudf;

import com.aliyun.odps.udf.UDF;
import com.jayway.jsonpath.JsonPath;
import java.util.List;
public class MaxCallUseTime extends UDF {
    // TODO define parameters and return type, e.g:  public String evaluate(String a, String b)
    public Long evaluate(String s) {
        Long max =0L;
        String de="";
        try {
            List<Object> x =JsonPath.read(s, "$..use_time");
            if (x != null) {
                for(Object o:x){
                    Long l = Long.valueOf((Integer)o);
                    if(l>max){
                        max =l;
                    }
                }
            }
        }catch(Exception e){
            de = e.toString();
        }
        return max;
    }
}