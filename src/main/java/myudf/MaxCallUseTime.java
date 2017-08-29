package myudf;

import com.aliyun.odps.udf.UDF;
import com.jayway.jsonpath.JsonPath;
import java.util.List;
import java.util.HashMap;
public class MaxCallUseTime extends UDF {
    // TODO define parameters and return type, e.g:  public String evaluate(String a, String b)
    public Long evaluate(String s) {
        Long max =0L;
        String de="";
        HashMap<String,Integer> result=new HashMap<String, Integer>();
        try {
            List<Object> x =JsonPath.read(s, "$.[?(@.use_time)]");
            if (x != null) {
                for(Object o:x){
                    HashMap hm = (HashMap)o;
                    int use_time= (Integer) hm.get("use_time");
                    String phone =(String) hm.get("other_cell_phone");
                    de=phone;
                    if(result.containsKey(phone)){
                       result.put(phone, result.get(phone)+use_time);
                    }else{
                        result.put(phone, use_time);
                    }
                    /*Long l = Long.valueOf((Integer)o);
                    if(l>max){
                        max =l;
                    }*/
                }
                for(String str:result.keySet()){
                    Long l = Long.valueOf(result.get(str));
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