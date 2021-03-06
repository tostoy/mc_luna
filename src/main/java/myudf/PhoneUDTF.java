package myudf;

import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;
import com.jayway.jsonpath.JsonPath;
import util.PhoneUtil;


import java.util.ArrayList;
import java.util.List;

// TODO define input and output types, e.g. "string,string->string,bigint".
@Resolve({"string,bigint->string,bigint"})
public class PhoneUDTF extends UDTF {

    @Override
    public void setup(ExecutionContext ctx) throws UDFException {

    }

    @Override
    public void process(Object[] args) throws UDFException {
        // TODO
        Long a = (Long) args[1];
        String b = (String) args[0];
        String c="";
        List<Integer> call_in_cnt= null;
        List<Integer> call_out_cnt = null;
        List<String> phone_number = null;
        List<String> result = new ArrayList<String>();
        if(b !=null && !b.equals("null")){
            try {
                call_in_cnt = JsonPath.read(b, "$.contact_list..call_in_cnt");
                call_out_cnt = JsonPath.read(b,"$.contact_list..call_out_cnt");
                phone_number = JsonPath.read(b,"$.contact_list..phone_num");
            }catch(Exception e){
            }

        }
        if(call_in_cnt !=null && call_out_cnt !=null && phone_number != null){
            int length = call_in_cnt.size();
            for(int i = 0; i < length; i++){

                int in = call_in_cnt.get(i);
                int out = call_out_cnt.get(i);
                String num = phone_number.get(i);
                if(num.startsWith("+86")){
                    num= num.substring(3);
                }else if(num.startsWith("+")){
                    num= num.substring(1);
                }
                if(!PhoneUtil.filter(num)){
                    continue;
                }
                if(in >= out * 3 && in >= 3) {
                   result.add(num);
                }

            }
        }else{
            result = null;
        }
        if(result != null){
            for(Object o:result){
                forward(o.toString(),a);
            }
        }
    }

    @Override
    public void close() throws UDFException {

    }

}