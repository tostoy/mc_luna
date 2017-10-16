package myudf;

import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;
import com.jayway.jsonpath.JsonPath;


import java.util.ArrayList;
import java.util.List;
import util.PhoneUtil;

// TODO define input and output types, e.g. "string,string->string,bigint".
@Resolve({"string,bigint->string,bigint"})
public class PhoneUDTF2 extends UDTF {

    @Override
    public void setup(ExecutionContext ctx) throws UDFException {

    }

    @Override
    public void process(Object[] args) throws UDFException {
        // TODO
        Long a = (Long) args[1];
        String b = (String) args[0];
        String c="";
        List<Integer> call_out_len= null;
        List<Integer> call_out_cnt = null;
        List<String> phone_number = null;
        List<String> result = new ArrayList<String>();
        if(b !=null && !b.equals("null")){
            try {
                call_out_len = JsonPath.read(b, "$.contact_list..call_out_len");
                call_out_cnt = JsonPath.read(b,"$.contact_list..call_out_cnt");
                phone_number = JsonPath.read(b,"$.contact_list..phone_num");
            }catch(Exception e){
            }

        }
        if(call_out_len !=null && call_out_cnt !=null && phone_number != null){
            int length = call_out_len.size();
            for(int i = 0; i < length; i++){
                Object call_len = call_out_len.get(i);
                double call_len1 =0;
                if(call_len instanceof  Double){
                    call_len1 = ((Double) call_len).doubleValue();
                }else
                if(call_len instanceof  Integer){
                    call_len1 = ((Integer) call_len).doubleValue();
                }

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
                if(call_len1 * 60 <= out * 15) {
                    result.add(num);
                }

            }
        }else{
            result = null;
        }
        if(result != null){
            for(Object o:result){
                forward(o.toString(), a);
            }
        }
    }

    @Override
    public void close() throws UDFException {

    }

}