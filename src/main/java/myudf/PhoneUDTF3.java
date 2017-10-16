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
@Resolve({"string,bigint,bigint->string,bigint,bigint"})
public class PhoneUDTF3 extends UDTF {

    @Override
    public void setup(ExecutionContext ctx) throws UDFException {

    }

    @Override
    public void process(Object[] args) throws UDFException {
        // TODO
        Long a = (Long) args[1];
        String b = (String) args[0];
        Long a2 = (Long) args[2];
        String c="";
        List<String> phone_number = null;
        List<String> result = new ArrayList<String>();
        if(b !=null && !b.equals("null")){
            try {
                phone_number = JsonPath.read(b,"$.contact_list..phone_num");
            }catch(Exception e){
            }

        }
        if( phone_number != null){
            int length = phone_number.size();
            for(int i = 0; i < length; i++){

                String num = phone_number.get(i);
                if(num.startsWith("+86")){
                    num= num.substring(3);
                }else if(num.startsWith("+")){
                    num= num.substring(1);
                }
                if(!PhoneUtil.filter(num)){
                    continue;
                }
                    result.add(num);
            }
        }else{
            result = null;
        }
        if(result != null){
            for(Object o:result){
                forward(o.toString(),a, a2);
            }
        }
    }

    @Override
    public void close() throws UDFException {

    }

}