package myudf;

import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;

import java.util.List;
import java.io.FileInputStream;

// TODO define input and output types, e.g. "string,string->string,bigint".
@Resolve({"bigint,string->bigint,string"})
public class UDTFHello extends UDTF {

    @Override
    public void setup(ExecutionContext ctx) throws UDFException {

    }

    @Override
    public void process(Object[] args) throws UDFException {
        // TODO
        Long a = (Long) args[0];
        String b = (String) args[1];
        String c="";
        List<Object> x= null;
        if(b !=null && !b.equals("null")){
            try {
                x = com.jayway.jsonpath.JsonPath.read(b, "$.card_list..card_no");

            }catch(Exception e){

            }

        }
        if(x !=null ){
//          forward(a, x.getClass().getName());
            for(Object o:x){
                forward(a, o.toString());
            }
        }else {
            forward(a, c);
        }
    }

    @Override
    public void close() throws UDFException {

    }

}