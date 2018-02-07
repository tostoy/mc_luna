package myudf.featureUDTF;

import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;

import java.util.ArrayList;
import java.util.List;

// TODO define input and output types, e.g. "string,string->string,bigint".
@Resolve({"string,string,bigint,string,string,string,string->string,string,bigint,string,string,string,bigint,bigint,bigint,string,string"})
public class CreditJxlDataFlat extends UDTF {

    @Override
    public void setup(ExecutionContext ctx) throws UDFException {

    }

    @Override
    public void process(Object[] args) throws UDFException {
        //id,person_id,credit_person_id,created_time,x_data,collect_token,updated_time
        //id,person_id,credit_person_id,created_time,phone_num, contact_name, call_in_cnt, call_out_cnt, call_cnt ,collect_token,updated_time

        String id = (String) args[0];
        String person_id = (String) args[1];
        Long credit_person_id = (Long) args[2];
        String created_time = (String) args[3];
        String x_data = (String) args[4];
        String collect_token = (String) args[5];
        String updated_time = (String) args[6];

        String phone_num = "", contact_name = "";
        int call_in_cnt, call_out_cnt, call_cnt;

        if (x_data != null & id != null & person_id != null ) {
            x_data = x_data.trim();
            ReadContext context = JsonPath.parse(x_data);
            List items = new ArrayList();
            items = context.read("$.contact_list.[*]");
            for (Object item : items) {
                contact_name = JsonPath.read(item, "$.contact_name");
                call_in_cnt = JsonPath.read(item, "$.call_in_cnt");
                call_out_cnt = JsonPath.read(item, "$.call_out_cnt");
                call_cnt = JsonPath.read(item, "$.call_cnt");
                try {
                    phone_num = JsonPath.read(item, "$.phone_num");
                } catch (Exception e) {
                    phone_num = "未知号码";
                }
                forward(id,person_id,credit_person_id,created_time,phone_num, contact_name, call_in_cnt, call_out_cnt, call_cnt ,collect_token,updated_time);
            }
        }
    }

    @Override
    public void close() throws UDFException {

    }
}
