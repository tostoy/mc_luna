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
@Resolve({"string,string,string,string,string,string->string,string,string,string,string,string,string,string,string,string"})
public class ContactRecordsFlat extends UDTF {

    @Override
    public void setup(ExecutionContext ctx) throws UDFException {

    }

    @Override
    public void process(Object[] args) throws UDFException {
        //id,user_id,x_data,created_at,updated_at,type
        //ArrayList<List> contentResultList  = new ArrayList();
        String id = (String) args[0];
        String user_id = (String) args[1];
        String x_data = (String) args[2];
        String created_at = (String) args[3];
        String updated_at = (String) args[4];
        String type = (String) args[5];
        // String pt = (String) args[3];
        String date = "", duration = "", name = "", phone_num = "", call_type = "";
        if (x_data != null & id != null & user_id != null) {
            x_data = x_data.trim();
            id = id.trim();
            user_id = user_id.trim();
            ReadContext context  = JsonPath.parse(x_data);
            List items = new ArrayList();
            items = context.read("$.[*]");
            for (Object item : items) {
                date = JsonPath.read(item,"$.date");
                duration = JsonPath.read(item,"$.duration");
                name = JsonPath.read(item,"$.name");
                phone_num = JsonPath.read(item,"$.phone_num");
                call_type = JsonPath.read(item,"$.type");
                forward(id, user_id, date, duration, name, phone_num, call_type, created_at, updated_at, type);
            }
        }
    }

    @Override
    public void close() throws UDFException {

    }
}
