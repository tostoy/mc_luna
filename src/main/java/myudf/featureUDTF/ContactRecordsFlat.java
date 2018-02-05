package myudf.featureUDTF;

        import com.alibaba.fastjson.JSON;
        import com.alibaba.fastjson.JSONArray;
        import com.aliyun.odps.udf.ExecutionContext;
        import com.aliyun.odps.udf.UDFException;
        import com.aliyun.odps.udf.UDTF;
        import com.aliyun.odps.udf.annotation.Resolve;
        import com.jayway.jsonpath.JsonPath;

// TODO define input and output types, e.g. "string,string->string,bigint".
@Resolve({"string,string,string,string,string,string->string,string,string,string,string,string,string,string,string,string"})
public class ContactRecordsFlat extends UDTF{

    @Override
    public void setup(ExecutionContext ctx) throws UDFException {

    }

    @Override
    public void process(Object[] args) throws UDFException {
        //id,user_id,x_data,created_at,updated_at,type
        String id = (String) args[0];
        String user_id = (String) args[1];
        String x_data = (String) args[2];
        String created_at = (String) args[3];
        String updated_at = (String) args[4];
        String type = (String) args[5];
        // String pt = (String) args[3];
        if (x_data != null & id!= null & user_id!= null) {
            x_data = x_data.trim();
            id = id.trim();
            user_id = user_id.trim();
            JSONArray items = JSON.parseArray(x_data);
            for (int i = 0; i < items.size(); i++) {
                Object date = items.getJSONObject(i).getString("date");
                Object duration = items.getJSONObject(i).getString("duration");
                Object name = items.getJSONObject(i).getString("name");
                Object phone_num = items.getJSONObject(i).getString("phone_num");
                Object call_type = items.getJSONObject(i).getString("type");
                forward(id, user_id, date, duration, name, phone_num, call_type, created_at, updated_at, type);
            }
        }
    }

    @Override
    public void close() throws UDFException {

    }
}
