package myudf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Json数据扁平化
 */
// TODO define input and output types, e.g. "string,string->string,bigint".
@Resolve({"string,string,string,string->string"})
public class JsonDataFlat extends UDTF {

    @Override
    public void setup(ExecutionContext ctx) throws UDFException {

    }

    @Override
    public void process(Object[] args) throws UDFException {
        // 接收参数
        String jsonStr = (String) args[0];                  // 等待解析的JSON字符串
        String contentItems = (String) args[1];             // 内容项(具体要抽取的项,可能是多行)
        String externalAppendItems = (String) args[2];      // 外部追加项(值为固定值，会被附加到每一行的contentItems中)
        String internalAppendItems = (String) args[3];      // 内部追加项(值为json字符串中的单值,会被附加到每一行的contentItems中)
/*
        String jsonStr = "{\"_id\":\"58f8b3ac55a7c25a578b4567\",\"create_time\":\"2017-04-20 21:12:12\",\"person_id\":1839810,\"telecom_type\":1,\"real_name\":\"**婷\",\"mobile\":\"13825442077\",\"id_card\":\"未身份确认\",\"reg_time\":\"2013-12-06 00:00:00\",\"transactions\":[{\"total_amt\":100.32,\"update_time\":\"2017-05-10 12:42:46\",\"pay_amt\":150.0,\"plan_amt\":64.0,\"bill_cycle\":\"2017-04-01 00:00:00\",\"cell_phone\":\"13825442077\"},{\"total_amt\":84.73,\"update_time\":\"2017-05-10 12:42:46\",\"pay_amt\":0.0,\"plan_amt\":64.0,\"bill_cycle\":\"2017-03-01 00:00:00\",\"cell_phone\":\"13825442077\"},{\"total_amt\":84.19,\"update_time\":\"2017-05-10 12:42:46\",\"pay_amt\":190.0,\"plan_amt\":44.0,\"bill_cycle\":\"2017-02-01 00:00:00\",\"cell_phone\":\"13825442077\"},{\"total_amt\":132.62,\"update_time\":\"2017-05-10 12:42:46\",\"pay_amt\":80.0,\"plan_amt\":63.99,\"bill_cycle\":\"2017-01-01 00:00:00\",\"cell_phone\":\"13825442077\"},{\"total_amt\":101.22,\"update_time\":\"2017-05-10 12:42:46\",\"pay_amt\":100.0,\"plan_amt\":83.98,\"bill_cycle\":\"2016-12-01 00:00:00\",\"cell_phone\":\"13825442077\"}],\"call_list\":[{\"start_time\":\"2017-05-01 10:48:53\",\"update_time\":\"2017-05-10 12:42:46\",\"use_time\":112,\"subtotal\":0.0,\"place\":\"惠州A\",\"init_type\":\"被叫A\",\"call_type\":\"4G+高清语音(VoLTE)本地A\",\"other_cell_phone\":\"15217839190\",\"cell_phone\":\"13825442077\"},{\"start_time\":\"2017-05-01 12:08:40\",\"update_time\":\"2017-05-10 12:42:46\",\"use_time\":8,\"subtotal\":0.0,\"place\":\"惠州B\",\"init_type\":\"被叫B\",\"call_type\":\"4G+高清语音(VoLTE)本地B\",\"other_cell_phone\":\"18026585865\",\"cell_phone\":\"13825442077\"},{\"start_time\":\"2017-05-01 14:22:31\",\"update_time\":\"2017-05-10 12:42:46\",\"use_time\":12,\"subtotal\":0.17,\"place\":\"惠州C\",\"init_type\":\"主叫C\",\"call_type\":\"本地C\",\"other_cell_phone\":\"18607528399\",\"cell_phone\":\"13825442077\"}],\"type\":\"chinamobilegd\",\"update_time\":\"2017-05-10 12:43:22\"}";
        String contentItems = "{'place':'$.call_list.[*].place', 'init_type':'$.call_list.[*].init_type'}";
        String internalAppendItems = "{'id':'$._id', 'create_time': '$.create_time'}";
        String externalAppendItems = "{'abc':'abc_value', 'def': 'def_value'}";
*/
        // 解析JSON值
        ReadContext ctx = JsonPath.parse(jsonStr);

        // 声明数组以便存放对应值键名及键值
        ArrayList<String> contentNameList  = new ArrayList();
        ArrayList<List> contentResultList  = new ArrayList();
        ArrayList<String> internalAppendNameList  = new ArrayList();
        ArrayList<String> internalAppendResultList = new ArrayList();
        ArrayList<String> externalAppendNameList  = new ArrayList();
        ArrayList<String> externalAppendResultList = new ArrayList();

        // 内容项的数据提取及整理
        int i = 0;
        JSONObject jsonObj2 = JSON.parseObject(contentItems);
        for (Map.Entry<String, Object> entry : jsonObj2.entrySet()) {
            contentNameList.add(i, entry.getKey());
            contentResultList.add(i, ctx.read(entry.getValue().toString(), List.class));
            i++;
        }

        // 外部追加项的数据提取及整理
        i = 0;
        if (externalAppendItems != null && externalAppendItems.trim().length() > 0) {
            JSONObject jsonObj1 = JSON.parseObject(externalAppendItems);
            for (Map.Entry<String, Object> entry : jsonObj1.entrySet()) {
                externalAppendNameList.add(i, entry.getKey());
                externalAppendResultList.add(i, entry.getValue().toString());
                i++;
            }
        }

        // 内部追加项的数据提取及整理
        i = 0;
        if (internalAppendItems != null && internalAppendItems.trim().length() > 0) {
            JSONObject jsonObj1 = JSON.parseObject(internalAppendItems);
            for (Map.Entry<String, Object> entry : jsonObj1.entrySet()) {
                internalAppendNameList.add(i, entry.getKey());
                internalAppendResultList.add(i, ctx.read(entry.getValue().toString(), String.class));
                i++;
            }
        }

        // 组装数据
        for (int x = 0 ; x < contentResultList.get(0).size() ; x++ ) {
            HashMap<String, String> lineResult = new HashMap<String, String>();

            // 内容项
            for (int z = 0; z < contentNameList.size(); z++) {
                lineResult.put(contentNameList.get(z), contentResultList.get(z).get(x).toString());
            }

            // 外部追加项
            if (externalAppendItems != null && externalAppendItems.trim().length() > 0) {
                for (int y = 0; y < externalAppendNameList.size(); y++) {
                    lineResult.put(externalAppendNameList.get(y), externalAppendResultList.get(y));
                }
            }

            // 内部追加项
            if (internalAppendItems != null && internalAppendItems.trim().length() > 0) {
                for (int y = 0; y < internalAppendNameList.size(); y++) {
                    lineResult.put(internalAppendNameList.get(y), internalAppendResultList.get(y));
                }
            }

            // UDTF发送
            forward(JSON.toJSONString(lineResult));

            //System.out.println( JSON.toJSONString(lineResult));
        }
    }

    @Override
    public void close() throws UDFException {

    }

}