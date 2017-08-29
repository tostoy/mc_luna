package myudf.idcard;

import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDF;
import com.aliyun.odps.udf.UDFException;
import com.google.common.collect.Maps;
import model.ChinaIdArea;
import util.IdCardUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

public class IdCardValid extends UDF {
    // TODO define parameters and return type, e.g:  public String evaluate(String a, String b)
    ExecutionContext ctx;
    private Map<String, ChinaIdArea> chinaIdAreaMap;

    @Override
    public void setup(ExecutionContext ctx) throws UDFException {
        this.ctx = ctx;
        try {
            InputStream in = ctx.readResourceFileAsStream("china_p_c_a.txt");
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String line;
            chinaIdAreaMap = Maps.newHashMap();
            while ((line = br.readLine()) != null) {
                if(line.startsWith("#")){
                    continue;
                }
                String[] results = line.split("\t", 4);
                chinaIdAreaMap.put(results[0], new ChinaIdArea(results[1], results[2], results[3]));
            }
            br.close();


        } catch (IOException e) {
            throw new UDFException(e);
        }
    }
    public Boolean evaluate(String card) {
        return IdCardUtil.isValidIdCard(card,chinaIdAreaMap);
    }
}