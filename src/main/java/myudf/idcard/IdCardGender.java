package myudf.idcard;

import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDF;
import com.aliyun.odps.udf.UDFException;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import model.ChinaIdArea;
import util.IdCardUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

public class IdCardGender extends UDF {

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
    // TODO define parameters and return type, e.g:  public String evaluate(String a, String b)
    public String evaluate(String card) {
        if (IdCardUtil.isValidIdCard(card,chinaIdAreaMap)) {
            String cardString = card;
            int cardLength = cardString.length();
            int genderValue;
            if (cardLength == 15) {
                genderValue = cardString.charAt(14) - 48;
            }else if (cardLength == 18){
                genderValue = cardString.charAt(17) - 48;
            }else{
                return null;
            }


            if (genderValue % 2 == 0) {
                return "女";
            } else {
                return "男";
            }
        }

        return null;
    }

//    private static boolean isValidIdCard(String card) {
//        if (!Strings.isNullOrEmpty(card)) {
//            int cardLength = card.length();
//            //身份证只有15位或18位
//            if (cardLength == 18) {
//                String card17 = card.substring(0, 17);
//                // 前17位必需都是数字
//                if (!card17.matches("[0-9]+")) {
//                    return false;
//                }
//                char validateCode = IdCardUtil.getValidateCode(card17);
//                if (validateCode == card.charAt(17)) {
//                    return true;
//                }
//            } else if (cardLength == 15) {
//                if (!card.matches("[0-9]+") || IdCardUtil.getCardValue(card) == null) {
//                    return false;
//                }
//                return true;
//            }
//        }
//
//        return false;
//    }
}