package myudf.idcard;

import com.aliyun.odps.udf.UDF;
import model.ChinaIdArea;

/**
 * Created by Administrator on 2017/8/29 0029.
 */
public class IdCardBirthday extends UDF{
    public String evaluate(String card) {
        int cardLength = card.length();
        if (cardLength == 15) {
            return "19" + card.substring(6, 12);
        } else if(cardLength == 18){
            return card.substring(6, 14);
        }
        return null;
    }
}
