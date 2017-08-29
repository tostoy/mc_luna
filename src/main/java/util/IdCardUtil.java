package util;

import com.google.common.base.Strings;
import model.ChinaIdArea;

import java.util.Map;

/**
 * Created by Administrator on 2017/8/29 0029.
 */
public class IdCardUtil {
    private static int[] weight = {7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2};    //十七位数字本体码权重
    private static char[] validate = {'1', '0', 'X', '9', '8', '7', '6', '5', '4', '3', '2'};    //mod11,对应校验码字符值

   public static boolean isValidIdCard(String card, Map<String,ChinaIdArea> chinaIdAreaMap) {
        if (card != null && card.length() != 0) {
            int cardLength = card.length();
            //身份证只有15位或18位
            if (cardLength == 18) {
                String card17 = card.substring(0, 17);
                // 前17位必需都是数字
                if (!card17.matches("[0-9]+")) {
                    return false;
                }
                char validateCode = getValidateCode(card17);
                if (validateCode == card.charAt(17)) {
                    return true;
                }
            } else if (cardLength == 15) {
                if (!card.matches("[0-9]+") || getCardValue(card,chinaIdAreaMap) == null) {
                    return false;
                }
                return true;
            }
        }

        return false;
    }
    /**
     * 获取正确的校验码
     *
     * @param card17 18位身份证前17位
     * @return
     */
    private static char getValidateCode(String card17) {
        int sum = 0, mode = 0;
        for (int i = 0; i < card17.length(); i++) {
            sum = sum + (card17.charAt(i) - 48) * weight[i];
        }
        mode = sum % 11;
        return validate[mode];
    }
    public static ChinaIdArea getCardValue(String cardString, Map<String,ChinaIdArea> chinaIdAreaMap) {

        int cardLength = cardString.length();
        //身份证只有15位或18位
        if (cardLength != 15 && cardLength != 18) {
            return null;
        }

        String cardPrefix = cardString.substring(0, 6);
        if (chinaIdAreaMap.containsKey(cardPrefix)) {
            return chinaIdAreaMap.get(cardPrefix);
        }

        return null;
    }
}
