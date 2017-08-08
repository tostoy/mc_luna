package myudf;

import com.aliyun.odps.udf.UDF;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;

public class GetCountByDate extends UDF {
    // TODO define parameters and return type, e.g:  public String evaluate(String a, String b)
    public Long evaluate(String date, String dateStr, String countStr) {
        if (dateStr == null || dateStr.length() <= 0 || countStr == null || countStr.length() <= 0) {
            return 0L;
        }
/*      String date = "20170301";
        String dateStr = "20170425,20161108,20170301,20161201,20161021,20170309,20170722,20170103,20161029,20170511,20170111,20161116,20161217,20170121,20170325,20170531,20161226,20161013,20170707,20170619";
        String countStr = "15,4,12,6,2,13,20,9,3,16,10,5,7,11,14,17,8,1,19,18";*/

        String[] dateArr = dateStr.split(",");
        String[] countArr = countStr.split(",");

        TreeMap<String, Long> dict = new TreeMap<String, Long>();
        for (int i = 0 ; i <dateArr.length ; i++ ) {
            dict.put(dateArr[i].toString(), Long.parseLong(countArr[i]));
        }

        Long count = 0L;
        String maxDate = "";
        String preKey = "";
        Set<String> keySet = dict.keySet();
        Iterator<String> iter = keySet.iterator();
        while (iter.hasNext()) {
            String key = iter.next();

            if (date.equals(key)) {
                count = dict.get(key);
            }else if (preKey.length() > 0 && date.compareTo(preKey) > 0 & date.compareTo(key) < 0) {
                count = dict.get(preKey);
            }

            preKey = key;
        }

        maxDate = preKey.toString();

        if (count == 0 && date.compareTo(maxDate) > 0) {
            count = dict.get(maxDate);
        }

        return count;
    }
}