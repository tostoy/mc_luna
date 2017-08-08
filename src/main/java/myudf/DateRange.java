package myudf;

import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

// TODO define input and output types, e.g. "string,string->string,bigint".
@Resolve({"string,string->string"})
public class DateRange extends UDTF {

    @Override
    public void setup(ExecutionContext ctx) throws UDFException {

    }

    @Override
    public void process(Object[] args) throws UDFException {

        String startDate = (String) args[0];
        String endDate = (String) args[1];
        String nowDate = startDate;

        while (true) {
            forward(nowDate);
            if (nowDate.equals(endDate)) break;
            nowDate = getDate(nowDate,1);
        }
    }

    @Override
    public void close() throws UDFException {

    }

    public static String getDate(String startDate, int day) {
        Calendar c = Calendar.getInstance();//获得一个日历的实例
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date date = null;
        try{
            date = sdf.parse(startDate);//初始日期
        }catch(Exception e){

        }
        c.setTime(date);//设置日历时间
        c.add(Calendar.DATE, day);//在日历的月份上增加
        String strDate = sdf.format(c.getTime());//的到你想要得后的日期
        return strDate;
    }

}