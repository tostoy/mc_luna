package util;

/**
 * Created by Administrator on 2017/10/12 0012.
 */
public class PhoneUtil {
    /**
     * 2.热线电话不查
     *   400、800开头10位号码；95开头5-15位号码；0开头的3-4位区号的5位96号码；1开头3-5位号码；1010开头8位号码；10086、10001、10000、10010开头任意位数号码及四个号码本身。
     * 3. 有效手机号不查
     * 13、15、18、17开头且是11位；或86+13、15、18、17开头开头，且位数是13位.
     *
     * @param  phone
     * @return
     */
    public static boolean filter(String phone){
            if(phone ==null || phone.length() ==0){
                return false;
            }
            if((phone.startsWith("400") || phone.startsWith("800")) && phone.length() ==10 ){
                return false;
            }
        if(phone.startsWith("95") &&  phone.length() <= 15 && phone.length() >= 5  ){
            return false;
        }
        if(phone.startsWith("0") &&  phone.length() <= 9 && phone.length() >= 8  ){
            return false;
        }
        if(phone.startsWith("1") &&  phone.length() <= 5 && phone.length() >= 3 ){
            return false;
        }
        if(phone.startsWith("1010") &&  phone.length() == 8 ){
            return false;
        }
        if(phone.startsWith("10086") ||  phone.startsWith("10001")  || phone.startsWith("10000")  || phone.startsWith("10010") ){
            return false;
        }
        if((phone.startsWith("13") ||  phone.startsWith("15")  || phone.startsWith("18")  || phone.startsWith("17")) &&  phone.length() ==11 ){
            return false;
        }
        if((phone.startsWith("8613") ||  phone.startsWith("8615")  || phone.startsWith("8618")  || phone.startsWith("8617")) &&  phone.length() ==13 ){
            return false;
        }
        if((phone.startsWith("+8613") ||  phone.startsWith("+8615")  || phone.startsWith("+8618")  || phone.startsWith("+8617")) &&  phone.length() ==14 ){
            return false;
        }
        return true;

    }
}
