package cn.bithachi.demo.sql.udf;

import org.apache.spark.sql.api.java.UDF1;

/**
 * @Author: BitHachi
 * @Email: bithachi@163.com
 * @Date: 2022/8/25
 * @Description:
 */
public class PhoneUDF implements UDF1 {
    @Override
    public Object call(Object o) throws Exception {
        String phone = (String) o;
        String result = "手机号码错误";
        if (phone != null && phone.length() == 11) {
            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer.append(phone.substring(0, 3));
            stringBuffer.append("****");
            stringBuffer.append(phone.substring(7));
            result = stringBuffer.toString();
        }
        return result;
    }
}
