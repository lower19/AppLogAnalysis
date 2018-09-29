package hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MonthBeginUDF extends UDF {
    // 计算本月的起始时间(毫秒数)
    public long evaluate() throws ParseException {
        return DateUtil.getMonthBeginTime(new Date()).getTime() ;
    }

    // 计算距离本月offset个月的月起始时间(毫秒数)
    public long evaluate(int offset) throws ParseException {
        return DateUtil.getMonthBeginTime(new Date(),offset).getTime();
    }

    // 计算某月的起始时间(毫秒数)
    public long evaluate(Date d) throws ParseException {
        return DateUtil.getMonthBeginTime(d).getTime();
    }

    // 计算距离指定月offset个月的月起始时间(毫秒数)
    public long evaluate(Date d,int offset) throws ParseException {
        return DateUtil.getMonthBeginTime(d,offset).getTime();
    }

    // 按照默认格式对输入的String类型日期进行解析，计算某月的起始时间 (毫秒数)
    public long evaluate(String dateStr) throws ParseException {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date d = sdf.parse(dateStr);

        return DateUtil.getMonthBeginTime(d).getTime();
    }

    // 按照默认格式对输入的String类型日期进行解析，计算距离某月offset个月之后的起始时间 (毫秒数)
    public long evaluate(String dateStr,int offset) throws ParseException {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date d = sdf.parse(dateStr);

        return DateUtil.getMonthBeginTime(d, offset).getTime();
    }

    // 按照指定的fmt格式对输入的String类型日期进行解析，计算某月的起始时间 (毫秒数)
    public long evaluate(String dateStr, String fmt) throws ParseException {

        SimpleDateFormat sdf = new SimpleDateFormat(fmt);
        Date d = sdf.parse(dateStr);

        return DateUtil.getMonthBeginTime(d).getTime();
    }

    // 按照指定的fmt格式对输入的String类型日期进行解析，计算距离某月offset月之后的起始时间(毫秒数)
    public long evaluate(String dateStr, String fmt,int offset) throws ParseException {

        SimpleDateFormat sdf = new SimpleDateFormat(fmt);
        Date d = sdf.parse(dateStr);

        return DateUtil.getMonthBeginTime(d, offset).getTime();
    }

}
