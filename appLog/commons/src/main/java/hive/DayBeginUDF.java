package hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DayBeginUDF extends UDF {
    // 获取当天的起始时刻(毫秒数)
    public long evaluate() throws ParseException {
        return evaluate(new Date());
    }

    // 计算距离当天offset天之后的起始时间(毫秒数)
    public long evaluate(int offset) throws ParseException {
        return DateUtil.getDayBeginTime(new Date(), offset).getTime();
    }

    // 计算某天的起始时间 (毫秒数)
    public long evaluate(Date d) throws ParseException {
        return DateUtil.getDayBeginTime(d).getTime();
    }

    // 计算距离某天offset天之后的起始时间 (毫秒数)
    public long evaluate(Date d, int offset) throws ParseException {
        return DateUtil.getDayBeginTime(d, offset).getTime();
    }

    // 按照默认格式对输入的String类型日期进行解析，计算某天的起始时间 (毫秒数)
    public long evaluate(String dateStr) throws ParseException {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date d = sdf.parse(dateStr);

        return evaluate(d);
    }

    // 按照默认格式对输入的String类型日期进行解析，计算距离某天offset天之后的起始时间 (毫秒数)
    public long evaluate(String dateStr, int offset) throws ParseException {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date d = sdf.parse(dateStr);

        return DateUtil.getDayBeginTime(d, offset).getTime();
    }

    // 按照指定的fmt格式对输入的String类型日期进行解析，计算某天的起始时间 (毫秒数)
    public long evaluate(String dateStr, String fmt) throws ParseException {

        SimpleDateFormat sdf = new SimpleDateFormat(fmt);
        Date d = sdf.parse(dateStr);

        return DateUtil.getDayBeginTime(d).getTime();
    }

    // 按照指定的fmt格式对输入的String类型日期进行解析，计算距离某天offset天之后的起始时间(毫秒数)
    public long evaluate(String dateStr, String fmt, int offset) throws ParseException {

        SimpleDateFormat sdf = new SimpleDateFormat(fmt);
        Date d = sdf.parse(dateStr);

        return DateUtil.getDayBeginTime(d, offset).getTime();
    }

}
