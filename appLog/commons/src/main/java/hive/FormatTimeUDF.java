package hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class FormatTimeUDF extends UDF {
    // 根据输入的时间毫秒值（long类型）和格式化要求，返回时间（String类型）
    public String evaluate(long ms,String fmt) throws ParseException {

        SimpleDateFormat sdf = new SimpleDateFormat(fmt) ;
        Date d = new Date();
        d.setTime(ms);

        return sdf.format(d) ;
    }

    // 根据输入的时间毫秒值（String类型）和格式化要求，返回时间（String类型）
    public String evaluate(String ms,String fmt) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(fmt) ;
        Date d = new Date();

        d.setTime(Long.parseLong(ms));

        return sdf.format(d) ;

    }

    // 根据输入的时间毫秒值（long类型）、格式化要求，返回当前周的起始时间（String类型）
    public String evaluate(long ms ,String fmt, int week) throws ParseException {

        Date d = new Date();
        d.setTime(ms);

        // 获取周内第一天
        Date firstDay = DateUtil.getWeekBeginTime(d) ;
        SimpleDateFormat sdf = new SimpleDateFormat(fmt);

        return sdf.format(firstDay) ;
    }


}
