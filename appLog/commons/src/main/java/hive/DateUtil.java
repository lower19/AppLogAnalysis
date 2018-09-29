package hive;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {

    //得到指定天的零点时刻
    public static Date getDayBeginTime(Date date) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd 00:00:00");
            return sdf.parse(sdf.format(date));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    //得到指定date的偏移量零时刻
    public static Date getDayBeginTime(Date d, int offset) {
        try {
            // 通过SimpleDateFormat获得指定日期的零时刻
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd 00:00:00");
            Date beginDate = sdf.parse(sdf.format(d));

            Calendar calendar = Calendar.getInstance();
            calendar.setTime(beginDate);
            // 通过Calendar实例实现按照天数偏移
            calendar.add(Calendar.DAY_OF_MONTH, offset);

            return calendar.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    //得到指定date所在周的起始时刻
    public static Date getWeekBeginTime(Date date) {
        try {
            Date beginTime = getDayBeginTime(date);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(beginTime);

            //得到当前日期是所在周的第几天
            int n = calendar.get(Calendar.DAY_OF_WEEK);
            calendar.add(Calendar.DAY_OF_MONTH, -(n - 1));

            return calendar.getTime();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 得到距离指定date所在周offset周之后的一周的起始时刻.
     */
    public static Date getWeekBeginTime(Date d, int offset) {

        try {
            //得到d的零时刻
            Date beginDate = getDayBeginTime(d);
            Calendar c = Calendar.getInstance();
            c.setTime(beginDate);
            int n = c.get(Calendar.DAY_OF_WEEK);

            //定位到本周第一天
            c.add(Calendar.DAY_OF_MONTH, -(n - 1));
            //通过Calendar实现按周进行偏移
            c.add(Calendar.DAY_OF_MONTH, offset * 7);

            return c.getTime();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    //得到指定date所在月的起始时刻
    public static Date getMonthBeginTime(Date date) {
        try {
            Date dayBeginTime = getDayBeginTime(date);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/01 00:00:00");

            return sdf.parse(sdf.format(dayBeginTime));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 得到距离指定date所在月offset个月之后的月的起始时刻.
     */
    public static Date getMonthBeginTime(Date d, int offset) {

        try {
            //得到d的零时刻
            Date beginDate = getDayBeginTime(d);
//得到date所在月的第一天的零时刻
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/01 00:00:00");

            //d所在月的第一天的零时刻
            Date firstDay = sdf.parse(sdf.format(beginDate));

            Calendar c = Calendar.getInstance();
            c.setTime(firstDay);

            //通过Calendar实现按月进行偏移
            c.add(Calendar.MONTH, offset);

            return c.getTime();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

}
