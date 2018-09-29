package client;

import com.alibaba.fastjson.JSONObject;
import common.StartupReportLogs;

import java.util.Random;

public class GenBehavior {
    // 随机数生成
    private static Random random = new Random();
    // 可选变量值
    private static String[] userIds = initUserIds();
    private static String[] userPlatforms = {"android", "ios"};
    private static String appId = "app00001";
    private static String[] appVersions = {"1.0.1", "1.0.2"};
    private static String[] cities = {"Beijing", "Shanghai", "Guangzhou", "Tianjin", "Hangzhou", "Shenzhen", "Hunan", "Xian", "Shenyang", "Xinjiang"};




    //获取用户id
    private static String[] initUserIds() {
        String base = "user11";
        String[] result = new String[100];
        for (int i = 0; i < 100; i++) {
            result[i] = base + i + "";
        }
        return result;
    }

    private static StartupReportLogs initAppStartupLogs(String userId, String userPlatform, Long beginTime) {
        StartupReportLogs appStartupLog = new StartupReportLogs();

        appStartupLog.setUserId(userId);
        appStartupLog.setAppPlatform(userPlatform);
        appStartupLog.setAppId(appId);

        appStartupLog.setAppVersion(appVersions[random.nextInt(appVersions.length)]);
        appStartupLog.setStartTimeInMs(beginTime);

        // APP的使用时间限制在半小时以内
        String timeMid = String.valueOf(random.nextInt(1200000));
        appStartupLog.setActiveTimeInMs(Long.valueOf(timeMid));

        String subUserId = userId.substring(userId.length() - 1);
        int cityDetermin = Integer.parseInt(subUserId);
        appStartupLog.setCity(cities[cityDetermin]);

        return appStartupLog;
    }

    public static void gather()  {
        try {
            for (int i = 1; i <= 200000000; i++) {

                String userId = userIds[random.nextInt(userIds.length - 1)];
                String userPlatform = userPlatforms[random.nextInt(userPlatforms.length)];

                // BeginTime生成
                Long beginTime = System.currentTimeMillis();

                StartupReportLogs appStartupLogs = initAppStartupLogs(userId, userPlatform, beginTime);


                String startupJson = JSONObject.toJSONString(appStartupLogs);
                UploadUtil.upload(startupJson, 0);


                Thread.sleep(1000);
            }
        } catch (Exception ex) {
            System.out.println(ex);
        }
   }

    public static void main(String[] args) {
        gather();
    }
}

