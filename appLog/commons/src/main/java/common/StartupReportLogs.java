package common;

public class StartupReportLogs extends BasicLog {
    private String appVersion;
    private Long startTimeInMs;
    private Long activeTimeInMs;
    private String city;

    public String getAppVersion() {
        return appVersion;
    }

    public void setAppVersion(String appVersion) {
        this.appVersion = appVersion;
    }

    public Long getStartTimeInMs() {
        return startTimeInMs;
    }

    public void setStartTimeInMs(Long startTimeInMs) {
        this.startTimeInMs = startTimeInMs;
    }

    public Long getActiveTimeInMs() {
        return activeTimeInMs;
    }

    public void setActiveTimeInMs(Long activeTimeInMs) {
        this.activeTimeInMs = activeTimeInMs;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }
}
