package com.atguigu.controller;

import com.alibaba.fastjson.JSON;

import com.atguigu.common.StartupReportLogs;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;

@Controller
@RequestMapping("/logs")
public class LogCollector {
    /**
     * 地理信息缓存
     */
    private static final Logger logger = Logger.getLogger(LogCollector.class);

    @RequestMapping(value = "/startupLogs", method = RequestMethod.POST)
    @ResponseBody
    public StartupReportLogs startupCollect(@RequestBody StartupReportLogs e, HttpServletRequest req) {

        String LogString = JSON.toJSONString(e);

        logger.info(LogString);

        return e;
    }
}
