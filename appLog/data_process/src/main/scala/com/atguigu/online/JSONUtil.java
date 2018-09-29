package com.atguigu.online;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

public class JSONUtil {
    public static <T> T json2Object(String json,Class<T> clazz) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();

        return objectMapper.readValue(json,clazz);
    }
}
