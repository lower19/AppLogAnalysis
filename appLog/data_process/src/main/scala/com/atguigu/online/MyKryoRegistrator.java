package com.atguigu.online;

import com.esotericsoftware.kryo.Kryo;
import common.StartupReportLogs;
import org.apache.spark.serializer.KryoRegistrator;

public class MyKryoRegistrator implements KryoRegistrator {
    public void registerClasses(Kryo kryo) {
        kryo.register(StartupReportLogs.class);
    }
}
