package com.atguigu.offline;


import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.*;

public class KafkaConsumer {
    private static Configuration configuration = new Configuration();
    private static FileSystem fs = null;
    private static FSDataOutputStream outputStream = null;
    private static Path writePath = null;
    private static String hdfsBasicPath = "hdfs://hadoop102:8020/user/atguigu/appLog/";

    public static void main(String[] args) {
        // 创建配置
        Properties properties = new Properties();
        properties.put("zookeeper.connect", "192.168.1.102:2181");
        properties.put("group.id", "group1");
        properties.put("zookeeper.session.timeout.ms", "1000");
        properties.put("zookeeper.sync.time.ms", "250");
        properties.put("auto.commit.interval.ms", "1000");

        // 创建消费者连接器
        // 消费者客户端会通过消费者连接器（ConsumerConnector）连接ZK集群
        // 获取分配的分区，创建每个分区对应的消息流（MessageStream），最后迭代消息流，读取每条消息
        ConsumerConnector connector = Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));

        try {
            fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration, "atguigu");
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 创建HashMap结构，用于指定消费者订阅的主题和每个主题需要的消费者线程数
        // Key：主题名称   Value：消费线程个数
        // 一个消费者可以设置多个消费者线程，一个分区只会被分配给一个消费者线程
        HashMap<String, Integer> topicCount = new HashMap<>();

        // 消费者采用多线程访问的分区都是隔离的，所以不会出现一个分区被不同线程同时访问的情况
        // 在上述线程模型下，消费者连接器负责处理分区分配和拉取消息

        // 每一个Topic至少需要创建一个Consumer thread
        // 如果有多个Partitions，则可以创建多个Consumer thread线程
        // Consumer thread数量 > Partitions数量，会有Consumer thread空闲

        // 设置每个主题的线程数量
        // 设置analysis-test的线程数量为1
        topicCount.put("analysis-test", 1);

        // 每个消费者线程都对应了一个消息流
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = connector.createMessageStreams(topicCount);

        // 消费者迭代器，从消息流中取出数据
        // consumerMap.get("analysis-test")获取analysis-test主题的所有数据流
        // 由于只有一个线程，只有一个消息流，因此get(0)获取这个唯一的消息流
        KafkaStream<byte[], byte[]> stream = consumerMap.get("analysis-test").get(0);

        // 获取MessageStream中的数据迭代器
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        // 获取当前时间
        Long lastTime = System.currentTimeMillis();

        // 获取数据写入全路径（yyyyMM/dd/HHmm）
        String totalPath = getTotalPath(lastTime);

        // 根据路径创建Path对象
        writePath = new Path(totalPath);
        // 创建文件流
        try{
            if (fs.exists(writePath)) {
                outputStream = fs.append(writePath);
            } else {
                outputStream = fs.create(writePath, true);
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        while (it.hasNext()) {
            // 收集两分钟的数据后更换目录
            if (System.currentTimeMillis() - lastTime > 6000) {
                try{
                    outputStream.close();
                    // 重新获取时间，更新目录
                    Long currentTime = System.currentTimeMillis();
                    String newPath = getTotalPath(currentTime);
                    writePath = new Path(newPath);
                    outputStream = fs.create(writePath);
                    lastTime = currentTime;
                }catch (Exception e){
                    e.printStackTrace();
                }
            }

            String jasonStr = new String(it.next().message());

            if (jasonStr.contains("appVersion")) {
                System.out.println("startupPage");
                save(jasonStr);
            } else if (jasonStr.contains("currentPage")) {
                System.out.println("PageLog");
            } else {
                System.out.println("ErrorLog");
            }
        }

    }

    private static void save(String log) {
        try {
            // 将日志内容写入HDFS文件系统
            String logEnd = log + "\r\n";
            outputStream.write(logEnd.getBytes());
            // 一致性模型
            outputStream.hflush();
            outputStream.hsync();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //将Long类型的日期转变为yyyyMM-dd-HHmm的格式
    private static String timeTransform(Long timeInmills) {
        Date date = new Date(timeInmills);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM-dd-HHmm");
        String formatDate = sdf.format(timeInmills);

        return formatDate;
    }

    private static String getDirectoryFromDate(String date) {
        // yyyyMM-dd
        String[] directories = date.split("-");
        // yyyyMM/dd/
        String directory = directories[0] + "/" + directories[1];
        return directory;
    }

    private static String getFileName(String date){
        // HHmm
        String[] dateSplit = date.split("-");
        // HHmm
        String fileName = dateSplit[2];
        return fileName;
    }

    private static String getTotalPath(Long lastTime){
        // 时间格式转换（yyyyMM-dd-HHmm）
        String formatDate = timeTransform(lastTime);
        // 提取目录（yyyyMM/dd/）
        String directory = getDirectoryFromDate(formatDate);
        // 提取文件名称（HHmm）
        String fileName = getFileName(formatDate);
        // 全路径（yyyyMM/dd/HHmm）
        String totalPath = hdfsBasicPath + directory + "/" + fileName;

        return totalPath;
    }
}