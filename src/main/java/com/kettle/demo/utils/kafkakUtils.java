package com.kettle.demo.utils;


import com.google.common.collect.Lists;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;


public class kafkakUtils {

    public static FlinkKafkaProducer<String> getKafkaProducer(String kafkaipport, String topic) {

        //配置信息
        Properties prop = new Properties();
        //zk 地址
        prop.setProperty("bootstrap.servers", kafkaipport);
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.setProperty("auto.offset.reset", "latest");

        return new FlinkKafkaProducer<String>(kafkaipport, topic, new SimpleStringSchema());
    }

    public static void checkAndCreateTopic(String kafkaipport, String topicName) throws ExecutionException, InterruptedException {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaipport);
        AdminClient adminClient = AdminClient.create(adminProps);

        ListTopicsResult listTopics = adminClient.listTopics();
        Set<String> topicNames = listTopics.names().get();
        if (!topicNames.contains(topicName)) {
            // Topic不存在，创建Topic
            NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
            adminClient.createTopics(Lists.newArrayList(newTopic)).all().get();
            System.out.println("Topic " + topicName + "  Created");
        }
        adminClient.close();
    }


}
