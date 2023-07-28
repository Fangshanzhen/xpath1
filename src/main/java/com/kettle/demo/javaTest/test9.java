//package com.kettle.demo.javaTest;
//
//import com.alibaba.ververica.newCDCUtils.debezium.DebeziumDeserializationSchema;
//import com.kettle.demo.utils.kafkakUtils;
//import com.ververica.newCDCUtils.connectors.oracle.OracleSource;
//import com.ververica.newCDCUtils.connectors.oracle.table.StartupOptions;
//import com.ververica.newCDCUtils.debezium.JsonDebeziumDeserializationSchema;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
//import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
//
//import org.apache.flink.util.Collector;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.common.serialization.StringSerializer;
//import org.apache.kafka.connect.source.SourceRecord;
//
//import java.util.Properties;
//
//public class test9 {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(5000);
//
//        // Oracle CDC Source
//
//        Properties properties = new Properties();
//        properties.put("decimal.handling.mode", "double");
//        properties.put("database.url","jdbc:oracle:thin:@172.16.202.120:1521:orcl");
//        properties.setProperty("debezium.database.tablename.case.insensitive", "false");
//        properties.setProperty("debezium.log.mining.strategy", "online_catalog");
//        properties.setProperty("debezium.log.mining.continuous.mine", "true");
//        properties.setProperty("scan.startup.mode", "latest-offset");
//        SingleOutputStreamOperator<String> stream = env.addSource(OracleSource.<String>builder()
//                .hostname("172.16.202.120")
//                .port(1521)
//                .database("orcl") // monitor XE database
//                .schemaList("HUAYIN") // monitor inventory schema
//                .tableList("HUAYIN.TEST") // monitor products table
//                .username("huayin")
//                .password("huayin")
////                .startupOptions(StartupOptions.latest())
//                .debeziumProperties(properties)
//                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
//                .build())
//                .uid("oracle-source")
//                .setParallelism(1)
//                .map(myRecord -> {
//                    // Do some transformations (optional)
//                    return myRecord.toString();
//                })
//                .uid("my-transformation")
//                .setParallelism(1);
//
//
//        //打印到控制台
//        stream.print();
//
//        // Flink Kafka Producer
//
////        kafkakUtils.checkAndCreateTopic("10.0.108.51:9092", "test0620");
////        //将数据输出到kafka中
////        stream.addSink(kafkakUtils.getKafkaProducer("10.0.108.51:9092", "test0620"));
//
//        env.execute("Flink Oracle CDC");
//    }
//}
//
//class MyStringDeserializer implements DebeziumDeserializationSchema<String>, com.ververica.newCDCUtils.debezium.DebeziumDeserializationSchema<String> {
//    @Override
//    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
//        String s = sourceRecord.value().toString();
//        collector.collect(s);
//    }
//
//    @Override
//    public TypeInformation<String> getProducedType() {
//        return TypeInformation.of(String.class);
//    }
//}