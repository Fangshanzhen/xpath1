//package com.kettle.demo.utils;
//
//
//import com.ververica.cdc.connectors.mysql.source.MySqlSource;
//import com.ververica.cdc.connectors.mysql.table.StartupOptions;
//import io.debezium.data.Envelope;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.configuration.RestOptions;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.kafka.connect.data.Schema;
//import org.apache.kafka.connect.data.Struct;
//import org.pentaho.di.core.logging.LogChannel;
//import org.pentaho.di.core.logging.LogChannelFactory;
//import com.alibaba.fastjson.JSONObject;
//import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
//import java.util.List;
//import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.util.Collector;
//import org.apache.kafka.connect.data.Field;
//
//import org.apache.kafka.connect.source.SourceRecord;
//
//import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
//import java.text.SimpleDateFormat;
//
//
//同样的代码在mysqlCDC中运行可以，在这里运行失败

//public class MySqlCDC {
//
//    private static final LogChannelFactory logChannelFactory = new org.pentaho.di.core.logging.LogChannelFactory();
//    private static final LogChannel kettleLog = logChannelFactory.create("MYSQL数据CDC增量");
//
//
//
//    public static void main(String[] args) throws Exception {
//
//        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
//                .hostname("127.0.0.1")
//                .port(3306)
//                .databaseList("test") // set captured database
//                .tableList("test.newtable") // set captured table
//                .username("root")
//                .password("123456")
//                .deserializer(new CustomDeserialization())
//                .startupOptions(StartupOptions.initial())
//                .build();
//
//        Configuration config = new Configuration();
//        config.setInteger(RestOptions.PORT, 8899);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
//
//        env.enableCheckpointing(5000);
//        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
//                .setParallelism(1) .addSink(new CustomSink())
//        ;
//        env.execute("flinkcdc");
//
//    }
//
//
//    public static class CustomDeserialization implements DebeziumDeserializationSchema<String> {
//
//
//        @Override
//        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
//
//            JSONObject res = new JSONObject();
//
//            // 获取数据库和表名称
//            String topic = sourceRecord.topic();
//            String[] fields = topic.split("\\.");
//            String database = fields[1];
//            String tableName = fields[2];
//
//            Struct value = (Struct) sourceRecord.value();
//            // 获取before数据
//            Struct before = value.getStruct("before");
//            JSONObject beforeJson = new JSONObject();
//            if (before != null) {
//                Schema beforeSchema = before.schema();
//                List<Field> beforeFields = beforeSchema.fields();
//                for (Field field : beforeFields) {
//                    Object beforeValue = before.get(field);
//                    beforeJson.put(field.name(), beforeValue);
//                }
//            }
//
//            // 获取after数据
//            Struct after = value.getStruct("after");
//            JSONObject afterJson = new JSONObject();
//            if (after != null) {
//                Schema afterSchema = after.schema();
//                List<Field> afterFields = afterSchema.fields();
//                for (Field field : afterFields) {
//                    Object afterValue = after.get(field);
//                    afterJson.put(field.name(), afterValue);
//                }
//            }
//
//            //获取操作类型 READ DELETE UPDATE CREATE
//            Envelope.Operation operation = Envelope.operationFor(sourceRecord);
//            String type = operation.toString().toLowerCase();
//            if ("create".equals(type)) {
//                type = "insert";
//            }
//
//            // 将字段写到json对象中
//            res.put("database", database);
//            res.put("tableName", tableName);
//            res.put("before", beforeJson);
//            res.put("after", afterJson);
//            res.put("type", type);
//
//            //输出数据
//            collector.collect(res.toString());
//            System.out.println("__________________" + res.toString());
//        }
//
//        @Override
//        public TypeInformation<String> getProducedType() {
//            return BasicTypeInfo.STRING_TYPE_INFO;
//        }
//    }
//
//
//        public static class CustomSink extends RichSinkFunction<String> {
//
//        @Override
//        public void invoke(String json, Context context) throws Exception {
//            //OP字段：该字段也有4种取值，分别是C（create），U（update）, D（delete）
//            //对于u操作，其数据部分包含了before和after
//            System.out.println("--------------------------------------" + json);
//        }
//
//        /**
//         * 打开连接
//         *
//         * @param parameters
//         * @throws Exception
//         */
//        @Override
//        public void open(Configuration parameters) throws Exception {
//
//        }
//
//        /**
//         * 关闭连接
//         *
//         * @throws Exception
//         */
//        @Override
//        public void close() throws Exception {
//
//        }
//
//    }
//
//
//}
//
//
