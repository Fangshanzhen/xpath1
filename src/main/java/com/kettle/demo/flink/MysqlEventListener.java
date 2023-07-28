//package com.kettle.demo.flink;
//
//import com.ververica.newCDCUtils.connectors.mysql.MySqlSource;
//import com.ververica.newCDCUtils.connectors.mysql.table.StartupOptions;
//import com.ververica.newCDCUtils.debezium.DebeziumSourceFunction;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.springframework.boot.ApplicationArguments;
//import org.springframework.boot.ApplicationRunner;
//import org.springframework.stereotype.Component;
//
//
//@Component
//public class MysqlEventListener implements ApplicationRunner {
//
//    private final DataChangeSink dataChangeSink;
//
//    public MysqlEventListener(DataChangeSink dataChangeSink) {
//        this.dataChangeSink = dataChangeSink;
//    }
//
//    @Override
//    public  void run(ApplicationArguments args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        DebeziumSourceFunction<DataChangeInfo> dataChangeInfoMySqlSource = buildDataChangeSource();
//        DataStream<DataChangeInfo> streamSource = env
//                .addSource(dataChangeInfoMySqlSource, "mysql-source")
//                .setParallelism(1);
//        streamSource.addSink(dataChangeSink);
//        env.execute("mysql-stream-newCDCUtils");
//
//    }
//
//    /**
//     * 构造变更数据源
//     *
//     * @param
//     * @return DebeziumSourceFunction<DataChangeInfo>
//     * @author lei
//     * @date 2022-08-25 15:29:38
//     */
//    private DebeziumSourceFunction<DataChangeInfo> buildDataChangeSource() {
//        return MySqlSource.<DataChangeInfo>builder()
//                .hostname("127.0.0.1")
//                .port(3308)
//                .databaseList("test")
//                .tableList("test,test")
//                .username("root")
//                .password("123")
//
//                /**initial初始化快照,即全量导入后增量导入(检测更新数据写入)
//                 * latest:只进行增量导入(不读取历史变化)
//                 * timestamp:指定时间戳进行数据导入(大于等于指定时间错读取数据)
//                 */
//                .startupOptions(StartupOptions.latest())
//                .deserializer(new MysqlDeserialization())
//                .serverTimeZone("GMT+8")
//                .build();
//    }
//
//
//}
