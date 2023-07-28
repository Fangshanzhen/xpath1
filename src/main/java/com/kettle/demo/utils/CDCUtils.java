//package com.kettle.demo.utils;
//
//
//import java.io.IOException;
//import java.nio.charset.StandardCharsets;
//import java.sql.*;
//import java.util.Date;
//import java.text.SimpleDateFormat;
//import java.time.Duration;
//import java.util.*;
//
//import com.alibaba.fastjson.JSONObject;
//import com.alibaba.ververica.cdc.connectors.postgres.PostgreSQLSource;
//import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
//import com.kettle.demo.constant.Constant;
//import com.kettle.demo.response.logResponse;
//import com.kettle.demo.utils.ReadLogUtils;
//import com.kettle.demo.utils.ResultSetUtils1;
//import com.kettle.demo.utils.kafkakUtils;
////import com.ververica.newCDCUtils.connectors.oracle.OracleSource;
//import lombok.extern.slf4j.Slf4j;
//import oracle.jdbc.OracleConnection;
//import oracle.jdbc.pool.OracleDataSource;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.restartstrategy.RestartStrategies;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
//import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
//import org.apache.flink.connector.jdbc.JdbcSink;
//import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
//import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
//import org.apache.flink.streaming.connectors.kafka.table.KafkaOptions;
//import org.apache.flink.table.api.EnvironmentSettings;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.types.Row;
//import org.apache.flink.util.Collector;
//import org.apache.kafka.connect.data.Field;
//import org.apache.kafka.connect.source.SourceRecord;
//import org.pentaho.di.core.KettleEnvironment;
//import org.pentaho.di.core.database.Database;
//import org.pentaho.di.core.database.DatabaseMeta;
//import org.pentaho.di.core.exception.KettleException;
//import org.pentaho.di.core.logging.LogChannel;
//import org.pentaho.di.core.logging.LogChannelFactory;
//import org.pentaho.di.core.row.RowMetaInterface;
//import org.pentaho.di.core.row.ValueMetaInterface;
//import org.apache.flink.streaming.api.functions.sink.SinkFunction;
//import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
//import org.apache.flink.streaming.api.operators.StreamSink;
//import org.pentaho.di.trans.Trans;
//import org.pentaho.di.trans.TransHopMeta;
//import org.pentaho.di.trans.TransMeta;
//import org.pentaho.di.trans.step.StepMeta;
//import org.pentaho.di.trans.steps.tableinput.TableInputMeta;
//import org.pentaho.di.trans.steps.tableoutput.TableOutputMeta;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.PreparedStatement;
//import java.sql.SQLException;
//import java.util.Properties;
//import javax.naming.Context;
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.sql.Statement;
//
//import static com.kettle.demo.utils.incrementData1.checkTableExist;
//
///**
// * 使用前需要现在目标数据库中建立一张表，用来记录表名的时间，因为kafka会存很多以前的日志，全量的时候这些数据已经有了
// * <p>
// * CREATE TABLE `tabletime` (
// * `tablename` varchar(100) DEFAULT NULL,
// * `time` varchar(100) DEFAULT NULL
// * ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
// * <p>
// * <p>
// * UPDATE test.tabletime   SET time= null,tablename=null
// */
//
//@Slf4j
//public class CDCUtils {
//
//    private static final long DEFAULT_HEARTBEAT_MS = Duration.ofMinutes(10).toMillis();
//    private static final  LogChannelFactory logChannelFactory = new org.pentaho.di.core.logging.LogChannelFactory();
//    private static final  LogChannel kettleLog = logChannelFactory.create("数据CDC增量");
//
//    public static void incrementData(String originalDatabaseType, String originalDbname, String originalSchema, String originalIp, String originalPort,
//                                     String originalUsername, String originalPassword,
//                                     String targetDatabaseType, String targetDbname, String targetSchema, String targetIp, String targetPort,
//                                     String targetUsername, String targetPassword,
//                                     String tableList, //表名，多表以逗号隔开，//
//                                     String kafkaipport,
//                                     String topic
//    ) throws Exception {
//
//
//
//        KettleEnvironment.init();
//        DatabaseMeta originalDbmeta = null; //
//        DatabaseMeta targetDbmeta = null; //
//        try {
//
//            originalDbmeta = new DatabaseMeta(originalDbname, originalDatabaseType, "Native(JDBC)", originalIp, originalDbname, originalPort, originalUsername, originalPassword);
//            targetDbmeta = new DatabaseMeta(targetDbname, targetDatabaseType, "Native(JDBC)", targetIp, targetDbname, targetPort, targetUsername, targetPassword);
//            kettleLog.logBasic("源数据库、目标数据库连接成功！");
//        } catch (Exception e) {
//            log.error(e + "");
//        }
//
//        if (originalDbmeta != null && targetDbmeta != null) {
//            Database originalDatabase = new Database(originalDbmeta);
//            originalDatabase.connect();
//            Database targetDatabase = new Database(targetDbmeta);
//            targetDatabase.connect(); //连接数据库
//
//            try {
//                if (tableList != null) {//填入表名的
//                    List<String> allTableList = null;
//                    if (tableList.contains(",")) {
//                        allTableList = Arrays.asList(tableList.split(","));
//                    } else {
//                        allTableList = Collections.singletonList(tableList);
//                    }
//                    if (allTableList.size() > 0) {
//                        for (String table : allTableList) {
//                            String sql = null;
//                            if (originalDatabaseType.equals("ORACLE")) {
//                                sql = "select * from " + originalSchema + "." + table + " where rownum <=10 ";  //用sql来获取字段名及属性以便在目标库中创建表
//                            } else {
//                                sql = "select * from " + originalSchema + "." + table + "  limit 10;";
//                            }
//
//                            RowMetaInterface rowMetaInterface = originalDatabase.getQueryFieldsFromPreparedStatement(sql);
//
//                            String sql1 = targetDatabase.getDDLCreationTable(targetSchema + "." + table, rowMetaInterface);
//                            if (sql1.length() > 0) {
//                                if (!checkTableExist(targetDatabase, targetSchema, table)) {  //判断目标数据库中表是否存在
//                                    targetDatabase.execStatement(sql1.replace(";", ""));  //创建表
//                                    kettleLog.logBasic(table + " 创建输出表成功！");
//                                }
//                            }
//                            //输出表建好了
//
//                            //第一次全量数据
//                            String time;
//                            Statement tableTime = null;
//                            ResultSet resultSetlogPointerTime = null;
//                            String pointerSql = "SELECT time  FROM  " + targetSchema + ".tabletime  where tablename= '" + table + "'";
//                            List<String> pointList = new ArrayList<>();
//                            try {
//                                tableTime = executeSql(pointerSql, targetDatabase.getConnection());
//                                resultSetlogPointerTime = tableTime.executeQuery(pointerSql);
//                                if (resultSetlogPointerTime != null) {
//                                    pointList = ResultSetUtils1.allResultSet(resultSetlogPointerTime);
//                                }
//                                if (pointList == null || (pointList.size() == 0) || (pointList.size() > 0 && pointList.get(0) == null)
//                                        || (pointList.size() > 0 && pointList.get(0).equals("")) || (pointList.size() > 0 && pointList.get(0).equals("null"))) {
//                                    TransMeta transMeta = createTrans(table, originalDbmeta, targetDbmeta);
//                                    //第一次全量传数据 UPDATE test.tabletime   SET time= null,tablename=null
//                                    String dataSql = null;
//                                    if (tableList != null) {
//                                        dataSql = "select * from " + originalSchema + "." + table;
//                                    }
//                                    StepMeta step1 = createStep1(transMeta, dataSql, originalDbmeta);
//                                    transMeta.addStep(step1);
//                                    StepMeta step2 = createStep2(transMeta, targetDbmeta, targetSchema, table, "1");
//                                    transMeta.addStep(step2);
//                                    TransHopMeta TransHopMeta = createHop(step1, step2);
//                                    transMeta.addTransHop(TransHopMeta);
//                                    runTrans(transMeta);
//
//                                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//                                    Date now = new Date();
//                                    time = sdf.format(now);
//                                    String sqlNew = "INSERT INTO " + targetSchema + ".tabletime  (tablename,time) VALUES " + "('" + table + "'," + " '" + time + "')";//更新logpointer表，记录全量数据时的日志指针
//                                    tableTime = executeSql(sqlNew, targetDatabase.getConnection());
//                                    tableTime.execute(sqlNew);  //全量时的时间
//
//                                } else { //增量数据
//                                    time = pointList.get(0);
//
//                                    //创建执行环境
//                                    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//
//                                    SourceFunction<String> pgsqlSource=null;
//                                    //创建Flink-PgSQL-CDC的Source 读取生产环境pgsql数据库
//                                    if(originalDatabaseType.equals("POSTGRESQL")) {
//
//                                        Properties properties = new Properties();
//                                        properties.setProperty("snapshot.mode", "never");
//                                        properties.setProperty("debezium.slot.name", "pg_cdc");
//                                        properties.setProperty("debezium.slot.drop.on.stop", "true");
//                                        properties.setProperty("include.schema.changes", "true");
//                                        //使用连接器配置属性启用定期心跳记录生成
////                                        properties.setProperty("heartbeat.interval.ms", String.valueOf(DEFAULT_HEARTBEAT_MS));
//
//                                        pgsqlSource= PostgreSQLSource.<String>builder()
//                                                .hostname(originalIp)
//                                                .port(Integer.parseInt(originalPort))
//                                                .database(originalDbname) // monitor postgres database
//                                                .schemaList(originalSchema)  // monitor inventory schema
//                                                .tableList(originalSchema + "." + table) // monitor products table
//                                                .username(originalUsername)
//                                                .password(originalPassword)
//                                                //反序列化
//                                                .deserializer(new MyDebezium())
//                                                //标准逻辑解码输出插件
//                                                .decodingPluginName("pgoutput")
//                                                //配置
//                                                .debeziumProperties(properties)
//                                                .build();
//                                    }
//
//                                    //使用CDC Source从PgSQL读取数据
//                                    DataStreamSource<String> pgsqlDS = env.addSource(pgsqlSource);
//
//                                    //打印到控制台
//                                    pgsqlDS.print();
//
//                                    //判断topic存不存在， 创建topic
//                                    kafkakUtils.checkAndCreateTopic(kafkaipport, topic+"_"+table);
//                                    //将数据输出到kafka中
//                                    pgsqlDS.addSink(kafkakUtils.getKafkaProducer(kafkaipport, topic+"_"+table));
//
//                                    // 配置 Kafka 消费者
//                                    Properties consumerProps = new Properties();
//                                    consumerProps.setProperty("bootstrap.servers", kafkaipport);
//                                    //    consumerProps.setProperty("group.id", "flink-newCDCUtils-consumer");
//                                    consumerProps.setProperty("auto.offset.reset", "earliest");
//
//                                    FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic+"_"+table, new MyKafkaDeserializationSchema(), consumerProps);
//                                    kettleLog.logBasic( table+" 数据写入kafka成功！");
//                                    env.addSource(consumer)
//                                            // 转换为 JSON 对象
//                                            .map((MapFunction<String, String>) value -> {
//                                                // 在此处进行 JSON 字符串的解析，获取 CDC 事件详细信息
//                                                return value;
//                                            })
//                                            // 输出到CDCConsumer 实现中
//                                            .addSink(new MyCDCConsumer(time, targetSchema, table, targetDatabaseType, targetIp, targetPort, targetUsername, targetPassword, targetDbname));
//
//                                    //执行任务
//                                    env.execute();
//                                }
//
//                            } catch (Exception e) {
//                                throw new Exception("ERROR:   " + e);
//                            } finally {
//                                close(null, tableTime, resultSetlogPointerTime);
//                            }
//
//                        }
//                    }
//                }
//
//
//            } finally {
//                originalDatabase.disconnect();
//                targetDatabase.disconnect();
//            }
//
//        }
//    }
//
//
//
//
//
//
//
//
//
//
//    public static class MyCDCConsumer implements SinkFunction<String> {
//
//        private String time;
//        private String targetSchema;
//        private String table;
//        private String targetDatabaseType;
//        private String targetIp;
//        private String targetPort;
//        private String targetUsername;
//        private String targetPassword;
//        private String targetDbname;
//
//        public MyCDCConsumer(String time, String targetSchema, String table, String targetDatabaseType, String targetIp, String targetPort,
//                             String targetUsername, String targetPassword, String targetDbname
//        ) {
//            this.time = time;
//            this.targetSchema = targetSchema;
//            this.table = table;
//            this.targetDatabaseType = targetDatabaseType;
//            this.targetIp = targetIp;
//            this.targetPort = targetPort;
//            this.targetUsername = targetUsername;
//            this.targetPassword = targetPassword;
//            this.targetDbname = targetDbname;
//
//        }
//
//
//        @Override
//        public void invoke(String value, Context context) throws Exception {
//            JSONObject jsonObject = JSONObject.parseObject(value);
//            String tableName = jsonObject.getString("table");
//            String eventType = jsonObject.getString("operate_type");
//            JSONObject data = jsonObject.getJSONObject("sqlJson");
//            String operate_ms = jsonObject.getString("operate_ms");
//
//
//            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//            Date date = format.parse(time);
//            long longTime = date.getTime(); //全量时间
//            Date date1 = format.parse(operate_ms);
//            long longTime1 = date1.getTime(); //日志中的数据操作时间
//            if (longTime < longTime1) { //日志数据操作时间>全量的时间才进行增量操作，不然会重复
//
//                // 将解析出来的数据插入到下游数据库中
//                if ("INSERT".toLowerCase().equals(eventType)) {
//                    insertData(targetSchema, tableName, data);
//                } else if ("UPDATE".toLowerCase().equals(eventType)) {
//                    updateData(targetSchema, tableName, data);
//                } else if ("DELETE".toLowerCase().equals(eventType)) {
//                    deleteData(targetSchema, tableName, data);
//                }
//                Statement logPointerTime = null;
//                //更新时间
//                String sqlNew = "UPDATE " + targetSchema + ".tabletime  " + " SET time= " + "'" + operate_ms + "'" + " where tablename= '" + table + "'";
//                Connection conn = getConnection(targetDatabaseType, targetIp, targetPort, targetSchema, targetUsername, targetPassword, targetDbname);
//                logPointerTime = executeSql(sqlNew, conn);
//                logPointerTime.execute(sqlNew);
//                conn.close();
//            }
//        }
//
//        /**
//         * 写入 INSERT 事件到下游数据库
//         */
//        private void insertData(String database, String tableName, JSONObject data) throws SQLException {
//            Connection conn = getConnection(targetDatabaseType, targetIp, targetPort, targetSchema, targetUsername, targetPassword, targetDbname);
//            conn.setAutoCommit(false);
//            StringBuilder sql = new StringBuilder("INSERT INTO " + database + "." + tableName + " (");
//            StringBuilder value = new StringBuilder(" VALUES (");
//            if (data.size() > 0) {
//                for (String key : data.keySet()) {
//                    sql.append(key).append(",");
//                    value.append("?,");
//                }
//                sql.deleteCharAt(sql.length() - 1).append(")");
//                value.deleteCharAt(value.length() - 1).append(")");
//
//                PreparedStatement stmt = conn.prepareStatement(sql.toString() + "  " + value.toString());
//                int i = 1;
//                for (Object valueObj : data.values()) {
//                    stmt.setObject(i, valueObj);
//                    i++;
//                }
//
//                stmt.execute();
//                conn.commit();
//                conn.close();
//                kettleLog.logBasic("有数据新增！");
//            }
//        }
//
//        /**
//         * 写入 UPDATE 事件到下游数据库
//         */
//        private void updateData(String database, String tableName, JSONObject data) throws SQLException {
//            Connection conn = getConnection(targetDatabaseType, targetIp, targetPort, targetSchema, targetUsername, targetPassword, targetDbname);
//            assert conn != null;
//            conn.setAutoCommit(false);
//            List<String> values = new ArrayList<>();
//            StringBuilder sql = new StringBuilder("UPDATE " + database + "." + tableName + " SET ");  //set x=?, y=?
//            if (data.size() > 0) {
//                for (String key : data.keySet()) {
//                    if (!key.endsWith("@")) {
//                        sql.append(key).append("=?,");
//                        values.add((String) data.get(key));
//                    }
//                }
//                sql.deleteCharAt(sql.length() - 1);  //去掉最后的,
//                sql.append("  where  ");
//                for (String key : data.keySet()) {
//                    if (key.endsWith("@")) {
//                        sql.append(key.substring(0, key.length() - 1)).append("=? ");//把1去掉   where 后面是and  where x=? and y=?
//
//                        sql.append("and ");
//                        values.add((String) data.get(key));
//                    } else {
//                        continue;
//                    }
//                }
//                String newsql = sql.toString().trim();
//                if (newsql.endsWith("and")) {
//                    newsql = newsql.substring(0, newsql.length() - 3);
//                }
//
//                PreparedStatement stmt = conn.prepareStatement(newsql);
//                int i = 1;
//                for (Object valueObj : values) {
//                    stmt.setObject(i, valueObj);
//                    i++;
//                }
//
//                stmt.execute();
//                conn.commit();
//                conn.close();
//                kettleLog.logBasic("有数据更新！");
//            }
//        }
//
//        /**
//         * 写入 DELETE 事件到下游数据库
//         */
//        private void deleteData(String database, String tableName, JSONObject data) throws SQLException {
//            Connection conn = getConnection(targetDatabaseType, targetIp, targetPort, targetSchema, targetUsername, targetPassword, targetDbname);
//            assert conn != null;
//            conn.setAutoCommit(false);
//            List<String> values = new ArrayList<>();
//            StringBuilder sql = new StringBuilder("DELETE FROM " + database + "." + tableName + " WHERE "); //where x=? and y=?
//            if (data.size() > 0) {
//                for (String key : data.keySet()) {
//                    sql.append(key).append("=? ");
//                    sql.append("and ");
//                    values.add((String) data.get(key));  // ?中的赋值
//                }
//                String newsql = sql.toString().trim();
//
//                if (newsql.endsWith("and")) {
//                    newsql = newsql.substring(0, newsql.length() - 3);
//                }
//                PreparedStatement stmt = conn.prepareStatement(newsql);
//                int i = 1;
//                for (Object valueObj : values) {
//                    stmt.setObject(i, valueObj);
//                    i++;
//                }
//
//                stmt.execute();
//                conn.commit();
//                conn.close();
//                kettleLog.logBasic("有数据删除！");
//            }
//        }
//
//        /**
//         * 获取数据库连接对象
//         */
//        private static Connection getConnection(String targetDatabaseType, String targetIp, String targetPort, String targetSchema,
//                                                String targetUsername, String targetPassword, String targetDbname) throws SQLException {
//
//            if (targetDatabaseType.equals("MYSQL")) {
//                String url = "jdbc:mysql://" + targetIp + ":" + targetPort + "/" + targetDbname;
//                Properties props = new Properties();
//                props.setProperty("user", targetUsername);
//                props.setProperty("password", targetPassword);
//                return DriverManager.getConnection(url, props);
//            }
//
//            if (targetDatabaseType.equals("POSTGRESQL")) {
//                String url = "jdbc:postgresql://" + targetIp + ":" + targetPort + "/" + targetDbname + "?searchpath=" + targetSchema;
//                Properties props = new Properties();
//                props.setProperty("user", targetUsername);
//                props.setProperty("password", targetPassword);
//                return DriverManager.getConnection(url, props);
//            }
//
//            if (targetDatabaseType.equals("ORACLE")) {
//                String url = "jdbc:oracle:thin:@" + targetIp + ":" + targetPort + ":" + targetDbname;
//                Properties props = new Properties();
//                props.setProperty("user", targetUsername);
//                props.setProperty("password", targetPassword);
//                return DriverManager.getConnection(url, props);
//            }
//            return null;
//
//        }
//    }
//
//
//    public static class MyKafkaDeserializationSchema implements KafkaDeserializationSchema<String> {
//
//        @Override
//        public boolean isEndOfStream(String nextElement) {
//            return false;
//        }
//
//        @Override
//        public String deserialize(org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> record) throws Exception {
//            return new String(record.value(), StandardCharsets.UTF_8);
//        }
//
//        @Override
//        public TypeInformation<String> getProducedType() {
//            return TypeInformation.of(String.class);
//        }
//
//    }
//
//    private static TransMeta createTrans(String name, DatabaseMeta databaseMeta1, DatabaseMeta databaseMeta2) {
//        TransMeta transMeta = new TransMeta();
//        // 设置转化的名称
//        transMeta.setName(name);
//        // 添加转换的数据库连接
//        transMeta.addDatabase(databaseMeta1);
//        transMeta.addDatabase(databaseMeta2);
//        return transMeta;
//    }
//
//    private static TransHopMeta createHop(StepMeta step1, StepMeta step2) {
//        // 设置起始步骤和目标步骤，把两个步骤关联起来
//        return new TransHopMeta(step1, step2);
//    }
//
//    private static void runTrans(TransMeta transMeta) throws KettleException {
//        Trans trans = new Trans(transMeta);
//        trans.execute(null);// 执行转换
//        trans.waitUntilFinished(); // 等待转换执行结束
//    }
//
//    private static StepMeta createStep1(TransMeta transMeta, String sql, DatabaseMeta databaseMeta) {
//        // 新建一个表输入步骤(TableInputMeta)
//        TableInputMeta tableInputMeta = new TableInputMeta();
//        // 设置步骤1的数据库连接
//        tableInputMeta.setDatabaseMeta(databaseMeta);
//        // 设置步骤1中的sql
//        tableInputMeta.setSQL(sql);
//        // 设置步骤名称
//        return new StepMeta("表输入", tableInputMeta);
//    }
//
//    private static StepMeta createStep2(TransMeta transMeta, DatabaseMeta databaseMeta, String schema, String tableName, String type) {
//        // 新建一个表输出步骤(TableOutputMeta)
//        TableOutputMeta tableOutMeta = new TableOutputMeta();
//        // 设置步骤2的数据库连接
//        tableOutMeta.setDatabaseMeta(databaseMeta);
//        tableOutMeta.setCommitSize(1000);
//        tableOutMeta.setUseBatchUpdate(true); //批量插入
//        tableOutMeta.setTableName(tableName);
//        tableOutMeta.setSchemaName(schema);
//
//        if (type.equals("3")) {
//            tableOutMeta.setTruncateTable(true);  //输出表清空原有数据
//        } else {
//            tableOutMeta.setTruncateTable(false);
//        }
//
//        // 设置步骤名称
//        return new StepMeta("表输出", tableOutMeta);
//    }
//
//    private static void close(Connection connection, Statement statement, ResultSet resultSet) throws SQLException {
//
//        if (resultSet != null) {
//            resultSet.close();
//        }
//
//        if (statement != null) {
//            statement.close();
//        }
//    }
//
//    private static Statement executeSql(String sql, Connection connection) throws Exception {
//        Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
//        statement.setQueryTimeout(6000);
//        statement.setFetchSize(100000);
//        return statement;
//    }
//
//    public static class MyDebezium implements DebeziumDeserializationSchema<String> {
//        @Override
//        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
//
//            org.apache.kafka.connect.data.Struct dataRecord = (org.apache.kafka.connect.data.Struct) sourceRecord.value();
//
//            org.apache.kafka.connect.data.Struct afterStruct = dataRecord.getStruct("after");
//            org.apache.kafka.connect.data.Struct beforeStruct = dataRecord.getStruct("before");
//
//            JSONObject operateJson = new JSONObject();
//
//            //操作的sql字段json数据
//            JSONObject sqlJson = new JSONObject();
//
//            //操作类型
//            String operate_type = "";
//
//            List<Field> fieldsList = null;
//            List<Field> beforeStructList = null;
//
//            if (afterStruct != null && beforeStruct != null) {
//                System.out.println("这是修改数据");
//                operate_type = "update";
//                fieldsList = afterStruct.schema().fields();
//                for (Field field : fieldsList) {
//                    String fieldName = field.name();
//                    Object fieldValue = afterStruct.get(fieldName);
//                    sqlJson.put(fieldName, fieldValue);
//                }
//                beforeStructList = beforeStruct.schema().fields();
//                for (Field field : beforeStructList) {
//                    String fieldName = field.name();
//                    Object fieldValue = beforeStruct.get(fieldName);
//                    sqlJson.put(fieldName + "@", fieldValue);
//                }
//
//            } else if (afterStruct != null) {
//                System.out.println("这是新增数据");
//                operate_type = "insert";
//                fieldsList = afterStruct.schema().fields();
//                for (Field field : fieldsList) {
//                    String fieldName = field.name();
//                    Object fieldValue = afterStruct.get(fieldName);
//                    sqlJson.put(fieldName, fieldValue);
//                }
//            } else if (beforeStruct != null) {
//                System.out.println("这是删除数据");
//                operate_type = "delete";
//                fieldsList = beforeStruct.schema().fields();
//                for (Field field : fieldsList) {
//                    String fieldName = field.name();
//                    Object fieldValue = beforeStruct.get(fieldName);
//                    sqlJson.put(fieldName, fieldValue);
//                }
//            } else {
//                kettleLog.logBasic("-----------数据无变化-------------");
//            }
//
//            operateJson.put("sqlJson", sqlJson);
//
//            org.apache.kafka.connect.data.Struct source = dataRecord.getStruct("source");
//
//            //操作的数据库名
//            String database = source.getString("db");
//
//            //操作的表名
//            String table = source.getString("table");
//
//            //操作的时间戳（单位：毫秒）
//            Object operate_ms = source.get("ts_ms");
//            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//            String date = sdf.format(new java.sql.Date((Long) operate_ms));
//
//            operateJson.put("database", database);
//            operateJson.put("table", table);
//            operateJson.put("operate_ms", date);
//            operateJson.put("operate_type", operate_type);
//
//            String topic = sourceRecord.topic();
//            System.out.println("topic = " + topic);
//            collector.collect(String.valueOf(operateJson));
//            kettleLog.logBasic("operateJson:   "+ operateJson);
//        }
//
//        @Override
//        public TypeInformation<String> getProducedType() {
//            return BasicTypeInfo.STRING_TYPE_INFO;
//        }
//
//    }
//
//}