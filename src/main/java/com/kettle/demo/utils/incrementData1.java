package com.kettle.demo.utils;

import com.kettle.demo.newUpdate.newUpdateMeta;
import com.kettle.demo.response.logResponse;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONException;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelFactory;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;
import org.pentaho.di.trans.steps.tableoutput.TableOutputMeta;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 最新代码，用于非读日志式增量  1 为时间戳  3为全量
 */

@Slf4j
public class incrementData1 {
    public static String incrementData(String originalDatabaseType, String originalDbname, String originalSchema, String originalIp, String originalPort,
                                       String originalUsername, String originalPassword,
                                       String targetDatabaseType, String targetDbname, String targetSchema, String targetIp, String targetPort,
                                       String targetUsername, String targetPassword,
                                       String type,     //1 为时间戳  3为全量
                                       String condition, //当为1时填时间戳字段；当为2是填日志路径
                                       String tableList, //表名，多表以逗号隔开，//
                                       String tableSql,// 加上支持sql的功能,以;结束  注意写sql语句的时候一定要加schema.
                                       String index, //索引字段名 ipid,pid 复合索引以逗号隔开
                                       String indexName,  //索引名称
                                       String etlTime,  //没有时间戳的时候，增加的时间戳字段，
                                       String keyLookup  //用于插入或更新用于比较的字段，多个以逗号隔开



    ) throws Exception {

        LogChannelFactory logChannelFactory = new org.pentaho.di.core.logging.LogChannelFactory();
        LogChannel kettleLog = logChannelFactory.create("数据传输");
        KettleEnvironment.init();
        DatabaseMeta originalDbmeta = null; //
        DatabaseMeta targetDbmeta = null; //


        try {

            originalDbmeta = new DatabaseMeta(originalDbname, originalDatabaseType, "Native(JDBC)", originalIp, originalDbname, originalPort, originalUsername, originalPassword);
            targetDbmeta = new DatabaseMeta(targetDbname, targetDatabaseType, "Native(JDBC)", targetIp, targetDbname, targetPort, targetUsername, targetPassword);
            kettleLog.logBasic("源数据库、目标数据库连接成功！");
        } catch (Exception e) {
            throw new Exception("ERROR:   " + e);
        }


        Database originalDatabase = new Database(originalDbmeta);
        originalDatabase.connect();
        Database targetDatabase = new Database(targetDbmeta);
        targetDatabase.connect(); //连接数据库

        try {
            if (tableList != null && tableSql == null) {//填入表名的
                List<String> allTableList = null;
                if (tableList.contains(",")) {
                    allTableList = Arrays.asList(tableList.split(","));
                } else {
                    allTableList = Collections.singletonList(tableList);
                }
                if (allTableList.size() > 0) {
                    for (String table : allTableList) {
                        String sql = null;
                        if (originalDatabaseType.equals("ORACLE")) {
                            sql = "select * from " + originalSchema + "." + table + " where rownum <=10 ";  //用sql来获取字段名及属性以便在目标库中创建表
                        } else {
                            sql = "select * from " + originalSchema + "." + table + "  limit 10;";
                        }

                        RowMetaInterface rowMetaInterface = originalDatabase.getQueryFieldsFromPreparedStatement(sql);

                        List<ValueMetaInterface> valueMetaInterfaces = rowMetaInterface.getValueMetaList();
                        for (ValueMetaInterface v : valueMetaInterfaces) {
                            if (condition != null && type.equals("1")) {
                                if (v.getName().toLowerCase().equals(condition.toLowerCase())) {  //适用于时间戳增量，将时间戳字段改为字符型,会增加时区后面多个+08
                                    v.setType(ValueMetaInterface.TYPE_STRING); //String
                                    v.setLength(100); //字段长度
                                    v.setConversionMask("yyyy-MM-dd HH:mm:ss");

                                }
                            }
                        }
                        String sql1 = targetDatabase.getDDLCreationTable(targetSchema + "." + table, rowMetaInterface);


                        if (type.equals("2") || type.equals("3")) { //类型为2 或3的时候  建表sql上加上时间戳字段
                            if (etlTime.length() > 0) {
                                int a = sql1.lastIndexOf(")"); //最后一个)
                                if (a > 0) {
                                    sql1 = sql1.replace(sql1.substring(a), "");
                                }
                                sql1 = sql1 + ",";
                                sql1 = sql1 + etlTime + "  " + "TIMESTAMP " + " NOT NULL DEFAULT CURRENT_TIMESTAMP";
                                sql1 = sql1 + Const.CR + ");";

                            }
                        }

                        if (sql1.length() > 0) {
                            if (!checkTableExist(targetDatabase, targetSchema, table)) {  //判断目标数据库中表是否存在
                                sameCreate(table, sql1, rowMetaInterface, originalDbmeta, originalDatabaseType, targetDatabaseType, targetSchema, targetDatabase, index, indexName);
                                kettleLog.logBasic(table + " 创建输出表成功！");
                            }
                        }

                        if (type.equals("1")) {//类型为1 有时间戳字段
                            type1(table, condition, originalDbmeta, originalSchema, originalDatabaseType, targetDbmeta, targetSchema, targetDatabase, tableList, null, keyLookup, rowMetaInterface);
                        }
                        if (type.equals("3")) {//类型为3，表示每次进行删除后全量
                            type3(table, originalDbmeta, originalSchema, targetDbmeta, targetDatabase, targetSchema, tableList, null, keyLookup, etlTime, rowMetaInterface);
                        }
//                            if (type.equals("2")) {//类型为2，读取日志获取数据
//                                type2(table, condition, originalDatabaseType, originalSchema, originalDbmeta, targetSchema, targetDatabaseType, targetDatabase, targetDbmeta, tableList, null);
//                            }
                        kettleLog.logBasic(table + " 完成数据传输！");
                    }
                }
            }
            if (tableList == null && tableSql != null) {//填入的是sql语句 ,多个sql语句用;隔开
                String[] sqlList;
                if (tableSql.contains(";")) {
                    sqlList = tableSql.split(";");
                } else {
                    sqlList = new String[]{tableSql};
                }
                for (String sql : sqlList) {

                    if (sql.toLowerCase().contains("select") && sql.toLowerCase().contains("from") && sql.toLowerCase().contains((originalSchema + ".").toLowerCase())) {
                        int a = sql.toLowerCase().indexOf((originalSchema + ".").toLowerCase());
                        a = a + originalSchema.length() + 1;
                        String table = sql.substring(a).trim();

                        RowMetaInterface rowMetaInterface = originalDatabase.getQueryFieldsFromPreparedStatement(sql);

                        List<ValueMetaInterface> valueMetaInterfaces = rowMetaInterface.getValueMetaList();
                        for (ValueMetaInterface v : valueMetaInterfaces) {
                            if (condition != null && type.equals("1")) {
                                if (v.getName().toLowerCase().equals(condition.toLowerCase())) {  //适用于时间戳增量，将时间戳字段改为字符型
                                    v.setType(ValueMetaInterface.TYPE_STRING); //String
                                    v.setLength(100); //字段长度
                                    v.setConversionMask("yyyy-MM-dd HH:mm:ss");
                                }
                            }
                        }

                        String sql1 = targetDatabase.getDDLCreationTable(targetSchema + "." + table, rowMetaInterface);

                        if (type.equals("2") || type.equals("3")) { //类型为2 或3的时候  建表sql上加上时间戳字段
                            if (etlTime.length() > 0) {
                                int aa = sql1.lastIndexOf(")"); //最后一个)
                                if (aa > 0) {
                                    sql1 = sql1.replace(sql1.substring(aa), "");
                                }
                                sql1 = sql1 + ",";
                                sql1 = sql1 + etlTime + "  " + "TIMESTAMP " + " NOT NULL DEFAULT CURRENT_TIMESTAMP";
                                sql1 = sql1 + Const.CR + ");";

                            }
                        }


                        if (sql1.length() > 0) {
                            if (!checkTableExist(targetDatabase, targetSchema, table)) {  //判断目标数据库中表是否存在
                                sameCreate(table, sql1, rowMetaInterface, originalDbmeta, originalDatabaseType, targetDatabaseType, targetSchema, targetDatabase, index, indexName);
                                kettleLog.logBasic(table + " 创建输出表成功！");
                            }
                        }
                        if (type.equals("1") && condition != null) {//有时间戳字段
                            type1(table, condition, originalDbmeta, originalSchema, originalDatabaseType, targetDbmeta, targetSchema, targetDatabase, null, sql, keyLookup, rowMetaInterface);
                        }
                        if (type.equals("3")) {//类型为3，表示每次进行删除后全量
                            type3(table, originalDbmeta, originalSchema, targetDbmeta, targetDatabase, targetSchema, null, sql, keyLookup, etlTime, rowMetaInterface);
                        }
//                            if (type.equals("2")) {//类型为2，读取日志获取数据
//                                type2(table, condition, originalDatabaseType, originalSchema, originalDbmeta, targetSchema, targetDatabaseType, targetDatabase, targetDbmeta, null, sql);
//                            }
                    }
                }

            }

        } catch (Exception e) {
            throw new Exception("ERROR：   " + e);
        } finally {
            originalDatabase.disconnect();
            targetDatabase.disconnect();
        }


        return null;
    }


    private static void type3(String table, DatabaseMeta originalDbmeta, String originalSchema, DatabaseMeta targetDbmeta, Database targetDatabase, String targetSchema,
                              String tableList, String tableSql, String keyLookup, String etlTime, RowMetaInterface rowMetaInterface) throws KettleException {
        LogChannelFactory logChannelFactory = new org.pentaho.di.core.logging.LogChannelFactory();
        LogChannel kettleLog = logChannelFactory.create("type3：全量插入或更新数据");


        TransMeta transMeta = createTrans(table, originalDbmeta, targetDbmeta);

        String dataSql = null;
        if (tableList != null && tableSql == null) {
            dataSql = "select * from " + originalSchema + "." + table;
        }
        if (tableList == null && tableSql != null) {
            dataSql = tableSql;
        }
        // 创建步骤1并添加到转换中
        StepMeta step1 = createStep1(transMeta, dataSql, originalDbmeta);
        transMeta.addStep(step1);

        // 判断表是否有数据
        RowMetaAndData rowMetaAndData = targetDatabase.getOneRow("SELECT COUNT(*) FROM  " + targetSchema + "." + table);

        StepMeta step2 = null;
        if ((Long) rowMetaAndData.getData()[0] > 0) { //表有数据，插入/更新
            step2 = createStep3(transMeta, targetDbmeta, targetSchema, table, keyLookup, etlTime, rowMetaInterface);
            transMeta.addStep(step2);
            kettleLog.logBasic("插入/更新");
        } else { //表没有数据，表输出
            step2 = createStep2(transMeta, targetDbmeta, targetSchema, table, "3");
            transMeta.addStep(step2);
            kettleLog.logBasic("表输出");
        }

        // 创建hop连接并添加hop
        TransHopMeta TransHopMeta = createHop(step1, step2);
        transMeta.addTransHop(TransHopMeta);
        runTrans(transMeta);

    }

    private static void type1(String table, String condition, DatabaseMeta originalDbmeta, String originalSchema, String originalDatabaseType, DatabaseMeta targetDbmeta, String targetSchema, Database targetDatabase,
                              String tableList, String tableSql, String keyLookup, RowMetaInterface rowMetaInterface) throws KettleException, SQLException, JSONException {


        LogChannelFactory logChannelFactory = new org.pentaho.di.core.logging.LogChannelFactory();
        LogChannel kettleLog = logChannelFactory.create("type1：根据时间戳进行增量--");

        String maxTime = null;
        String maxTimeSql = "select  max(" + condition + ") from  " + targetSchema + "." + table;
        ResultSet resultSet = targetDatabase.openQuery(maxTimeSql);
        if (resultSet != null) {
            List<String> timeList = ResultSetUtils.allResultSet(resultSet);
            if (timeList != null && timeList.size() > 0) {
                maxTime = timeList.get(0);
                if (maxTime.length() > 20) {    //+08是时区，kettle时间类型转字符型时会加上+08  2023-07-11 00:00:00+08去掉+08   2021-01-11 00:00:00.0 去掉.0
                    maxTime = maxTime.substring(0, 19);                     //  不同类型数据库，时间同步过去会有异常
                }
            } else {
                maxTime = "1800-01-01 00:00:00";  //设置第一次起始时间
            }
        }
        assert maxTime != null;
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        // 将字符串转为 LocalDateTime
        LocalDateTime datetime = LocalDateTime.parse(maxTime, formatter);
        // 在 LocalDateTime 上加一秒
        datetime = datetime.plusSeconds(1);
        // 将 maxTime 转回字符串
        maxTime = formatter.format(datetime);   //把maxTime格式固定yyyy-MM-dd HH:mm:ss

        TransMeta transMeta = createTrans(table, originalDbmeta, targetDbmeta);
        // 创建步骤1并添加到转换中
        String dataSql = null;

        if (tableList != null && tableSql == null) {
            if (originalDatabaseType.equals("ORACLE")) {
                dataSql = "select * from " + originalSchema + "." + table + "  where " + condition + " >= " + "TO_TIMESTAMP('" + maxTime + "','YYYY-MM-DD HH24:MI:SS')"; //TO_TIMESTAMP根据时间戳字段类型修改
            } else {
                dataSql = "select * from " + originalSchema + "." + table + "  where " + condition + " >= " + "'" + maxTime + "'";
            }
        }
        if (tableList == null && tableSql != null) {
            dataSql = tableSql;
            if (originalDatabaseType.equals("ORACLE")) {
                dataSql = dataSql.replace(";", "") + "  where " + condition + " >= " + "TO_TIMESTAMP('" + maxTime + "','YYYY-MM-DD HH24:MI:SS')";
            } else {
                dataSql = dataSql.replace(";", "") + "  where " + condition + " >=" + "'" + maxTime + "'";
            }
        }

        kettleLog.logBasic("增量sql：  " + dataSql);
        StepMeta step1 = createStep1(transMeta, dataSql, originalDbmeta);
        transMeta.addStep(step1);

        // 判断表是否有数据
        RowMetaAndData rowMetaAndData = targetDatabase.getOneRow("SELECT COUNT(*) FROM  " + targetSchema + "." + table);
        // 创建步骤2并添加到转换中
        StepMeta step2 = null;
        if ((Long) rowMetaAndData.getData()[0] > 0) { //表有数据，插入/更新
            step2 = createStep3(transMeta, targetDbmeta, targetSchema, table, keyLookup, condition, rowMetaInterface);
            transMeta.addStep(step2);
            kettleLog.logBasic("插入/更新");
        } else { //表没有数据，表输出 不清空原数据
            step2 = createStep2(transMeta, targetDbmeta, targetSchema, table, "1");
            transMeta.addStep(step2);
            kettleLog.logBasic("表输出");
        }

        transMeta.addStep(step2);
        // 创建hop连接并添加hop
        TransHopMeta TransHopMeta = createHop(step1, step2);
        transMeta.addTransHop(TransHopMeta);
        runTrans(transMeta);
    }


    public static void sameCreate(String table, String sql1, RowMetaInterface rowMetaInterface, DatabaseMeta originalDbmeta, String originalDatabaseType, String targetDatabaseType, String targetSchema, Database targetDatabase, String index, String indexName) throws KettleDatabaseException {
        /**
         * 新建索引语句，对于text、longtext类型的字段建索引需要指定长度，否则会报错
         */
        if (sql1.toLowerCase().contains("create")) {
            if (indexName!=null && indexName.length() > 0 && index.length() > 0) {
                for (int i = 0; i < rowMetaInterface.size(); i++) {
                    ValueMetaInterface v = rowMetaInterface.getValueMeta(i);
                    String x = originalDbmeta.getFieldDefinition(v, null, null, false); // ipid LONGTEXT  b TEXT

                    if (index.contains(",")) {   //ipid,pid 复合索引
                        String[] indexes = index.split(",");
                        for (int j = 0; j < indexes.length; j++) {
                            String in = indexes[j];
                            String x1 = null;
                            x1 = transform(x, x1, in);
                            if (x1 != null && x1.length() > 0) {
                                if (originalDatabaseType.equals("POSTGRESQL") && targetDatabaseType.equals("MYSQL") && x.contains("TEXT")) {
                                    x = x.replace("TEXT", "LONGTEXT");  //pg里面text类型同步到mysql中会变成longtext
                                }
                                if (sql1.contains(x)) {
                                    sql1 = sql1.replace(x, x1);
                                }
                            }
                        }
                    } else {   //只有一个索引字段
                        String x1 = null;
                        x1 = transform(x, x1, index);

                        if (x1 != null && x1.length() > 0) {
                            if (originalDatabaseType.equals("POSTGRESQL") && targetDatabaseType.equals("MYSQL") && x.contains("TEXT")) {
                                x = x.replace("TEXT", "LONGTEXT");  //pg里面text类型同步到mysql中会变成longtext
                            }
                            if (sql1.contains(x)) {
                                sql1 = sql1.replace(x, x1);   //将text类型修改为varchar(1000)
                            }
                        }
                    }
                }
                if (targetSchema.length() > 0) {   //分情况添加索引语句
                    String indexSql = "CREATE UNIQUE INDEX " + indexName + " ON " + targetSchema + "." + table + " (" + index + "); ";
                    sql1 = sql1 + Const.CR + indexSql;
                } else {
                    String indexSql = "CREATE UNIQUE INDEX " + indexName + " ON " + targetSchema + " (" + index + "); ";
                    sql1 = sql1 + Const.CR + indexSql;
                }
            } else {
                sql1 = sql1;
            }
            if (targetDatabaseType.equals("POSTGRESQL")) { //postgresql 没有主键的时候更新、删除会出现报错
                String alterSql = " ALTER TABLE " + targetSchema + "." + table + " REPLICA IDENTITY FULL;";
                sql1 = sql1 + Const.CR + alterSql;
            }
        }


        if (sql1.contains(";")) {
            String[] sql2 = sql1.split(";");
            for (String e : sql2) {
                if (!e.trim().equals(""))
                    targetDatabase.execStatement(e.toLowerCase());  //创建表和索引加时间戳
            }
        }
    }

    private static String transform(String x, String x1, String in) {
        if (x.contains(in)) {  //ipid
            if (x.contains("TEXT") && !x.contains("LONG")) {
                x1 = x.replace("TEXT", "VARCHAR(255)");
            } else if (x.contains("LONGTEXT")) {
                x1 = x.replace("LONGTEXT", "VARCHAR(255)");
            } else if (x.contains("text") && !x.contains("long")) {
                x1 = x.replace("text", "VARCHAR(255)");
            } else if (x.contains("longtext")) {
                x1 = x.replace("longtext", "VARCHAR(255)");
            }
        }

        return x1;
    }


    private static StepMeta createStep1(TransMeta transMeta, String sql, DatabaseMeta databaseMeta) {
        // 新建一个表输入步骤(TableInputMeta)
        TableInputMeta tableInputMeta = new TableInputMeta();
        // 设置步骤1的数据库连接
        tableInputMeta.setDatabaseMeta(databaseMeta);
        // 设置步骤1中的sql
        tableInputMeta.setSQL(sql);
        // 设置步骤名称
        return new StepMeta("表输入", tableInputMeta);
    }

    private static StepMeta createStep2(TransMeta transMeta, DatabaseMeta databaseMeta, String schema, String tableName, String type) {
        // 新建一个表输出步骤(TableOutputMeta)
        TableOutputMeta tableOutMeta = new TableOutputMeta();
        // 设置步骤2的数据库连接
        tableOutMeta.setDatabaseMeta(databaseMeta);
        tableOutMeta.setCommitSize(1000);
        tableOutMeta.setUseBatchUpdate(true); //批量插入
        tableOutMeta.setTableName(tableName);
        tableOutMeta.setSchemaName(schema);

        if (type.equals("3")) {
            tableOutMeta.setTruncateTable(true);  //输出表清空原有数据
        } else {
            tableOutMeta.setTruncateTable(false);
        }

        // 设置步骤名称
        return new StepMeta("表输出", tableOutMeta);
    }


    private static StepMeta createStep3(TransMeta transMeta, DatabaseMeta databaseMeta, String schema, String tableName, String keyLookup, String etlTime, RowMetaInterface rowMetaInterface) {
        // 新建一个插入/更新步骤
        newUpdateMeta insertUpdateMeta = new newUpdateMeta();
        insertUpdateMeta.setEtlTime(etlTime);//etlTime
        // 设置步骤2的数据库连接
        insertUpdateMeta.setDatabaseMeta(databaseMeta);
        insertUpdateMeta.setSchemaName(schema);
        // 设置目标表
        insertUpdateMeta.setTableName(tableName);
        String[] fileds = null;
        if (keyLookup != null) {  //根据指定字段进行比对，多字段用逗号隔开
            if (keyLookup.contains(",")) {
                fileds = keyLookup.split(",");
            } else {
                fileds = new String[]{keyLookup};
            }
        }
        insertUpdateMeta.setKeyLookup(fileds);

        assert fileds != null;
        String[] keyCondition = new String[fileds.length];
        Arrays.fill(keyCondition, "=");
        insertUpdateMeta.setKeyCondition(keyCondition);

        insertUpdateMeta.setKeyStream(fileds);
        String[] Stream2 = new String[fileds.length];
        Arrays.fill(Stream2, "");
        insertUpdateMeta.setKeyStream2(Stream2);// 一定要加上

        // 设置要更新的字段，这里设置全部字段更新
        String[] updateLookup = rowMetaInterface.getFieldNames();
        String[] updateStream = rowMetaInterface.getFieldNames();
        Boolean[] updateOrNot = new Boolean[updateLookup.length];
        Arrays.fill(updateOrNot, true);  //全部设置更新
        // 设置表字段
        insertUpdateMeta.setUpdateLookup(updateLookup);
        // 设置流字段
        insertUpdateMeta.setUpdateStream(updateStream);
        // 设置是否更新
        insertUpdateMeta.setUpdate(updateOrNot);
        // 设置步骤的名称
        return new StepMeta("step2name", insertUpdateMeta);

    }

    private static TransMeta createTrans(String name, DatabaseMeta databaseMeta1, DatabaseMeta databaseMeta2) {
        TransMeta transMeta = new TransMeta();
        // 设置转化的名称
        transMeta.setName(name);
        // 添加转换的数据库连接
        transMeta.addDatabase(databaseMeta1);
        transMeta.addDatabase(databaseMeta2);
        return transMeta;
    }

    private static TransHopMeta createHop(StepMeta step1, StepMeta step2) {
        // 设置起始步骤和目标步骤，把两个步骤关联起来
        return new TransHopMeta(step1, step2);
    }

    private static void runTrans(TransMeta transMeta) throws KettleException {
        Trans trans = new Trans(transMeta);
        trans.execute(null);// 执行转换
        trans.waitUntilFinished(); // 等待转换执行结束
    }

    public static boolean checkTableExist(Database database, String schema, String tablename) throws Exception {
        try {

            // Just try to read from the table.
            String sql = "select 1 from  " + schema + "." + tablename;
            Connection connection = null;
            Statement stmt = null;
            ResultSet rs = null;
            try {
                connection = database.getConnection();
                stmt = connection.createStatement();
                stmt.setFetchSize(1000);
                rs = stmt.executeQuery(sql);
                return true;
            } catch (SQLException e) {
                return false;
            } finally {
                close(connection, stmt, rs);
            }
        } catch (Exception e) {
            throw new KettleDatabaseException("", e);
        }

    }

    private static void close(Connection connection, Statement statement, ResultSet resultSet) throws SQLException {

        if (resultSet != null) {
            resultSet.close();
        }

        if (statement != null) {
            statement.close();
        }
    }

    private static Statement executeSql(String sql, Connection connection) throws Exception {
        Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        statement.setQueryTimeout(6000);
        statement.setFetchSize(100000);
        return statement;
    }


    private static void type2(String table, String condition, String originalDatabaseType, String originalSchema, DatabaseMeta originalDbmeta, String targetSchema, String targetDatabaseType, Database targetDatabase, DatabaseMeta targetDbmeta,
                              String tableList, String tableSql) throws Exception {

        LogChannelFactory logChannelFactory = new org.pentaho.di.core.logging.LogChannelFactory();
        LogChannel kettleLog = logChannelFactory.create("type2：根据数据库日志进行增量--");

        if (originalDatabaseType.equals("POSTGRESQL")) {  //读取postgresql日志的
            /**
             * 需要在目标数据库中建一张表记录上次日志读取到的位置logpointer
             **/
            Long logPointer;
            Statement logPointerTime = null;
            ResultSet resultSetlogPointerTime = null;
            String pointerSql = "SELECT pointer  FROM  " + targetSchema + ".logpointer  where tablename= '" + table + "'";
            List<String> pointList = new ArrayList<>();
            try {
                logPointerTime = executeSql(pointerSql, targetDatabase.getConnection());
                resultSetlogPointerTime = logPointerTime.executeQuery(pointerSql);
                if (resultSetlogPointerTime != null) {
                    pointList = ResultSetUtils1.allResultSet(resultSetlogPointerTime);
                }
                if (pointList == null || (pointList.size() == 0) || (pointList.size() > 0 && pointList.get(0) == null)
                        || (pointList.size() > 0 && pointList.get(0).equals("")) || (pointList.size() > 0 && pointList.get(0).equals("null"))) {
                    TransMeta transMeta = createTrans(table, originalDbmeta, targetDbmeta);
                    //第一次全量传数据 UPDATE test.logpointer   SET pointer= null,tablename=null
                    String dataSql = null;
                    if (tableList != null && tableSql == null) {
                        dataSql = "select * from " + originalSchema + "." + table;
                    }
                    if (tableList == null && tableSql != null) {
                        dataSql = tableSql;
                    }
                    StepMeta step1 = createStep1(transMeta, dataSql, originalDbmeta);
                    transMeta.addStep(step1);
                    StepMeta step2 = createStep2(transMeta, targetDbmeta, targetSchema, table, "1");
                    transMeta.addStep(step2);
                    TransHopMeta TransHopMeta = createHop(step1, step2);
                    transMeta.addTransHop(TransHopMeta);
                    runTrans(transMeta);
                    logResponse logResponse = ReadLogUtils.call(condition, 0); //第一次从0开始读取
                    if (logResponse != null) {
                        if (logResponse.getPointer() != null) {
                            logPointer = logResponse.getPointer();
                            String sqlNew = "INSERT INTO " + targetSchema + ".logpointer  (pointer,tablename) VALUES " + "('" + logPointer + "'," + " '" + table + "')";//更新logpointer表，记录全量数据时的日志指针
                            logPointerTime = executeSql(sqlNew, targetDatabase.getConnection());
                            logPointerTime.execute(sqlNew);
                        }
                    }
                } else { //以后就读日志了
                    logResponse logResponse = ReadLogUtils.call(condition, Long.parseLong(pointList.get(0))); //从表中读日志上次的位置
                    if (logResponse != null) {
                        if (logResponse.getList() != null) {
                            List<String> list = logResponse.getList();
                            for (int i = 0; i < list.size(); i++) {
                                if (list.get(i).contains(originalSchema + "." + table) && list.get(i).contains("LOG")) {
                                    String ddl = list.get(i);
                                    if (ddl.toLowerCase().contains("insert")) {
                                        int a = ddl.toLowerCase().indexOf("insert");
                                        ddl = ddl.substring(a);
                                    }
                                    if (ddl.toLowerCase().contains("update")) {
                                        int a = ddl.toLowerCase().indexOf("update");
                                        ddl = ddl.substring(a);
                                    }
                                    if (ddl.toLowerCase().contains("delete")) {
                                        int a = ddl.toLowerCase().indexOf("delete");
                                        ddl = ddl.substring(a);
                                    }
                                    StringBuilder stringBuilder = new StringBuilder();
                                    stringBuilder.append(ddl);
                                    for (int j = i + 1; j < list.size(); j++) {
                                        String parameters = list.get(j);
                                        if (!parameters.toLowerCase().contains("parameters: $")) {
                                            stringBuilder.append("  ").append(parameters);
                                        }
                                        if (parameters.toLowerCase().contains("parameters: $")) {

                                            // 定义正则表达式匹配占位符
                                            String regex = "\\$\\d+";
                                            Pattern pattern = Pattern.compile(regex);

                                            Matcher matcher = pattern.matcher(stringBuilder);
                                            while (matcher.find()) {
                                                String paramIndex = matcher.group(); // 获取占位符，例如：$1
                                                String paramValue = ""; // 定义参数值

                                                // 在parameters中查找与占位符匹配的参数值
                                                String searchStr = paramIndex + " = ";
                                                int startIndex = parameters.indexOf(searchStr);
                                                if (startIndex > -1) {
                                                    int endIndex = parameters.indexOf(",", startIndex);
                                                    if (endIndex == -1) {
                                                        endIndex = parameters.length(); // 参数值是parameters中的最后一项
                                                    }
                                                    // 获取参数值
                                                    paramValue = parameters.substring(startIndex + searchStr.length(), endIndex);
                                                }
                                                // 处理参数值中的单引号
                                                paramValue = paramValue.replaceAll("'", "'");
                                                stringBuilder = new StringBuilder(stringBuilder.toString().replace(paramIndex, paramValue));
                                            }
                                            break;
                                        }
                                    }

                                    String logSql = stringBuilder.toString();
                                    kettleLog.logBasic("根据日志转为sql： " + logSql);
                                    if (logSql.contains((originalSchema + "." + table))) {
                                        logSql = logSql.replace((originalSchema + "." + table), (targetSchema + "." + table)); //替换schema
                                        logPointerTime = executeSql(logSql, targetDatabase.getConnection());//在目标数据库中执行与日志中相同的sql语句
                                        if (logSql.contains("\"") && targetDatabaseType.equals("MYSQL")) { //mysql中插入数据有双引号会报错
                                            logSql = logSql.replace("\"", "");
                                        }
                                        logPointerTime.execute(logSql);
                                        //------sql执行成功才更新日志指针位置-----
                                        if (logResponse.getPointer() != null) {
                                            logPointer = logResponse.getPointer();
                                            String sqlNew = "UPDATE " + targetSchema + ".logpointer  " + " SET pointer= " + "'" + logPointer + "'" + " where tablename= '" + table + "'";
                                            logPointerTime = executeSql(sqlNew, targetDatabase.getConnection());
                                            logPointerTime.execute(sqlNew);
                                        }
                                    }
                                }
                            }

                        }

                    }

                }
            } catch (Exception e) {
                throw new Exception("ERROR：   " + e);
            } finally {
                close(null, logPointerTime, resultSetlogPointerTime);
            }
        }
    }


}




