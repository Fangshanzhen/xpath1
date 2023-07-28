package com.kettle.demo.constant;

public class Constant {


    public final static String tableSqlPostgreSql = "select schemaname ||'@' ||tablename  from pg_tables where schemaname='?'";

    public final static String dataSql = "select * from tableName where  dataid is not null and  patientid is not null and patientid !='未采集' and patientid !='-'  and patientid !='888888888888888888'   " +
            " and  sjtbzt =0    limit ";

    //  and sjgxsj >'2023-02-06 00:00:00'
//    public final static String dataSql =
//            "select * from (select * , row_number()over(partition by dataid order by sjgxsj) as num1  from tableName where dataid is not null and patientid is not null and sjtbzt=0 ) cc  where num1=1 limit ";


    public final static String tablesqlMysql="SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='?'";
    //SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='test'
// UPDATED_DATE >SYSDATE -2


  public final static String oracleViewSql = "select * from tableName  where sjgxsj>= to_date('last_time','yyyy-mm-dd hh24:mi:ss') and   rownum <=    " ;
//    public final static String oracleViewSql = "select * from tableName  where PRESCRIBE_DATE >= 'last_time' and   rownum <=  " ;  //test

    public final static String oracleSql = "select * from tableName  where sjtbzt=0 and   rownum <=    " ;
//    public final static String oracleSql = "select * from tableName  where    rownum <=  " ;   //test

    public final static String tablesqlOracle="  select OWNER ||'@' ||TABLE_NAME  from all_tables where OWNER='?'    ";
    //SELECT * FROM all_tables WHERE OWNER = 'EHR'  //test

    public final static String viewsqlOracle="  select OWNER ||'@' ||view_name  from dba_views where OWNER='?'    ";
    //select OWNER ||'@' ||view_name  from dba_views where OWNER='EHR'

    public final static String countSql = "select start_time from @.etl_count limit 1 ";

    public final static String countOracleSql = "select start_time from @.etl_count   where rownum =1 ";

    public final static String tableSqlPostgreSql1 = "select 'schemaname' ||'@'|| tableName  from schemaname.tablestatus where status=1 ";

    public final static String tableSqlOraclel1 = "select 'schemaname' ||'@'|| TABLENAME  from schemaname.tablestatus where status=1 ";


    public static String LAST_SCN = "0";/**源数据库配置*/

    public static String DATABASE_DRIVER="oracle.jdbc.driver.OracleDriver";
    public static String SOURCE_DATABASE_URL="jdbc:oracle:thin:@127.0.0.1:1521:practice";
    public static String SOURCE_DATABASE_USERNAME="sync";
    public static String SOURCE_DATABASE_PASSWORD="sync";
    public static String SOURCE_CLIENT_USERNAME = "huayin";/**目标数据库配置*/

    public static String SOURCE_TARGET_URL="jdbc:oracle:thin:@127.0.0.1:1521:target";
    public static String SOURCE_TARGET_USERNAME="target";
    public static String SOURCE_TARGET_PASSWORD="target";/**日志文件路径*/

    public static String LOG_PATH = "/home/app/oracle/oradata/ORCL/";/**数据字典路径*/

    public static String DATA_DICTIONARY = "/home/app/oracle";


}
