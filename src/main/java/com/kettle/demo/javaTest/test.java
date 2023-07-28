package com.kettle.demo.javaTest;


//import com.kettle.demo.utils.IncrementData;
import com.kettle.demo.utils.incrementData1;
//import com.kettle.demo.utils.newCDCUtils;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.KettleEnvironment;
public class test {
    public static void main(String[] args) throws Exception {

        KettleEnvironment.init();


//        IncrementData.incrementData("POSTGRESQL","postgres","test","127.0.0.1","5432","postgres",
//                "123456","MYSQL","test?useUnicode=true&characterEncoding=utf-8","test", "127.0.0.1","3308","root",
//                "123","1","SJGXSJ",null,"select * from test.emrcrd;" +
//                        "select * from test.emrvdr");  //1. postgresql2mysql  时间戳 sql


//        IncrementData.incrementData("POSTGRESQL","postgres","public","127.0.0.1","5432","postgres",
//                "123456","MYSQL","test?useUnicode=true&characterEncoding=utf-8","test", "127.0.0.1","3308","root",
//                "123","2","D:\\PostgreSQL\\11\\data\\pg_log",null,"select * from public.cbjcjb;" +
//                        "select * from public.course");  //3. postgresql2mysql  日志  sql


//        incrementData1.incrementData("POSTGRESQL","postgres","public","127.0.0.1","5432","postgres",
//                "123456","MYSQL","test?useUnicode=true&characterEncoding=utf-8","test", "127.0.0.1","3308","root",
//                "123","2","D:\\PostgreSQL\\11\\data\\pg_log","cbjcjb,course",null);  //4. postgresql2mysql  日志  表名



//----------------------------------------------------------------------------------------------------------------------------------------------------

//                incrementData1.incrementData("POSTGRESQL","postgres","public","127.0.0.1","5432","postgres",
//                "123456","MYSQL","test?useUnicode=true&characterEncoding=utf-8","test", "127.0.0.1","3308","root",
//                "123","3",null,"weinan",null,"a,b","index_a","etlTime","a");
////           postgresql2mysql 表名  全量


//                incrementData1.incrementData("ORACLE","orcl","HUAYIN","172.16.202.120","1521","huayin",
//                "huayin","POSTGRESQL","postgres","test", "127.0.0.1","5432","postgres",
//                "123456","3",null,"TEST", null,"WW","index_a","etlTime","WW,EE");
////           oracle2postgresql 表名  全量



//        incrementData1.incrementData("ORACLE","orcl","HUAYIN","172.16.202.120","1521","huayin",
//                "huayin","POSTGRESQL","postgres","test", "127.0.0.1","5432","postgres",
//                "123456","1","SJGXSJ","EMRCRD",
//                               null,"dataid","index_dataid","SJGXSJ", "dataid");
//               //   oracle2postgresql


                incrementData1.incrementData("ORACLE","orcl","HUAYIN","172.16.202.120","1521","huayin",
                "huayin","POSTGRESQL","postgres","test", "127.0.0.1","5432","postgres",
                "123456","1","SJGXSJ",null,
                               "  select emrvdr0001||emrvdr0002||'test' as aa,emrvdr0002+1 as bb, substr(emrvdr0006,3) as cc, dataid as dd, sjgxsj from huayin.emrvdr ","dd","index_dd","SJGXSJ", "dd");
               //   oracle2postgresql

//        incrementData1.incrementData("POSTGRESQL","postgres","test","127.0.0.1","5432","postgres",
//                "123456","MYSQL","test?useUnicode=true&characterEncoding=utf-8","test", "127.0.0.1","3308","root",
//                "123","1","etltime","test",null,"ww","index_ww","etltime", "ww");  //2. postgresql2mysql  时间戳 表名

//        newCDCUtils.incrementData("POSTGRESQL", "postgres", "test", "127.0.0.1", "5432", "postgres",
//                "123456", "MYSQL", "test?useUnicode=true&characterEncoding=utf-8", "test", "127.0.0.1", "3308", "root",
//                "123", "test3","10.0.108.51:9092","test_cdc","x","x_index","etltime");  // postgresql2mysql


    }
}




