package com.kettle.demo.javaTest;


//import com.kettle.demo.utils.IncrementData;
import com.kettle.demo.utils.incrementData;
//import com.kettle.demo.utils.newCDCUtils;
import org.pentaho.di.core.KettleEnvironment;
public class test {
    public static void main(String[] args) throws Exception {

        KettleEnvironment.init();




//----------------------------------------------------------------------------------------------------------------------------------------------------

//                incrementData.incrementData("POSTGRESQL","postgres","public","127.0.0.1","5432","postgres",
//                "123456","MYSQL","test?useUnicode=true&characterEncoding=utf-8","test", "127.0.0.1","3308","root",
//                "123","3",null,"weinan",null,"a,b","index_a","etlTime","a");
////           postgresql2mysql 表名  全量


//                incrementData.incrementData("ORACLE","orcl","HUAYIN","172.16.202.120","1521","huayin",
//                "huayin","POSTGRESQL","postgres","test", "127.0.0.1","5432","postgres",
//                "123456","3",null,"TEST", null,"WW","index_a","etlTime","WW,EE");
////           oracle2postgresql 表名  全量



//        incrementData.incrementData("ORACLE","orcl","HUAYIN","172.16.202.120","1521","huayin",
//                "huayin","POSTGRESQL","postgres","test", "127.0.0.1","5432","postgres",
//                "123456","1","SJGXSJ","EMRCRD",
//                               null,"dataid","index_dataid","SJGXSJ", "dataid");
//               //   oracle2postgresql


//                incrementData.incrementData("ORACLE","orcl","HUAYIN","172.16.202.120","1521","huayin",
//                "huayin","POSTGRESQL","postgres","test", "127.0.0.1","5432","postgres",
//                "123456","1","SJGXSJ",null,
//                               "  select emrvdr0001||emrvdr0002||'test' as aa,emrvdr0002+1 as bb, substr(emrvdr0006,3) as cc, dataid as dd, sjgxsj from huayin.emrvdr ","dd","index_dd","SJGXSJ", "dd");
//               //   oracle2postgresql

//        incrementData.incrementData("POSTGRESQL","postgres","test","127.0.0.1","5432","postgres",
//                "123456","MYSQL","test?useUnicode=true&characterEncoding=utf-8","test", "127.0.0.1","3308","root",
//                "123","1","etltime","test",null,"ww","index_ww","etltime", "ww");  //2. postgresql2mysql  时间戳 表名

//        newCDCUtils.incrementData("POSTGRESQL", "postgres", "test", "127.0.0.1", "5432", "postgres",
//                "123456", "MYSQL", "test?useUnicode=true&characterEncoding=utf-8", "test", "127.0.0.1", "3308", "root",
//                "123", "test3","10.0.108.51:9092","test_cdc","x","x_index","etltime");  // postgresql2mysql


        //----------------------------------------------------------------------------------------------------------------------------------------------------



//        incrementData.incrementData("ORACLE","orcl","HUAYIN","172.16.202.120","1521","huayin",
//                "huayin","POSTGRESQL","postgres","test", "127.0.0.1","5432","postgres",
//                "123456","1","SJGXSJ",null,
//                "  select emrvdr0001||emrvdr0002||'test' as aa,emrvdr0002+1 as bb, substr(emrvdr0006,3) as cc, dataid as dd, sjgxsj from huayin.emrvdr ; select * from huayin.emrcrd","dd,dataid","index","SJGXSJ", "dd,dataid");
//        //   oracle2postgresql


//                incrementData.incrementData("POSTGRESQL","postgres","public","127.0.0.1","5432","postgres",
//                "123456","MYSQL","test?useUnicode=true&characterEncoding=utf-8","test", "127.0.0.1","3308","root",
//                "123","3",null,"weinan,cbjcjb",null,"a#b,cbjcjb0001","index","etlTime","a,cbjcjb0001");
////           postgresql2mysql 表名  全量





                incrementData.incrementData("POSTGRESQL","postgres","test","127.0.0.1","5432","postgres",
                "123456","MYSQL","test?useUnicode=true&characterEncoding=utf-8","test", "127.0.0.1","3308","root",
                "123","1","etltime","fang,fang1",null,"aa,fang","index","etltime", "aa,fang");  // postgresql2mysql  时间戳 表名
    }
}




