package com.kettle.demo.javaTest;
//
//
//import org.apache.flink.api.java.io.TextInputFormat;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//import org.apache.kafka.clients.admin.AdminClient;
//
//import java.security.CodeSource;
//import java.security.ProtectionDomain;
//
public class test8 {


        public static void main(String[] args) {
//            ProtectionDomain pd = AdminClient.class.getProtectionDomain();
//            CodeSource cs = pd.getCodeSource();
//            System.out.println(cs.getLocation());
            String s="column1@";
            System.out.println(s.substring(0, s.length() - 1));


    }



}
