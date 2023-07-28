package com.kettle.demo.utils;

import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogUtils {

    private long pointer = 0; //上次文件大小
    private static final Logger logger = LoggerFactory.getLogger(LogUtils.class);
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyy-MM-dd HH:mm:ss");

    ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);

    public List<String> realtimeShowLog(File logFile) throws Exception {
        List<String>logList=new ArrayList<>();

        if (logFile == null) {
            throw new IllegalStateException("logFile can not be null");
        }

        //启动一个线程每2秒读取新增的日志信息
        exec.scheduleWithFixedDelay(new Runnable() {

            @Override
            public void run() {

                //获得变化部分
                try {

                    long len = logFile.length();
                    if (len < pointer) {
                        logger.info("Log file was reset. Restarting logging from start of file.");
                        pointer = 0;
                    } else {

                        //指定文件可读可写
                        RandomAccessFile randomFile = new RandomAccessFile(logFile, "r");

                        //获取RandomAccessFile对象文件指针的位置，初始位置是0，字节数
                        System.out.println("文件指针位置:" + pointer);

                        randomFile.seek(pointer);//移动文件指针位置

                        String tmp = "";
                        while ((tmp = randomFile.readLine()) != null) {
                            String s=new String(tmp.getBytes("utf-8"));
                            logList.add(s);
                            System.out.println("数据库日志信息 : " + s);
                            pointer = randomFile.getFilePointer();
                        }

                        randomFile.close();
                    }

                } catch (Exception e) {
                    //实时读取日志异常，需要记录时间和lastTimeFileSize 以便后期手动补充
                    logger.error(dateFormat.format(new Date()) + " File read error, pointer: " + pointer);
                } finally {
                    //将pointer 落地以便下次启动的时候，直接从指定位置获取
                }
            }

        }, 0, 50, TimeUnit.SECONDS);
        return logList;

    }

    public void stop() {
        if (exec != null) {
            exec.shutdown();
            logger.info("file read stop !");
        }
    }


    public static List<String> call(String path) throws Exception {
        Date date = new Date();
        SimpleDateFormat formatterError = new SimpleDateFormat("yyyy-MM-dd");
        String date1 = formatterError.format(date);

        LogUtils view = new LogUtils();
        File tmpLogFile = new File(path);
        if (!tmpLogFile.exists()) {
            System.out.println(path + " not exists");//不存在就输出
        }
        File fa[] = tmpLogFile.listFiles();
        List<String> list = new ArrayList<>();
        for (int i = 0; i < fa.length; i++) {//循环遍历
            File fs = fa[i];//获取数组中的第i个
            if (!fs.isDirectory()) {
                if (fs.getName().contains(date1)) {
                    list.add(fs.getName());
                }

            }
        }
        List<String>logList =new ArrayList<>();
        if (list.size() > 0) {
            for (String s : list) {
                String path1 = path + "\\" + s;
                File tmpLogFile1 = new File(path1);
                view.pointer = 0;
                logList=view.realtimeShowLog(tmpLogFile1);
            }
        }
        System.out.println("--------------"+logList);
        return logList;
    }


//    public static void main(String[] args) throws Exception {
//
//
//        String path = "D:\\PostgreSQL\\11\\data\\pg_log";
//        call(path);
//
//
//    }
}
