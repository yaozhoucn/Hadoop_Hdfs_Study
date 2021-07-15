package com.atguigu.InputFormat;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 *
 * Created by WXHang on HANG at 2020/12/8 10:20
 * 负责将文件装换成一组key，value对；
 */
public class MyRecordReader extends RecordReader<Text, ByteWritable> {

    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    //初始化
    //开流
        FileSystem fileSystem = FileSystem.get(taskAttemptContext.getConfiguration());


    }
    //读取下一组key，是否读到
    public boolean nextKeyValue() throws IOException, InterruptedException {
        //用流


        return false;


    }
    //获取当前读到的key
    public Text getCurrentKey() throws IOException, InterruptedException {
        return null;
    }
    //获取当前读到的value
    public ByteWritable getCurrentValue() throws IOException, InterruptedException {
        return null;
    }

    public float getProgress() throws IOException, InterruptedException {
        //显示进度
        return 0;
    }

    public void close() throws IOException {
        //用来关闭资源
        //关流
    }
}
