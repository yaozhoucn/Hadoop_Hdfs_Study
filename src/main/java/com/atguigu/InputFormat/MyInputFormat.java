package com.atguigu.InputFormat;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Created by WXHang on HANG at 2020/12/8 10:17
 *
 * 返回一个自定义的recordreader
 */
public class MyInputFormat extends FileInputFormat <Text, ByteWritable>{//输出key，value的泛型
    public RecordReader<Text, ByteWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new MyRecordReader();
    }
}
