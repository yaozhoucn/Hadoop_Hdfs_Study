package com.atguigu.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by WXHang on HANG at 2020/12/3 14:25
 */
public class WordCountReduce extends Reducer<Text, IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
        int sum = 0;
        for (IntWritable word : values) {
            sum += word.get();
        }
        result.set(sum);
        context.write(key,result);
    }

}
