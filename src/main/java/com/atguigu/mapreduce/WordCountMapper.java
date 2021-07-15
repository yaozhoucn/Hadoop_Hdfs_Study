package com.atguigu.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by WXHang on HANG at 2020/12/3 14:25
 */
public class WordCountMapper extends Mapper <LongWritable, Text,Text, IntWritable> {
    private Text word = new Text();
    private IntWritable one = new IntWritable(1);
    /**
     *框架将一行一行的输入进来，我们将语句拆分成（key，one）的形式
     * @param key 行号
     * @param value 行内容
     * @param context 框架本身
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
        String line = value.toString();
        String[] words= line.split(" ");
        for (String word : words) {
            this.word.set(word);
            context.write(this.word,this.one);
        }
    }
}
