package com.atguigu.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

/**
 * Created by WXHang on HANG at 2020/12/3 14:25
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取job实例
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        //2.设置jar包；
        job.setJarByClass(WordCountDriver.class);

        //3.设置mapper与reduce
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);

        //4.设置mapper与reduce的输出类型

        //设置combiner
        //job.setCombinerClass(WordCountReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //5.设置输入输出文件
        Path inputpath = new Path(args[0]);
        Path outputpath = new Path(args[1]);
        FileInputFormat.setInputPaths(job,inputpath);
        FileOutputFormat.setOutputPath(job,outputpath);

        //6.提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
