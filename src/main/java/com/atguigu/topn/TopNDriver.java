package com.atguigu.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * Created by WXHang on HANG at 2020/12/24 9:57
 */
public class TopNDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(TopNDriver.class);

        job.setMapperClass(TopNmapper.class);
        job.setReducerClass(TopNReducer.class);

        //启用Combiner求MapTask的局部前十，优化Map输出的数据量
        job.setCombinerKeyGroupingComparatorClass(TopNCompartor.class);
        job.setCombinerClass(TopNReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setGroupingComparatorClass(TopNCompartor.class);

        job.setOutputKeyClass(FlowBean.class);
        job.setOutputValueClass(NullWritable.class);



        FileInputFormat.setInputPaths(job,new Path("D:/hadoop临时文件/input/top10input.txt"));
        FileOutputFormat.setOutputPath(job,new Path("D:/hadoop临时文件/output"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
