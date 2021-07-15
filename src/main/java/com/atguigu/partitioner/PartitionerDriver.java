package com.atguigu.partitioner;

import com.atguigu.flow.FlowBean;
import com.atguigu.mapreduce.WordCountMapper;
import com.atguigu.mapreduce.WordCountReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

/**
 * Created by WXHang on HANG at 2020/12/14 11:41
 */

public class PartitionerDriver {

    public static void main(String[] args) throws IOException {
        //获取job实例
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //设置jar包
        job.setJarByClass(PartitionerDriver.class);

        //设置map，reduce实例
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);

        //设置map与reduce的输入输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);


        //设置分区为5个
        job.setNumReduceTasks(5);
        //设置partitioner类
        job.setPartitionerClass(MyPartitioner.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //设置输入输出文件路径
        FileInputFormat.setInputPaths(job,new Path("//file:///e:/input"));
        FileOutputFormat.setOutputPath(job,new Path("//file:///e:/output"));

    }
}

