package com.atguigu.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;


/**
 * Created by WXHang on HANG at 2020/12/18 9:48
 */
public class MapJoinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(MapJoinDriver.class);

        job.setMapperClass(MapJoinMapper.class);
        //这里不需要reduce，设置reduce的数量为0
        job.setNumReduceTasks(0);

       //添加分布式缓存
        job.addCacheFile(URI.create("file:///D:/hadoop临时文件/input/pd.txt"));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path("D:/hadoop临时文件/input/order.txt"));
        FileOutputFormat.setOutputPath(job,new Path("D:/hadoop临时文件/output"));

        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);

    }
}
