package com.atguigu.mapjoin;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.shaded.org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by WXHang on HANG at 2020/12/18 9:48
 */
public class MapJoinMapper extends Mapper <LongWritable, Text,Text, NullWritable>{


    private Map<String,String> pdMap = new HashMap<>();
    private Text k = new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //首先获取到缓存中的文件名称
        URI[] cacheFiles = context.getCacheFiles();

        //开流读取文件
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        FSDataInputStream pd = fileSystem.open(new Path(cacheFiles[0]));

        //将文件按行处理，读取k，v值到map中
        BufferedReader pdreader = new BufferedReader(new InputStreamReader(pd));

        String line;
        while (StringUtils.isNotEmpty(line = pdreader.readLine())){
            String[] split = line.split("\t");
            pdMap.put(split[0],split[1]);
        }
        IOUtils.closeStream(pdreader);

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        k.set(split[0] + "\t"+
                pdMap.get(split[1]) + "\t" +
                //将pid替换  split[1]的值即为pdmap的key，pdmap通过key值获取到value
                split[2]);
        context.write(k,NullWritable.get());
    }


}
