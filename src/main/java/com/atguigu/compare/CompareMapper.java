package com.atguigu.compare;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by WXHang on HANG at 2020/12/15 11:05
 */
public class CompareMapper extends Mapper <LongWritable, Text, FlowBean,Text> {//默认会把key排序，所以我们必须把要排序的内容放到key的位置
    private Text phone = new Text();
    private FlowBean flowBean= new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

    //一行数据
        String line = value.toString();
        //切分
        String[] fields = line.split("/t");

        //封装
        phone.set(fields[0]);
        flowBean.setDownFlow(Long.parseLong(fields[2]));
        flowBean.setUpFlow(Long.parseLong(fields[1]));
        flowBean.setUpFlow(Long.parseLong(fields[3]));

        //写出去

        context.write(flowBean,phone);

    }
}
