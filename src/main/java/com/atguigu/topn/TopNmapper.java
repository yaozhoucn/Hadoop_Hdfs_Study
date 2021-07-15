package com.atguigu.topn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by WXHang on HANG at 2020/12/24 9:57
 */
public class TopNmapper extends Mapper<LongWritable, Text,FlowBean, NullWritable> {
    private FlowBean flowBean = new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        flowBean.setPhone(split[0]);
        flowBean.setUpFlow(Long.parseLong(split[1]));
        flowBean.setDownFlow(Long.parseLong(split[2]));
        flowBean.setSumFlow(Long.parseLong(split[3]));
        context.write(flowBean,NullWritable.get());

    }
}
