package com.atguigu.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by WXHang on HANG at 2020/12/17 10:43
 *
 * 输入时还是原始文件处理封装成ordeBean
 */
public class OrderMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable> {
    private OrderBean orderBean = new OrderBean();
    private String filename;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取数据数据的文件名
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        filename = fileSplit.getPath().getName();
    }

    //获取切分的文件名
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //切分一行内容
        String[] split = value.toString().split("\t");
        //封装，按照数据来源不同分别封装(把数据封装成我们的对象)
        if("order.txt".equals(filename)){
            //封装order
            orderBean.setId(split[0]);
            orderBean.setPid(split[1]);
            orderBean.setAmount(Integer.parseInt(split[2]));
            orderBean.setPname("");

            //封装pd
        }else {
            orderBean.setPid(split[0]);
            orderBean.setPname(split[1]);
            orderBean.setAmount(0);
            orderBean.setId("");
        }
        context.write(orderBean,NullWritable.get());
    }
}