package com.atguigu.compare;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;

/**
 * Created by WXHang on HANG at 2020/12/15 11:05
 * 收到的数据手机后在后，输出时手机号要在前面
 */
public class CompareReduce extends Reducer<FlowBean, Text,Text,FlowBean> {
    /**
     * Reduce收到的数据已经排完序了，我们反过来输出就ok了
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value,key);
        }
    }
}

