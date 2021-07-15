package com.atguigu.reducejoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by WXHang on HANG at 2020/12/17 10:43
 * 做数据替换工作
 */
public class OrderReducer extends Reducer<OrderBean, NullWritable,OrderBean,NullWritable> {
    @Override
    /**
     * 收到的数据pname在前，order紧随其后
     */
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //拿到迭代器
        Iterator<NullWritable> iterator = values.iterator();
        //迭代第一组数据
        iterator.next();
        String pname = key.getPname();
        //迭代剩下的数据并输出
       while (iterator.hasNext()){
           iterator.next();
           key.setPname(pname);
           context.write(key,NullWritable.get());
       }

    }
}
