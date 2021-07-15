package com.atguigu.topn;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by WXHang on HANG at 2020/12/24 9:57
 */
public class TopNReducer extends Reducer<FlowBean, NullWritable,FlowBean,NullWritable> {
    @Override
    protected void reduce(FlowBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<NullWritable> iterator = values.iterator();
        for (int i= 0;i< 10;i++){
            if(iterator.hasNext()){
                context.write(key,iterator.next());
            }
        }
    }
}
