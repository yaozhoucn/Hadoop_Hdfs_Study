package com.atguigu.compare;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by WXHang on HANG at 2020/12/15 15:22
 */
public class Flowcomparator extends WritableComparator {
    protected Flowcomparator() {
        super(FlowBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        FlowBean fa = (FlowBean) a;
        FlowBean fb = (FlowBean) b;
        return Long.compare(fb.getSumFlow(),fa.getSumFlow());
    }
}
