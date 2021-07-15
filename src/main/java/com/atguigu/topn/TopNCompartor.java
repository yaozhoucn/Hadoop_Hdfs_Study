package com.atguigu.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 将数据全部分到一组中
 *
 * Created by WXHang on HANG at 2020/12/24 9:58
 */
public class TopNCompartor extends WritableComparator {
    protected TopNCompartor() {
        super(FlowBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return 0;
    }
}
