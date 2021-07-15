package com.atguigu.reducejoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by WXHang on HANG at 2020/12/17 11:29
 */
public class OrderComparator extends WritableComparator {
    protected OrderComparator() {
        //传true才会生成新对象
        //让父类生成两个orderbean对象
        super(OrderBean.class,true);
    }

    @Override
    /**
     * 按照pid进行分组，比较a与b；
     */
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean abean = (OrderBean) a;
        OrderBean bbean = (OrderBean) b;
      return   abean.getPid().compareTo(bbean.getPid());
    }
}
