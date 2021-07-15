package com.atguigu.partitioner;

import com.atguigu.flow.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by WXHang on HANG at 2020/12/14 11:21
 */
public class MyPartitioner extends Partitioner <Text, FlowBean>{

    /**
     * 对每一条k，v对，返回它们对应的分区号
     * @param text  手机号
     * @param flowBean 流量
     * @param i 分区号
     * @return
     */
    public int getPartition(Text text, FlowBean flowBean, int i) {

        //获取手机号前三位
        String phone_head = text.toString().substring(0, 3);
        switch (phone_head) {
            case "136":
                return 0;
            case "137":
                return 1;
            case "138":
                return 2;
            case "139":
                return 3;
            default:
                return 4;
        }

    }
}
