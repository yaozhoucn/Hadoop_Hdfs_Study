package com.atguigu.compare;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 实现writcomparble接口（比较大小排序）
 */
public class FlowBean implements WritableComparable<FlowBean> {
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    public void set(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    /**
     * 将对象数据写出到框架指定地方
     * @param dataOutput 数据的容器
     * @throws IOException
     */
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    /**
     * 从框架指定地方读取数据填充对象
     * @param dataInput 数据的容器
     * @throws IOException
     */
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();
    }

    /**
     * 实现比较方法
     * @param o
     * @return
     */
    @Override
    public int compareTo(FlowBean o) {
      /*  if (this.sumFlow<o.sumFlow){
            return 1;
        }else if(this.sumFlow == o.sumFlow){
            return 0;
        }else {
            return -1;
        }*/
      //降序把o放前面，从大到小排列
        int i = Long.compare(this.sumFlow, o.sumFlow);
        return i;
    }
}
