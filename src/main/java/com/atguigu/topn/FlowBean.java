package com.atguigu.topn;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by WXHang on HANG at 2020/12/24 9:56
 */
public class FlowBean implements WritableComparable<FlowBean> {
    String phone;
    Long upFlow;
    Long downFlow;
    Long sumFlow;

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;

    }

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Long sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public String toString() {
        return this.phone + "\t" + this.upFlow + "\t" + this.downFlow + "\t" + this.sumFlow;
    }

    public FlowBean(String phone, Long upFlow, Long downFlow, Long sumFlow) {
        this.phone = phone;
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = sumFlow;
    }

    public FlowBean() {
    }

    @Override
    public int compareTo(FlowBean o) {
        return Long.compare(o.sumFlow,this.sumFlow);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(phone);
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
    this.phone = dataInput.readUTF();
    this.upFlow = dataInput.readLong();
    this.downFlow = dataInput.readLong();
    this.sumFlow  =dataInput.readLong();
    }
}
