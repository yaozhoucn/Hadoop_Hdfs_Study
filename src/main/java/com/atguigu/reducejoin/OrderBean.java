package com.atguigu.reducejoin;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by WXHang on HANG at 2020/12/17 10:23
 */
public class OrderBean implements WritableComparable<OrderBean> {
    //把两张表封装成一个对象来处理
    private String pid;
    private String id;
    private int amount;
    private String pname;

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    @Override
    public String toString() {
        return id + "\t" + pname + "\t" + amount;
        //只输出三列
    }

/*
    public OrderBean(String pid, String id, int amount, String pname) {
        this.pid = pid;
        this.id = id;
        this.amount = amount;
        this.pname = pname;
    }

    public OrderBean() {

    }
*/

    /**
     * 按照pid分组
     * @param o
     * @return
     */
    @Override
    //根据pid进行分组，组内按照pname进行降序排序
    public int compareTo(OrderBean o) {
        int i = this.pid.compareTo(o.pid);
        if(i != 0){
            return i;

        }else {
            return o.pname.compareTo(this.pname);
        }
    }

    @Override
    //序列化与反序列化
    public void write(DataOutput dataOutput) throws IOException {
        //序列化
        dataOutput.writeUTF(id);
        dataOutput.writeUTF(pid);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(pname);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        //反序列化
        this.id = dataInput.readUTF();
        this.pid = dataInput.readUTF();
        this.amount = dataInput.readInt();
        this.pname = dataInput.readUTF();
    }
}
