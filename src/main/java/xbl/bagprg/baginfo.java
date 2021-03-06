package xbl.bagprg;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class baginfo implements Writable {
    private Long uid;
    public Long getUid() {
        return uid;
    }

    public void setUid(Long uid) {
        this.uid = uid;
    }



    public void readFields(DataInput in) throws IOException {
        uid = in.readLong();

    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(uid);
    }

    @Override
    public String toString() {
        return ""+uid;
    }


  /*  private int id;
    private Long uid;
    private int tm;
    private int devtype;
    private int ts;  //tid stime
    private int te;  // tid endtime
    private String udid;
    private String deviceid;
    private String tid;
    private String gid;
    private String info;
    private String ip;
    private String prg;
    private int time;
    private int TidCount;
    private int Count;
    private int Sum;

    public Long getuid() {
        return uid;
    }
    public String getudid() {
        return udid;
    }
    public String getdeviceid() {
        return deviceid;
    }

    public String gettid() {
        return tid;
    }
    public String getgid() {
        return gid;
    }
    public String getinfo() {
        return info;
    }
    public String getPrg() {
        return prg;
    }
    public int getTime() {
        return time;
    }
    public int getCount() {
        return Count;
    }

    public void setuid(Long uid) {
        this.uid = uid;
    }
    public void setudid(String udid) {
        this.udid = udid;
    }
    public void setdeviceid(String deviceid) {
        this.deviceid = deviceid;
    }
    public void settid(String tid) {
        this.tid = tid;
    }
    public void setgid(String gid) {
        this.gid = gid;
    }
    public void setinfo(String info) {
        this.info = info;
    }
    public void setPrg(String prg) {
        this.prg = prg;
    }
    public void setTime(int time) {
        this.time = time;
    }


    public void setTidCount(int TidCount) {
        this.TidCount = TidCount;
    }
    public void setSum(int Sum) {
        this.Sum = Sum;
    }

    *//* 是把输入流字节反序列化*//*
    public void readFields(DataInput in) throws IOException {

       *//* tid = in.readUTF();
        gid = in.readUTF();
        deviceid = in.readUTF();
        info = in.readUTF();
        udid = in.readUTF(); *//*

        uid = in.readLong();

        //prg = in.readLine();
        //time = in.readInt();
    }

    *//* 是把每个对象序列化到输出流*//*
    public void write(DataOutput out) throws IOException {
       *//* out.writeUTF(info);
        out.writeUTF(tid);
        out.writeUTF(gid);
        out.writeUTF(udid);
        out.writeUTF(deviceid);*//*

        out.writeLong(uid);
        //out.writeChars(prg);
       // out.writeInt(time);
    }

    @Override
    public String toString() {
        System.out.println(Long.toString(uid) + "| udid:"+udid+"|deviceid"+deviceid+"|gid:"+gid+"|info:"+info);
        return uid + "\t" + udid  + "\t" + deviceid +  "\t" + gid + "\t" + info;
    }*/
}