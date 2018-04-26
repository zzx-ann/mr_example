package xbl.compare;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Prg implements Writable {
    private Text info;
    //private int Sum;
    private Text uid;
    private int Sum;
    private String ip;

    public Text getInfo() {
        return info;
    }
    public void setInfo(Text info) {
        this.info = info;
    }

    public int getSum() { return Sum; }
    public void setSum(int Sum) { this.Sum = Sum;}

    public Text getuid() { return uid; }
    public void setuid(Text  uid) { this.uid = uid;}

    public String getIp() { return ip; }
    public void setIp(String  ip) { this.ip = ip;}

    public void readFields(DataInput in) throws IOException {
        this.info = new Text(in.readUTF());
        this.uid = new Text(in.readUTF());
        Sum = in.readInt();
        ip = in.readUTF();
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(info.toString());
        out.writeUTF(uid.toString());
        out.writeInt(Sum);
        out.writeUTF(ip);
    }

    /*mr 输出到文件的的时候调用*/
    @Override
    public String toString() {
        return info.toString() + "\t" + uid.toString() + "\t" + Sum + "\t ip=" + ip;
    }
}
