package xbl.compare;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*自定义key比较函数类*/
public class Tgid implements WritableComparable<Tgid>{
    /**
     * 自定义类型的中包含的变量，本例中的变量都是用于排序的变量
     * 后序的事例中我们还将定义一些其它功能的变量
     */
    private String tid;
    private String gid;
    
    public Tgid() {}
    public Tgid(String tid, String gid) {
        this.tid = tid;
        this.gid = gid;
    }

    public String getTid() { return tid; }
    public void setTid(String  tid) { this.tid = tid;}

    public String getGid() { return gid; }
    public void setGid(String  gid) { this.gid = gid;}

    /**
     * 反序列化，从流中的二进制转换成自定义Key
     */
    public void readFields(DataInput in) throws IOException {
        tid = in.readUTF();
        gid = in.readUTF();
    }
    /**
     * 序列化，将自定义Key转化成使用流传送的二进制
     */
    public void write(DataOutput out) throws IOException {
        out.writeUTF(tid);
        out.writeUTF(gid);
    }

    /**
     * 这里不实现此方法，我们会在SortComparator中实现
     */
    public int compareTo(Tgid o) {
        return 0;
    }
    /*mr 输出到文件的的时候调用*/
    @Override
    public String toString() {
        return tid + "\t" + gid;
    }
}
