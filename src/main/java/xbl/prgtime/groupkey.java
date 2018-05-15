package xbl.prgtime;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*自定义key比较函数类*/
public class groupkey implements WritableComparable<groupkey>{
    /**
     * 自定义类型的中包含的变量，本例中的变量都是用于排序的变量
     * 后序的事例中我们还将定义一些其它功能的变量
     */
    private String tid;
    private String gid;
    private String prgname;

    public groupkey() {
        set(tid, gid, prgname);
    }

    public groupkey(String tid, String gid, String prgname) {
        set(tid, gid, prgname);
    }

    private void set(String tid, String gid, String prgname){
        this.tid = tid;
        this.gid = gid;
        this.prgname = prgname;
    }

    public String getTid() { return tid; }
    public void setTid(String  tid) { this.tid = tid;}

    public String getGid() { return gid; }
    public void setGid(String  gid) { this.gid = gid;}

    public String getPrgname() { return prgname; }
    public void setPrgname(String  prgname) { this.prgname = prgname;}

    /**
     * 反序列化，从流中的二进制转换成自定义Key
     */
    public void readFields(DataInput in) throws IOException {
        tid = in.readUTF();
        gid = in.readUTF();
        prgname = in.readUTF();
    }
    /**
     * 序列化，将自定义Key转化成使用流传送的二进制
     */
    public void write(DataOutput out) throws IOException {
        out.writeUTF(tid);
        out.writeUTF(gid);
        out.writeUTF(prgname);
    }

    /**
     * 实现分组
     */
    public int compareTo(groupkey o) {
        int cmp = 0;
        int tidcmp = this.tid.compareTo(o.getTid());
        int gidcmp = this.gid.compareTo(o.getGid());
        int prgnamecmp = this.prgname.compareTo(o.getPrgname());
        System.out.println("this:" + this + "|o:" + o  );
        System.out.println("cmd:" + tidcmp + "|" + gidcmp + "|" + prgnamecmp );
        if (tidcmp != 0) return -tidcmp;
        if (gidcmp != 0) return -gidcmp;
        if (prgnamecmp != 0) return -prgnamecmp;

        System.out.println("return:" +cmp );
        return cmp;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof groupkey){
            groupkey tp = (groupkey)obj;
            return tid.equals(tp.tid) && gid.equals(tp.gid) && prgname.equals(tp.prgname);
         }
        return false;
    }

    public int hashCode() {
        return tid.hashCode() * 163 + tid.hashCode();
    }

    /*mr 输出到文件的的时候调用*/
    @Override
    public String toString() {
        return tid + "\t" + gid + "\t" + prgname;
    }
}
