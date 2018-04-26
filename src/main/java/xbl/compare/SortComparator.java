package xbl.compare;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortComparator extends WritableComparator {
    public SortComparator() {
        super(Tgid.class, true);
    }
    public int compare(WritableComparable a, WritableComparable b) {
        Tgid a1 = (Tgid)a;
        Tgid b1 = (Tgid)b;
        /**
         * 首先根据第一个字段排序，然后根据第二个字段排序
         */
        if(!a1.getTid().equals(b1.getTid())) {
            return a1.getTid().compareTo(b1.getTid());
        }else {
            return  a1.getGid().compareTo(b1.getGid());
        }
    }
}