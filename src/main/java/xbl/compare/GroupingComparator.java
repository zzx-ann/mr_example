package xbl.compare;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator {
    public GroupingComparator() {
        super(Tgid.class, true);
    }
    public int compare(WritableComparable a, WritableComparable b) {
        Tgid a1 = (Tgid)a;
        Tgid b1 = (Tgid)b;
        /**
         *只根据第一个字段进行分组
         */
        return a1.getTid().compareTo(b1.getTid());
    }
}