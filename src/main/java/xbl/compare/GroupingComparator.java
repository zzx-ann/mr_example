package xbl.compare;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//主要就是对于分组进行排序，分组只按照组建键中的一个值进行分组
public class GroupingComparator extends WritableComparator {
    //必须要调用父类的构造器
    public GroupingComparator() {
        super(Tgid.class, true);
    }
    public int compare(WritableComparable a, WritableComparable b) {
        Tgid a1 = (Tgid)a;
        Tgid b1 = (Tgid)b;
        /**
         *只根据第一个字段进行分组
         */
        System.out.println("a1 g:" + a1);
        System.out.println("b1:g" + b1);
        return a1.getTid().compareTo(b1.getTid());
    }
}