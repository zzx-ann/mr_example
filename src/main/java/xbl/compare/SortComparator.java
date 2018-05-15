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
         *  //利用这个来控制升序或降序
         *         //this本对象写在前面代表是升序
         *         //this本对象写在后面代表是降序
         * 首先根据第一个字段排序，然后根据第二个字段排序
         */
        System.out.println("a1:" + a1);
        System.out.println("b1:" + b1);
        if(!a1.getTid().equals(b1.getTid())) {
            return a1.getTid().compareTo(b1.getTid());
        }else {
            return  a1.getGid().compareTo(b1.getGid());
        }
    }
}