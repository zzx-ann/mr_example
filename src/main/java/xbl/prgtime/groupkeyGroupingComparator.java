package xbl.prgtime;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
/*自定义key比较函数类*/
public class groupkeyGroupingComparator extends WritableComparator {

    //传入作为key的bean的class类型，以及制定需要让框架做反射获取实例对象
    protected groupkeyGroupingComparator() {
        super(groupkey.class, true);
    }

    public int compare(groupkey a, groupkey b) {
        groupkey a1 = (groupkey) a;
        groupkey b1 = (groupkey) b;
        int tidcmp = a1.getTid().compareTo(b1.getTid());
        int gidcmp = a1.getGid().compareTo(b1.getGid());
        int prgnamecmp = a1.getPrgname().compareTo(b1.getPrgname());
        System.out.println("cmd:" + tidcmp + "|" + gidcmp + "|" + gidcmp );
        if (tidcmp != 0) return tidcmp;
        if (gidcmp != 0) return gidcmp;
        if (prgnamecmp != 0) return prgnamecmp;
        return 0;
    }

}