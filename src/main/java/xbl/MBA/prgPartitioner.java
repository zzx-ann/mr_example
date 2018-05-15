package xbl.MBA;
import org.apache.hadoop.mapreduce.Partitioner;

public class prgPartitioner extends Partitioner<groupkey, Compute>{

    @Override
    public int getPartition(groupkey key, Compute value, int numReduceTasks) {
        //相同Tid+gid，会发往相同的partition
        //而且，产生的分区数，是会跟用户设置的reduce task数保持一致
        String partion = key.getTid()+key.getGid();
        return (partion.hashCode() & Integer.MAX_VALUE) % numReduceTasks;

    }

}