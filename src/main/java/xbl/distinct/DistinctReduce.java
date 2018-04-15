package xbl.distinct;


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;

public class DistinctReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable result = new IntWritable();
    IntWritable counts = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException {
        final Counter counter = context.getCounter("Sensitive", "count( distinct( uid ) )");
        counter.increment(1L);   //计数器加1

        int sum = 0;
        for(IntWritable val:values) {
            sum += val.get();
        }
        System.out.println("fields:" + key );
        result.set(sum);

/*        Text status = new Text();
        status.set("count(distinct(uid))");
*//*        Text count = new Text();
        count.set( Long.toString(counter.getValue()));*//*

        counts.set((int)counter.getValue());
        System.out.println("count:" + counts );

        context.write(status , counts);*/
        context.write(key,result);
        //context.write(key,new Text(""));
    }
}