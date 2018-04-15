package xbl.fields;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable one = new IntWritable(1);
    Text count = new Text();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException {
        final Counter counter = context.getCounter("Sensitive", "count()");
        counter.increment(1L);   //计数器加1
        count.set("count");
        System.out.println("count" + counter.getValue());
        context.write(count , one);
    }
}