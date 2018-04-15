package xbl.wordcount;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class hans_Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException {

        int sum = 0;
        for(IntWritable val:values) {
            sum += val.get();
        }
        System.out.println("word:" + key + "| sum:" + sum);
        result.set(sum);
        context.write(key,result);
    }
}