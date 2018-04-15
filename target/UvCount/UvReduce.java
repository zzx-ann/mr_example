package xbl.UvCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.Counter;

import java.io.IOException;

public class UvReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException {
        /*final String line = key.toString();
        StringTokenizer tokenizer = new StringTokenizer(key.toString());*/
        final Counter counter = context.getCounter("Sensitive", "count( distinct( uid ) )");/*
        if (value.toString().contains("hello")) {
            counter.increment(1L);   //当查询到包含hello的词语时，计数器加1
        }*/
        counter.increment(1L);   //当查询到包含hello的词语时，计数器加1

        int sum = 0;
        for(IntWritable val:values) {
            sum += val.get();
        }

        System.out.println("fields:" + key );
        result.set(sum);
        context.write(key,result);
        //context.write(key,new Text(""));
    }
}