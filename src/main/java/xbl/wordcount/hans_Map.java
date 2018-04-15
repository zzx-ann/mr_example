package xbl.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class hans_Map extends Mapper<Object, Text, Text, IntWritable> {
    IntWritable one = new IntWritable(1);
    Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException,InterruptedException {
        /*字符串分割，默认采的分隔符是“空格”、“制表符（‘\t’）”、“换行符(‘\n’）”、“回车符（‘\r’）”。*/
        StringTokenizer itr = new StringTokenizer(value.toString());
        System.out.println("line:" + itr);
        while(itr.hasMoreTokens()) {   //返回是否还有分隔符。
            word.set(itr.nextToken()); //返回从当前位置到下一个分隔符的字符串。
            System.out.println("word:" + word);
            context.write(word, one);
        }
    }
}