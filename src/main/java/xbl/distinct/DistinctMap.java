package xbl.distinct;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistinctMap extends Mapper<Object, Text, Text, IntWritable> {
    private static Text line=new Text();
    IntWritable one = new IntWritable(1);
    Text fields = new Text();

    public void map(Object key, Text value, Context context) throws IOException,InterruptedException {
        /*字符串分割，默认采的分隔符是“空格”、“制表符（‘\t’）”、“换行符(‘\n’）”、“回车符（‘\r’）”。*/
        StringTokenizer itr = new StringTokenizer(value.toString(),"#");
        System.out.println("line:" + value.toString());
        System.out.println("fields count:" + itr.countTokens());
        if( itr.countTokens() <= 0){
            fields.set(value.toString());
        }else{
            fields.set(itr.nextToken()); //返回从当前位置到下一个分隔符的字符串。
        }

        System.out.println("first fields:" + fields);
        context.write(fields, one);
       // context.write(fields, new Text(""));

/*        while(itr.hasMoreTokens()) {   //返回是否还有分隔符。
            line.set(itr.nextToken()); //返回从当前位置到下一个分隔符的字符串。
            System.out.println("word:" + line);
            context.write(line, one);
        }*/
    }
}