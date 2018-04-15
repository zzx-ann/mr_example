package xbl.fields;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DistinctCount {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 3) {
            System.err.println("Usage: need 3 file <in> <out>");
            System.exit(3);
        }

        /*distinct job*/
        Job job = new Job(conf, "hans : distinct");
        //*13最后，如果要打包运行改程序，则需要调用如下行
        job.setJarByClass(DistinctCount.class);
        /*指定执行的map 和reduce 类*/
        job.setMapperClass(DistinctMap.class);
        job.setCombinerClass(DistinctReduce.class);
        job.setReducerClass(DistinctReduce.class);
        /*只有map和reduce输出是不一样的时候要设置map的输出，默认map和reduce的输出是一样的。*/
        /*
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        */
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        /*count job*/
        Job job1 = new Job(conf, "hans : count");
        job1.setJarByClass(DistinctCount.class);
        job1.setMapperClass(CountMap.class);
        job1.setCombinerClass(CountReduce.class);
        job1.setReducerClass(DistinctReduce.class);
        //设定map和reduce的输入输出类型
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        //设定输入输出文件  将job的输出为做job1的输入
        FileInputFormat.addInputPath(job1, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));

        //提交job 等待job完成执行job1
        if (job.waitForCompletion(true)) {
            System.out.println("job success");
            System.exit(job1.waitForCompletion(true) ? 0 : 1);
        }
    }
}