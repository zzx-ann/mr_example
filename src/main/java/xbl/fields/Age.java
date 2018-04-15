package xbl.fields;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Age {
    public static class AgeMap extends
            Mapper<Object, Text, Text, MinMaxCountTuple> {

        private Text userName = new Text();
        private MinMaxCountTuple outTuple = new MinMaxCountTuple();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String content = itr.nextToken();
                String[] splits = content.split("#");
                System.out.println("line:" + value.toString());
                if(splits.length == 4){
                    String name = splits[0];
                    int min = Integer.valueOf(splits[1]);
                    int max = Integer.valueOf(splits[2]);
                    int count = Integer.valueOf(splits[3]);
                    outTuple.setMin(min);
                    outTuple.setMax(max);
                    outTuple.setCount(count);
                    userName.set(name);
                    context.write(userName, outTuple);
                }else{
                    String name = "";
                    int min = 0;
                    int max = 0;
                    int count = 0;
                    outTuple.setMin(min);
                    outTuple.setMax(max);
                    outTuple.setCount(count);
                    userName.set(name);
                    context.write(userName, outTuple);
                }
            }
        }
    }

    public static class AgeReduce extends Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple> {
        private MinMaxCountTuple result = new MinMaxCountTuple();

        public void reduce(Text key, Iterable<MinMaxCountTuple> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            result.setMax(0);
            result.setMin(Integer.MAX_VALUE);
            for (MinMaxCountTuple tmp : values) {
                if (tmp.getMin() < result.getMin()) {
                    result.setMin(tmp.getMin());
                }
                if (tmp.getMax() > result.getMax()) {
                    result.setMax(tmp.getMax());
                }
                sum += tmp.getCount();
            }
            result.setCount(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: MinMaxCountDriver <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "select Min Max Count ");
        job.setJarByClass(Age.class);
        job.setMapperClass(AgeMap.class);
        job.setCombinerClass(AgeReduce.class);
        job.setReducerClass(AgeReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MinMaxCountTuple.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}