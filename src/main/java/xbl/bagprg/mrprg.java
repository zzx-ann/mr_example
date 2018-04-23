package xbl.bagprg;

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

public class mrprg {
    public static class AgeMap extends
            Mapper<Object, Text, Text, Prg> {

        private Text userName = new Text();
        private Prg outTuple = new Prg();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String content = itr.nextToken();
                String[] splits = content.split("#");
                System.out.println("line:" + value.toString());
                if(splits.length == 18){
                    String name = splits[0];
                    int min = 0;
                    if(null != splits[5]){
                        min = Integer.valueOf(splits[5]);
                    }
                    outTuple.setMin(min);

                    userName.set(name);
                    context.write(userName, outTuple);
                }else{
                    String name = "";
                    int min = 0;
                    outTuple.setMin(min);

                    userName.set(name);
                    context.write(userName, outTuple);
                }
            }
        }
    }

    public static class AgeReduce extends Reducer<Text, Prg, Text, Prg> {
        private Prg result = new Prg();

        public void reduce(Text key, Iterable<Prg> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            result.setMin(0);
            result.setMin(Integer.MAX_VALUE);
            for (Prg tmp : values) {
                sum += 1;
            }
            result.setMin(sum);
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
        job.setJarByClass(Prg.class);
        job.setMapperClass(AgeMap.class);
        job.setCombinerClass(AgeReduce.class);
        job.setReducerClass(AgeReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Prg.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}