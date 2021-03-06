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
    public static class AgeMap extends  Mapper<Object, Text, Text, Prg> {

        private Text groupbykey = new Text();
        private Prg outTuple = new Prg();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String content = itr.nextToken();
                String[] splits = content.split("#");
                System.out.println("line:" + value.toString());
                if(splits.length == 18){
                    String tgid = splits[9] + "#" + splits[10];
                    String info = splits[15].length() > 0 ? splits[15] :  "aaa";
                    String uid = splits[2] +"_"+ splits[3] +"_"+ splits[5];
                    String ip = splits[16].length() > 0 ? splits[16] : "127.0.0.1";
                    outTuple.setInfo( new Text(info));
                    outTuple.setuid( new Text(uid));
                    outTuple.setIp(ip);
                    outTuple.setSum(1);
                    groupbykey.set(tgid);
                    System.out.println("ip:" + ip);
                    System.out.println("outTuple:" + outTuple);

                    context.write(groupbykey, outTuple);
                }else{
                    String tgid = "other";
                    String  info= "aaa";
                    String uid ="0_0_0";
                    String ip = "127.0.0.1";
                    outTuple.setInfo(new Text(info));
                    outTuple.setSum(1);
                    outTuple.setuid(new Text(uid));
                    outTuple.setIp(ip);
                    groupbykey.set(tgid);
                    System.out.println("outTuple:" + outTuple);
                    context.write(groupbykey, outTuple);
                }
            }
        }
    }


    public static class AgeReduce extends Reducer<Text, Prg, Text, Prg> {
        private Prg result = new Prg();

        public void reduce(Text key, Iterable<Prg> values, Context context) throws IOException, InterruptedException {

            int Sum = 0;
            for (Prg tmp : values) {

                Sum += tmp.getSum();
                result.setuid(tmp.getuid());
                result.setInfo(tmp.getInfo());
                result.setIp(tmp.getIp());
                System.out.println("tmp:" + tmp);
                System.out.println("Sum:" + Sum);
            }
/*            System.out.println("info:" + info);
            System.out.println("uid:" + uid);
            System.out.println("Sum:" + Sum);*/


            result.setSum(Sum);

            System.out.println("result:" + result);
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