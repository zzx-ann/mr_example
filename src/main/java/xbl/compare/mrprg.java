package xbl.compare;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;

public class mrprg {
    /*第一个Object表示输入key的类型；第二个Text表示输入value的类型；第三个Text表示表示输出键的类型；第四个IntWritable表示输出值的类型*/
    public static class AgeMap extends  Mapper<Object, Text, Tgid, Prg> {
        private Tgid groupbykey = new Tgid();
        private Prg outTuple = new Prg();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String content = itr.nextToken();
                String[] splits = content.split("#");
                System.out.println("line:" + value.toString());
                if(splits.length == 18){
                    String tid = splits[9];
                    String gid = splits[10];
                    String info = splits[15].length() > 0 ? splits[15] :  "aaa";
                    String uid = splits[2] +"_"+ splits[3] +"_"+ splits[5];
                    String ip = splits[16].length() > 0 ? splits[16] : "127.0.0.1";
                    outTuple.setInfo( new Text(info));
                    outTuple.setuid( new Text(uid));
                    outTuple.setIp(ip);
                    outTuple.setSum(1);

                    groupbykey.setTid(tid);
                    groupbykey.setGid(gid);
                    System.out.println("ip:" + ip);
                    System.out.println("outTuple:" + outTuple);

                    context.write(groupbykey, outTuple);
                }else{
                    String tid = "other";
                    String gid = "other";
                    String  info= "aaa";
                    String uid ="0_0_0";
                    String ip = "127.0.0.1";
                    outTuple.setInfo(new Text(info));
                    outTuple.setSum(1);
                    outTuple.setuid(new Text(uid));
                    outTuple.setIp(ip);
                    groupbykey.setTid(tid);
                    groupbykey.setGid(gid);
                    System.out.println("ip:" + ip);
                    System.out.println("outTuple:" + outTuple);
                    context.write(groupbykey, outTuple);
                }
            }
        }
    }

    /*输入键类型，输入值类型，输出键类型，输出值类型*/
    public static class AgeReduce extends Reducer<Tgid, Prg, Tgid, Prg> {
        private Prg result = new Prg();
        private Tgid keys = new Tgid();

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
            context.write(keys, result);
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
        //设置排序比较器
        job.setSortComparatorClass(SortComparator.class);
        //设置分组比较器
        job.setGroupingComparatorClass(GroupingComparator.class);

        job.setOutputKeyClass(Tgid.class);
        job.setOutputValueClass(Prg.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}