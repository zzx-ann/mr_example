package xbl.prgtime;

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

public class mrprgtime {
    /*第一个Object表示输入key的类型；第二个Text表示输入value的类型；第三个Text表示表示输出键的类型；第四个IntWritable表示输出值的类型*/
    public static class AgeMap extends  Mapper<Object, Text, groupkey, Compute> {
        private groupkey groupbykey = new groupkey();
        private Compute outTuple = new Compute();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreElements()) {
                String content = itr.nextToken();
                String[] splits = content.split("#");
                System.out.println("line:" + value.toString());
                if(splits.length == 18){
                    String tid = splits[9];
                    String gid = splits[10];
                    String info = splits[15];
                    String uid = splits[2] +"_"+ splits[3] +"_"+ splits[5];
                    String ip = splits[16].length() > 0 ? splits[16] : "127.0.0.1";

                    String[] files = splits[15].split("\\|");
                    System.out.println("info:" + info);
                    System.out.println("files:" + files);
                    for(int i=0; i< files.length; i++){
                        System.out.println("files:" + files[i]);
                        String prg = files[i];
                        System.out.println("prg:" + prg);
                        String[] prginfo = prg.split(",");
                        System.out.println("prginfo:" + prginfo.toString());
                        if(prginfo.length != 6) continue;

                        groupbykey.setTid(tid);
                        groupbykey.setGid(gid);
                        groupbykey.setPrgname(prginfo[0]+"#"+prginfo[1]);
                        int time = Integer.parseInt(prginfo[5]);
                        outTuple.setTime(time);
                        outTuple.setCount(1);
                        outTuple.setAverageTime(time);

                        System.out.println("groupbykey:" + groupbykey);
                        System.out.println("outTuple:" + outTuple);

                        context.write(groupbykey, outTuple);
                    }

                 /*   outTuple.setInfo( new Text(info));
                    outTuple.setuid( new Text(uid));
                    outTuple.setIp(ip);
                    outTuple.setSum(1);

                    groupbykey.setTid(tid);
                    groupbykey.setGid(gid);
                    System.out.println("ip:" + ip);
                    System.out.println("outTuple:" + outTuple);

                    context.write(groupbykey, outTuple);*/
                }/*else{
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
                    groupbykey.segroupkey(gid);
                    System.out.println("ip:" + ip);
                    System.out.println("outTuple:" + outTuple);
                    context.write(groupbykey, outTuple);
                }*/
            }
        }
    }

    /*输入键类型，输入值类型，输出键类型，输出值类型*/
    public static class AgeReduce extends Reducer<groupkey, Compute, groupkey, Compute> {
        private Compute result = new Compute();
        /*map的输出数据和redece的接受参类型不一致的时候redeuce不会执行*/
        public void reduce(groupkey key, Iterable<Compute> values, Context context) throws IOException, InterruptedException {


            int Time = 0;
            int Count = 0;
            System.out.println("key:" + key);
            for (Compute tmp : values) {
                Time += tmp.getTime();
                Count += tmp.getCount();
                System.out.println("tmp:" + tmp);
            }
            System.out.println("Time:" + Time);
            System.out.println("Count:" + Count);
            result.setAverageTime(Time/Count);
            result.setCount(Count);
            result.setTime(Time);

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
        job.setJarByClass(Compute.class);
        job.setMapperClass(AgeMap.class);
        //设置排序比较器  map阶段自定义排序
        // job.setSortComparatorClass(SortComparator.class);
        //设置分组比较器  reudce 阶段自定义分组
        job.setGroupingComparatorClass(groupkeyGroupingComparator.class);
        /*map 分区*/
        job.setPartitionerClass(prgPartitioner.class);
        /*分区数量*/
        job.setNumReduceTasks(3);
        /*本地合拼处理，减少网络传输资源*/
        //job.setCombinerClass(AgeReduce.class);
        job.setReducerClass(AgeReduce.class);


        job.setOutputKeyClass(groupkey.class);
        job.setOutputValueClass(Compute.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}