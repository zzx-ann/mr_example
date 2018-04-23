package xbl.bagprg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import xbl.fields.MinMaxCountTuple;

import java.io.IOException;
import java.util.StringTokenizer;

public class bagprgtime {

    public static class TidMap extends Mapper<Object, Text, Text, baginfo> {

        private Text keys = new Text();
        private baginfo outTuple = new baginfo();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String content = itr.nextToken();
                String[] splits = content.split("#");
                System.out.println("line:" + value.toString() + "| length:"+ splits.length);
                if(splits.length == 18){
/*                    id                      int
                    app                     string
                    udid                    string
                    deviceid                string
                    devtype                 int
                    uid                     int
                    ver                     string
                    tm                      int
                    tm1                     int
                    tid                     int
                    gid                     string
                    ver1                    string
                    ts                      int
                    te                      int
                    status                  int
                    info                    array<string>
                            ip                      string
                    ip2                     string
                    stime                   int
                    ymd                     string*/

                    String name = splits[0];
                    String udid = splits[2];
                    String deviceid = (splits[3]);
                    Long uid = 0L;
                    if(null != splits[5]){
                        uid = Long.parseLong(splits[5]);
                    }

                    String tid = (splits[9]);
                    String gid = (splits[10]);

                    String info = "aaa";
/*                    if(null !=  splits[15] && "" != splits[15] ){
                        info = splits[15];

                    }*/

                    /*outTuple.setudid(udid);
                    outTuple.setdeviceid(deviceid);
                    outTuple.setuid(uid);
                    outTuple.settid(tid);
                    outTuple.setgid(gid);
                    outTuple.setinfo(info);*/
                    outTuple.setUid(uid);
                    keys.set(tid);


/*                    String[] prginfo = info.split("|");
                    for ( int i = 0; i <= prginfo.length; i++) {
                        String[] prg = prginfo[i].split(",");
                        String[] nextprg = prginfo[i+1].split(",");
                        int time = Integer.valueOf(nextprg[4]).intValue() - Integer.valueOf(prg[4]).intValue();
                    }*/
                    System.out.println("keys:" + keys + "|" + outTuple);
                    context.write(keys, outTuple);
                }else{
                    keys.set("other");
                  /*  outTuple.setudid("default");
                    outTuple.setdeviceid("default");
                    outTuple.setuid(0L);
                    outTuple.settid("0");
                    outTuple.setgid("0");
                    outTuple.setinfo("default");*/
                    outTuple.setUid(0L);
                    System.out.println("keys:" + keys + "|" + outTuple);
                    context.write(keys, outTuple);
                   /* outTuple.setMin(min);
                    outTuple.setMax(max);
                    outTuple.setCount(count);
                    userName.set(name);
                    context.write(userName, outTuple);*/
                }
            }
        }
    }

    public static class TidReduce extends Reducer<Text, baginfo, Text, baginfo> {
        private baginfo result = new baginfo();

        public void reduce(Text key, Iterable<baginfo> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
/*            result.setTidCount(0);
            for (baginfo tmp : values) {

                sum += tmp.getCount();
            }
            result.setSum(sum);*/
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
        job.setJarByClass(bagprgtime.class);
        job.setMapperClass(TidMap.class);
        job.setCombinerClass(TidReduce.class);
        job.setReducerClass(TidReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(baginfo.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}