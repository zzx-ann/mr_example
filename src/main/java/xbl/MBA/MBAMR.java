package xbl.MBA;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import xbl.util.Combination;


public class MBAMR {
    /*第一个Object表示输入key的类型；第二个Text表示输入value的类型；第三个Text表示表示输出键的类型；第四个IntWritable表示输出值的类型*/

    public static class MBAMap extends  Mapper<Object, Text, Text, IntWritable> {

        public static final Logger THE_LOGGER = Logger.getLogger(MBAMR.class);
        private static final IntWritable NUMBER_ONE = new IntWritable(1);
        private static final Text reducerKey = new Text();
        int numberOfPairs; // will be read by setup(), set by driver

        private static List<String> convertItemsToList(String line) {
            if ((line == null) || (line.length() == 0)) {
                // no mapper output will be generated
                return null;
            }
            String[] tokens = StringUtils.split(line, "|");
            if ( (tokens == null) || (tokens.length == 0) ) {
                return null;
            }
            List<String> items = new ArrayList<String>();
            for (String token : tokens) {
                if (token != null) {
                    items.add(token.trim());
                }
            }
            return items;
        }

        /**
         *
         * build <key, value> by sorting the input list
         * If not sort the input, it may have duplicated list but not considered as a same list.
         * ex: (a, b, c) and (a, c, b) might become different items to be counted if not sorted
         * @param numberOfPairs   number of pairs associated
         * @param items      list of items (from input line)
         * @param context   Hadoop Job context
         * @throws IOException
         * @throws InterruptedException
         */
        private void generateMapperOutput(int numberOfPairs, List<String> items, Context context)
                throws IOException, InterruptedException {
            List<List<String>> sortedCombinations = Combination.findSortedCombinations(items, numberOfPairs);
            for (List<String> itemList: sortedCombinations) {
                System.out.println("itemlist="+itemList.toString());
                reducerKey.set(itemList.toString());
                context.write(reducerKey, NUMBER_ONE);
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            List<String> items = convertItemsToList(line);
            if ((items == null) || (items.isEmpty())) {
                // no mapper output will be generated
                return;
            }
            generateMapperOutput(numberOfPairs, items, context);
        }
    }


    /*输入键类型，输入值类型，输出键类型，输出值类型*/
    public static class MBAReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Compute result = new Compute();
        /*map的输出数据和redece的接受参类型不一致的时候redeuce不会执行*/
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0; // total items paired
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));

             /*   int Time = 0;
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
                context.write(key, result);*/
        }
    }

    private static int printUsage(){
        System.out.println("USAGE: [input-path] [output-path] [number-of-pairs]");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }


    public static void main(String[] args) throws Exception {
        if(args.length != 3){
            printUsage();
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];
        int numberOfPairs = Integer.parseInt(args[2]);

        System.out.println("inputPath:" + inputPath);
        System.out.println("outputPath:" + outputPath);
        System.out.println("numberOfPairs:" + numberOfPairs);

       /* THE_LOGGER.info("inputPath: " + inputPath);
        THE_LOGGER.info("outputPath: " + outputPath);
        THE_LOGGER.info("numberOfPairs: " + numberOfPairs);*/


        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = new Job(conf, "MBA MR ");
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));



        job.setJarByClass(MBAMR.class);
        job.setMapperClass(MBAMap.class);
        job.setCombinerClass(MBAReduce.class);
        job.setReducerClass(MBAReduce.class);

        //设置排序比较器  map阶段自定义排序
        // job.setSortComparatorClass(SortComparator.class);
        //设置分组比较器  reudce 阶段自定义分组
        //job.setGroupingComparatorClass(groupkeyGroupingComparator.class);
        /*map 分区*/
        //job.setPartitionerClass(prgPartitioner.class);
        /*分区数量*/
        //job.setNumReduceTasks(3);
        /*本地合拼处理，减少网络传输资源*/
        //job.setCombinerClass(AgeReduce.class);

        Path outputDir = new Path(outputPath);
        FileSystem.get(conf).delete(outputDir, true);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}