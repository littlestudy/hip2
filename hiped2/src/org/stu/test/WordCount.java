package org.stu.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool{
    
    public static void main(String[] args) throws Exception {
        args = new String [] {
            "hdfs://master:9000/user/hadoop/data/test.txt",
            "hdfs://master:9000/user/hadoop/output/test"
        };
        int res = ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);   
        
        Configuration conf = super.getConf();
        
        Job job = new Job(conf);
        job.setJarByClass(WordCount.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);
        if (job.waitForCompletion(true))
            return 0;
        return 1;
    }
    
    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable>{
        private IntWritable count = new IntWritable(1);
        private Text outKey = new Text(); 
        protected void map(LongWritable key, Text value, Context context) 
                throws IOException ,InterruptedException {
            String[] words = value.toString().split(" ", -1);
            for (String w : words){
                outKey.set(w);
                context.write(outKey, count);
            }
        }
    }
    
    public static class ReducerClass extends Reducer<Text, IntWritable, Text, Text>{
        private Text outValue = new Text();
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException ,InterruptedException {
            int count = 0;
            for (IntWritable i : values){
                count += i.get();
            }
            outValue.set(String.valueOf(count));
            context.write(key, outValue);
        }
    }

}
