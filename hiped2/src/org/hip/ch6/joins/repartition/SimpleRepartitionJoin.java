package org.hip.ch6.joins.repartition;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hip.ch6.joins.User;
import org.hip.ch6.joins.UserLog;
import org.htuple.Tuple;

import com.google.common.collect.Lists;

public class SimpleRepartitionJoin extends Configured implements Tool{

    private static Log LOG = LogFactory.getLog(SimpleRepartitionJoin.class);
    
    public static void main(String[] args) throws Exception {    
        args = new String[] {
            "hdfs://master:9000/user/hadoop/data/users.txt",
            "hdfs://master:9000/user/hadoop/data/user-logs.txt",
            "hdfs://master:9000/user/hadoop/output/SimpleRepartitionJoin2"
        };        
        
        int res = ToolRunner.run(new Configuration(), new SimpleRepartitionJoin(), args);
        System.exit(res);
    }
    
    enum ValueFields{
        DATASET, DATA
    }

    public static final int USERS = 0;
    public static final int USER_LOGS = 1;
    
        @Override
    public int run(String[] args) throws Exception {
        Path usersPath = new Path(args[0]);
        Path userLogsPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);
        
        Configuration conf = super.getConf();
        
        Job job = Job.getInstance(conf);
        
        job.setJarByClass(SimpleRepartitionJoin.class);
        
        MultipleInputs.addInputPath(job, usersPath, TextInputFormat.class, UserMap.class);
        MultipleInputs.addInputPath(job, userLogsPath, TextInputFormat.class, UserLogMap.class);
        
        job.setReducerClass(Reduce.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Tuple.class);
        
        FileOutputFormat.setOutputPath(job, outputPath);
        return job.waitForCompletion(true) ? 0 : 1;
    }
        
    public static class UserMap extends Mapper<LongWritable, Text, Text, Tuple>{
        protected void map(LongWritable key, Text value, Context context)
                throws IOException ,InterruptedException {
            User user = User.fromText(value);
            
            Tuple outputValue = new Tuple();
            outputValue.setInt(ValueFields.DATASET, USERS);
            outputValue.setString(ValueFields.DATA, value.toString());
            
            context.write(new Text(user.getName()), outputValue);
        }
    }
    
    public static class UserLogMap extends Mapper<LongWritable, Text, Text, Tuple>{
        protected void map(LongWritable key, Text value, Context context)
                throws IOException ,InterruptedException {
            UserLog userLog = UserLog.fromText(value);
            
            Tuple outputValue = new Tuple();
            outputValue.setInt(ValueFields.DATASET, USER_LOGS);
            outputValue.setString(ValueFields.DATA, value.toString());
            
            context.write(new Text(userLog.getName()), outputValue);
        }
    }
    
    public static class Reduce extends Reducer<Text, Tuple, Text, Text>{
        List<String> users;
        List<String> userLogs;
        
        protected void reduce(Text key, Iterable<Tuple> values, Context context) 
                throws IOException ,InterruptedException {
            users = Lists.newArrayList();
            userLogs = Lists.newArrayList();
            
            for (Tuple tuple : values){
                LOG.info(" -----> Tuple: " + tuple);
                switch (tuple.getInt(ValueFields.DATASET)) {
                case USERS:
                    users.add(tuple.getString(ValueFields.DATA));
                    break;
                case USER_LOGS:
                    userLogs.add(tuple.getString(ValueFields.DATA));
                    break;
                default:
                    break;
                }
            }
        
            for (String user : users){
                for (String userLog : userLogs)
                    context.write(new Text(user), new Text(userLog));
            }
        }
    }
}
