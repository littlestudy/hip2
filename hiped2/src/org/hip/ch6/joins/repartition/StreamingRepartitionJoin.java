package org.hip.ch6.joins.repartition;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.hip.base.BaseMapReduce;
import org.hip.ch6.joins.User;
import org.hip.ch6.joins.UserLog;
import org.htuple.ShuffleUtils;
import org.htuple.Tuple;

import com.google.common.collect.Lists;

public class StreamingRepartitionJoin extends BaseMapReduce{	

	private static String jobId;
	private static Configuration conf_;
	
	public StreamingRepartitionJoin() {
		super(StreamingRepartitionJoin.class);
	}

	public static void main(String[] args) throws Exception {		
		args = new String[] {
            "hdfs://master:9000/user/hadoop/data/users.txt",
            "hdfs://master:9000/user/hadoop/data/user-logs.txt",
            "hdfs://master:9000/user/hadoop/output/StreamingRepartitionJoin13"
        };  
		
		exec(new StreamingRepartitionJoin(), args);
		showLogInfo(jobId, conf_, ShowLog.FULL);
	}
	
	enum KeyFields{
		USER, DATASET
	}
	
	enum ValueFields {
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
		job.setJarByClass(StreamingRepartitionJoin.class);
		
		MultipleInputs.addInputPath(job, usersPath, TextInputFormat.class, UserMap.class);
		MultipleInputs.addInputPath(job, userLogsPath, TextInputFormat.class, UserLogMap.class);
		
		ShuffleUtils.configBuilder()
			.useNewApi()
			.setSortIndices(KeyFields.USER, KeyFields.DATASET)
			.setPartitionerIndices(KeyFields.USER)
			.setGroupIndices(KeyFields.USER)
			.configure(job.getConfiguration());
		
		job.setReducerClass(Reduce.class);
		
		job.setMapOutputKeyClass(Tuple.class);
		job.setMapOutputValueClass(Tuple.class);
		
		FileOutputFormat.setOutputPath(job, outputPath);
		
		if (job.waitForCompletion(true)){
			jobId = job.getJobID().toString();
			conf_ = conf;
			return 0;
		}
		return 1;
	}
	
	public static class UserMap extends Mapper<LongWritable, Text, Tuple, Tuple>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			User user = User.fromText(value);
			
			Tuple outputKey = new Tuple();
			outputKey.setString(KeyFields.USER, user.getName());
			outputKey.setInt(KeyFields.DATASET, USERS);
			
			Tuple outputValue = new Tuple();
			outputValue.setInt(ValueFields.DATASET, USERS);
			outputValue.setString(ValueFields.DATA, value.toString());
			
			context.write(outputKey, outputValue);
		}
	}
	
	public static class UserLogMap extends Mapper<LongWritable, Text, Tuple, Tuple>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			UserLog userLog = UserLog.fromText(value);
			
			Tuple outputKey = new Tuple();
			outputKey.setString(KeyFields.USER, userLog.getName());
			outputKey.setInt(KeyFields.DATASET, USER_LOGS);
			
			Tuple outputValue = new Tuple();
			outputValue.setInt(ValueFields.DATASET, USER_LOGS);
			outputValue.setString(ValueFields.DATA, value.toString());
			
			context.write(outputKey, outputValue);
		}
	}
	
	public static class Reduce extends BaseReduce<Tuple, Tuple, Text, Text>{
		public Reduce() {
			super(Reduce.class);
		}

		List<String> users;
		
		@Override
		protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
				throws IOException, InterruptedException {
			users = Lists.newArrayList();
			
			info("key : " 
					+ key.getString(KeyFields.USER) 
					+  "  ----  "
					+ key.getInt(KeyFields.DATASET));
			
			for (Tuple tuple : values){
				info(tuple.getString(ValueFields.DATA));
				switch (tuple.getInt(ValueFields.DATASET)) {
				case USERS:
					users.add(tuple.getString(ValueFields.DATA));					
					break;
				case USER_LOGS:
					info("user.size() : " + users.size());
					String userLog = tuple.getString(ValueFields.DATA);
					for (String user : users){
						context.write(new Text(user), new Text(userLog));
					}
					break;
				}
			}
		}
	}
}
