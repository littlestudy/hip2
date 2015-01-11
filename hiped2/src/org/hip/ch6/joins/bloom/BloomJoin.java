package org.hip.ch6.joins.bloom;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.hip.base.BaseMapReduce;
import org.hip.base.LogReader.JobState;
import org.hip.base.LogReader.ShowLogType;
import org.hip.ch6.joins.User;
import org.hip.ch6.joins.UserLog;
import org.htuple.Tuple;

import com.google.common.collect.Lists;

public class BloomJoin extends BaseMapReduce{

	public BloomJoin() {
		super(BloomJoin.class);
	}

	enum ValueFields{
		DATASET, DATA
	}
	
	public static final int USERS = 0;
	public static final int USER_LOGS = 1;
	
	public static void main(String[] args) throws Exception {
		args = new String [] {
				"hdfs://master:9000/user/hadoop/data/users.txt",
				"hdfs://master:9000/user/hadoop/data/user-logs.txt",
				"hdfs://master:9000/user/hadoop/output/bloom-output/part-r-00000.avro",
				"hdfs://master:9000/user/hadoop/data/filter-output"
		};
		exec(new BloomJoin(), args);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Path usersPath = new Path(args[0]);
		Path userLogsPath = new Path(args[1]);
		Path bloomPath = new Path(args[2]);
		Path outputPath = new Path(args[3]);
		
		Configuration conf = getConf();
		
		Job job =Job.getInstance(conf);
		job.setJarByClass(BloomJoin.class);
		
		MultipleInputs.addInputPath(job, usersPath, TextInputFormat.class, UserMap.class);
		MultipleInputs.addInputPath(job, userLogsPath, TextInputFormat.class, UserLogMap.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Tuple.class);
		
		job.setReducerClass(ReducerClass.class);
		
		job.addCacheFile(bloomPath.toUri());
		info("bloomPath.toUri: " + bloomPath.toUri().toString());
		job.getConfiguration().set(AbstractFilterMap.DISTCACHE_FILENAME_CONFIG, bloomPath.getName());
		
		//FileInputFormat.setInputPaths(job, userLogsPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		removeHdfsFile(outputPath);
		
		if (job.waitForCompletion(true)){
			setJobIdAndShowLog(job, JobState.SUCCESS, ShowLogType.IDENTITY);
			return 0;
		}
		setJobIdAndShowLog(job, JobState.FAILURE, ShowLogType.IDENTITY);
		return 1;
	}
	
	public static abstract class AbstractFilterMap extends BaseMapper<LongWritable, Text, Text, Tuple>{

		public AbstractFilterMap(Class<?> cls) {
			super(cls);
		}
		
		public static final String DISTCACHE_FILENAME_CONFIG = "bloomjoin.distcache.filename";
		private BloomFilter filter;
		
		abstract String getUserName(Text value);
		abstract int getDataset();
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			final String distributedCacheFilename = context.getConfiguration().get(DISTCACHE_FILENAME_CONFIG);
			info("distributedCacheFile name: " + distributedCacheFilename);
			filter = BloomFilterCreator.fromFile(new File(distributedCacheFilename));
			info("distributedCacheFile path: " + new File(distributedCacheFilename).getParent());
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String user = getUserName(value);
			if (filter.membershipTest(new Key(user.getBytes()))){
				Tuple outputValue = new Tuple();
				outputValue.setInt(ValueFields.DATASET, getDataset());
				outputValue.setString(ValueFields.DATA, value.toString());
				
				context.write(new Text(user), outputValue);
			}
		}
	}
	
	public static class UserMap extends AbstractFilterMap{

		public UserMap() {
			super(UserMap.class);
		}

		@Override
		String getUserName(Text value) {
			return User.fromText(value).getName();
		}

		@Override
		int getDataset() {
			return USERS;
		}		
	}
	
	public static class UserLogMap extends AbstractFilterMap{

		public UserLogMap() {
			super(UserLogMap.class);
		}

		@Override
		String getUserName(Text value) {
			return UserLog.fromText(value).getName();
		}

		@Override
		int getDataset() {
			return USER_LOGS;
		}		
	}
	
	public static class ReducerClass extends BaseReduce<Text, Tuple, Text, Text>{

		List<String> users;
		List<String> userLogs;
		
		public ReducerClass() {
			super(ReducerClass.class);
		}
		
		@Override
		protected void reduce(Text key, Iterable<Tuple> values, Context context)
				throws IOException, InterruptedException {
			users = Lists.newArrayList();
			userLogs = Lists.newArrayList();
			
			for (Tuple tuple : values){
				info("Tuple: " + tuple);
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
