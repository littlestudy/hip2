package org.hip.ch6.joins.replicated.simple;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.hip.base.BaseMapReduce;
import org.hip.base.LogReader.JobState;
import org.hip.base.LogReader.ShowLogType;
import org.hip.ch6.joins.User;
import org.hip.ch6.joins.UserLog;

public class ReplicatedJoin extends BaseMapReduce{

	public ReplicatedJoin() {
		super(ReplicatedJoin.class);
	}

	public static void main(String[] args) throws Exception {
		args = new String[] {
				"hdfs://master:9000/user/hadoop/data/users.txt",
				"hdfs://master:9000/user/hadoop/data/user-logs.txt",
				"hdfs://master:9000/user/hadoop/output/replicatedJoin",
		};
		exec(new ReplicatedJoin(), args);
	}
	
	@Override
	public int run(String[] args) throws Exception {

		Path usersPath = new Path(args[0]);
		Path userLogsPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);
		
		Configuration conf = getConf();
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(ReplicatedJoin.class);
		
		job.setMapperClass(JoinMap.class);
		
		job.addCacheFile(usersPath.toUri());
		job.getConfiguration().set(JoinMap.DISTCACHE_FILENAME_CONFIG, usersPath.getName());
		
		job.setNumReduceTasks(0);
		
		FileInputFormat.setInputPaths(job, userLogsPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		removeHdfsFile(outputPath);
		
		if (job.waitForCompletion(true)){
			setJobIdAndShowLog(job, JobState.SUCCESS, ShowLogType.IDENTITY);
			return 0;
		}
		
		setJobIdAndShowLog(job, JobState.FAILURE, null);
		return 1;		
	}
	
	public static class JoinMap extends BaseMapper<LongWritable, Text, Text, Text>{

		public static final String DISTCACHE_FILENAME_CONFIG = "	replicatedjoin.distcache.filename";
		private Map<String, User> users = new HashMap<String, User>();
		
		public JoinMap() {
			super(JoinMap.class);
		}
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {			
			
			final String distributedCacheFilename = context.getConfiguration().get(DISTCACHE_FILENAME_CONFIG);
						
			File cacheFile = new File(distributedCacheFilename);
			if (cacheFile.exists()){
				info("load cache file:" + cacheFile.toString());
				loadCache(cacheFile);
			}
			else 
				throw new IOException("Unable to find file " + distributedCacheFilename);
			
			/*
			boolean found = false;
			for(URI uri : files){
				info("Distcache file: " + uri);
				File path = new File(uri.getPath());
				
				if (path.getName().equals(distributedCacheFilename)){
					loadCache(path);
					found = true;
					break;
				}
			}
			
			if (!found)
				throw new IOException("Unable to find file " + distributedCacheFilename);
			*/
		}
		
		private void loadCache(File file) throws IOException{
			for (String line : FileUtils.readLines(file)){
				User user = User.fromString(line);
				users.put(user.getName(), user);
			}
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			UserLog userLog = UserLog.fromText(value);
			User user = users.get(userLog.getName());
			if (user != null)
				context.write(new Text(user.toString()), new Text(userLog.toString()));
		}
		
	}

}
