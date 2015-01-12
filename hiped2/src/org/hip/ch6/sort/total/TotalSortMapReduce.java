package org.hip.ch6.sort.total;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.hip.base.BaseMapReduce;
import org.hip.base.LogReader.JobState;
import org.hip.base.LogReader.ShowLogType;

public class TotalSortMapReduce extends BaseMapReduce{

	public TotalSortMapReduce() {
		super(TotalSortMapReduce.class);
	}

	public static void main(String[] args) throws Exception {
		args = new String [] {
			"hdfs://master:9000/user/hadoop/data/names.txt",
			"hdfs://master:9000/user/hadoop/data/large-names-sampled.txt",
			"hdfs://master:9000/user/hadoop/output/TotalSortMapReduce2"
		};
		exec(new TotalSortMapReduce(), args);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		int numReducers = 2;
		
		Path input = new Path(args[0]);
		Path partitionFile = new Path(args[1]);
		Path output = new Path(args[2]);
		
		InputSampler.Sampler<Text, Text> sampler = 
				new InputSampler.RandomSampler<Text, Text>(0.1, 10000, 10);
			
		Configuration conf = getConf();
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(TotalSortMapReduce.class);
		
		job.setNumReduceTasks(numReducers);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setPartitionerClass(TotalOrderPartitioner.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionFile);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		InputSampler.writePartitionFile(job, sampler);
		
		URI partitionUri = new URI(partitionFile.toString()
				+ "#" + "_sortPartitoning");
		//DistributedCache.addCacheFile(partitionUri, conf);
		info("partition uri: " + partitionUri.toString());
		job.addCacheFile(partitionUri);
		removeHdfsFile(output);
		if (job.waitForCompletion(true)){
			setJobIdAndShowLog(job, JobState.SUCCESS, ShowLogType.IDENTITY);
			return 0;
		}
		setJobIdAndShowLog(job, JobState.FAILURE, ShowLogType.IDENTITY);
		return 1;
	}

}
