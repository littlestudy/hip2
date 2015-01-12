package org.hip.ch6.sampler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.hip.base.BaseMapReduce;
import org.hip.base.LogReader.JobState;
import org.hip.base.LogReader.ShowLogType;

public class SamplerJob extends BaseMapReduce{

	public SamplerJob() {
		super(SamplerJob.class);
	}
	
	public static void main(String[] args) throws Exception {
		args = new String [] {
				"hdfs://master:9000/user/hadoop/data/names.txt",
				"hdfs://master:9000/user/hadoop/output/samplerJob"
		};
		exec(new SamplerJob(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		Configuration conf = getConf();
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(SamplerJob.class);
		
		ReservoirSamplerInputFormat.setInputFormat(job, TextInputFormat.class);
		
		ReservoirSamplerInputFormat.setNumSamples(job, 10);
		ReservoirSamplerInputFormat.setMaxRecordsToRead(job, 100);
		ReservoirSamplerInputFormat.setUseSamplesNumberPerInputSplit(job, true);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		removeHdfsFile(outputPath);
		
		if (job.waitForCompletion(true)){
			setJobIdAndShowLog(job, JobState.SUCCESS, ShowLogType.IDENTITY);
			return 0;
		}
		setJobIdAndShowLog(job, JobState.FAILURE, ShowLogType.IDENTITY);
		return 1;
	}

}
