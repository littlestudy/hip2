package org.hip.ch6.sort.secondary;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.hip.base.BaseMapReduce;
import org.hip.base.LogReader.JobState;
import org.hip.base.LogReader.ShowLogType;

public class SortMapReduce  extends BaseMapReduce{

	public SortMapReduce() {
		super(SortMapReduce.class);
	}

	public static void main(String[] args) throws Exception {
		args = new String [] {
				"hdfs://master:9000/user/hadoop/data/username.txt",
				"hdfs://master:9000/user/hadoop/output/sortUserName"
		};
		exec(new SortMapReduce(), args);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		Configuration conf = getConf();
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(SortMapReduce.class);
		
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		job.setMapOutputKeyClass(Person.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setPartitionerClass(PersonNamePartitioner.class);
		job.setSortComparatorClass(PersonComparator.class);
		job.setGroupingComparatorClass(PersonNameComparator.class);
		
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

	public static class MapperClass extends BaseMapper<Text, Text, Person, Text>{

		private Person outputKey = new Person();
		
		public MapperClass() {
			super(MapperClass.class);
		}
		
		@Override
		protected void map(Text lastName, Text firstName, Context context)
				throws IOException, InterruptedException {
			outputKey.set(lastName.toString(), firstName.toString());
			context.write(outputKey, firstName);
		}		
	}
	
	public static class ReducerClass extends BaseReduce<Person, Text, Text, Text>{
		
		Text lastName = new Text();

		public ReducerClass() {
			super(ReducerClass.class);
		}

		@Override
		protected void reduce(Person key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			lastName.set(key.getLastName());
			for (Text firstName : values)
				context.write(lastName, firstName);
		}
		
	}
}
