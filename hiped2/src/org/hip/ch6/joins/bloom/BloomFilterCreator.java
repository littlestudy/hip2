package org.hip.ch6.joins.bloom;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.hip.base.BaseMapReduce;
import org.hip.base.LogReader.JobState;
import org.hip.base.LogReader.ShowLogType;
import org.hip.ch3.avro.AvroBytesRecord;
import org.hip.ch6.joins.User;

public class BloomFilterCreator extends BaseMapReduce{

	public BloomFilterCreator() {
		super(BloomFilterCreator.class);
	}

	public static void main(String[] args) throws Exception {
		args = new String[] {
				"hdfs://master:9000/user/hadoop/data/users.txt",
				"hdfs://master:9000/user/hadoop/output/bloom-output"
		};
		exec(new BloomFilterCreator(), args);		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		Path usersPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		Configuration conf = getConf();
		
		Job job =Job.getInstance(conf);
		job.setJarByClass(BloomFilterCreator.class);
		
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		
		AvroJob.setOutputKeySchema(job, AvroBytesRecord.SCHEMA);
		job.getConfiguration().set(AvroJob.CONF_OUTPUT_CODEC,SnappyCodec.class.getName());
		
		job.setOutputFormatClass(AvroKeyOutputFormat.class);
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(BloomFilter.class);
		
		FileInputFormat.setInputPaths(job, usersPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		removeHdfsFile(outputPath);
		
		job.setNumReduceTasks(1);
		if (job.waitForCompletion(true)){
			setJobIdAndShowLog(job, JobState.SUCCESS, ShowLogType.IDENTITY);
			return 0;
		}		
		
		return 1;
	}	
	
	public static class MapperClass extends BaseMapper<LongWritable, Text, NullWritable, BloomFilter>{

		private BloomFilter filter = new BloomFilter(1000, 5, Hash.MURMUR_HASH);
		
		public MapperClass() {
			super(MapperClass.class);
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			User user = User.fromText(value);
			if ("CA".equals(user.getState())){
				info("map --> " + user.getName());
				filter.add(new Key(user.getName().getBytes()));
			}
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			context.write(NullWritable.get(), filter);
		}
	}
	
	public static class ReducerClass extends BaseReduce<NullWritable, BloomFilter, AvroKey<GenericRecord>, NullWritable>{

		private BloomFilter filter = new BloomFilter(1000, 5, Hash.MURMUR_HASH);
		
		public ReducerClass() {
			super(ReducerClass.class);
		}
		
		@Override
		protected void reduce(NullWritable key, Iterable<BloomFilter> values, Context arg2)
				throws IOException, InterruptedException {
			for (BloomFilter bf : values)
				filter.or(bf);
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			context.write(new AvroKey<GenericRecord>(AvroBytesRecord.toGenericRecord(filter)), NullWritable.get());
			
		}
	}
	
	public static BloomFilter readFromAvro(InputStream is) throws IOException{
		DataFileStream<Object> reader = new DataFileStream<Object>(is, new GenericDatumReader<Object>());
		reader.hasNext();
		BloomFilter filter = new BloomFilter();
		AvroBytesRecord.fromGenericRecord((GenericRecord) reader.next(), filter);
		IOUtils.closeStream(is);
		IOUtils.closeStream(reader);
		
		return filter;
	}
	
	public static BloomFilter fromFile(File f) throws IOException{
		return readFromAvro(FileUtils.openInputStream(f));
	}

}