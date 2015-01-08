package org.hip.base;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public abstract class BaseMapReduce extends Configured implements Tool{
	private final Log LOG; 
	private static final String LOG_IDENTITY = "----------";
	private static final String LOG_DIR = "hdfs://master:9000/tmp/logs/hadoop/logs/";
	
	public enum ShowLog{
		NO, FULL, IDENTITY
	}
	
	public BaseMapReduce(Class<?> cls){
		LOG = LogFactory.getLog(cls);
	}
	
	public void info(String info){		
		LOG.info(LOG_IDENTITY + info);
	}
	
	public static void exec(Tool tool, String[] args) throws Exception{
		int res = ToolRunner.run(new Configuration(), tool, args);
		System.exit(res);
	}
	
	public static class BaseReduce<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>{
		private final Log LOG;
		
		public BaseReduce(Class<?> cls){
			LOG = LogFactory.getLog(cls);
		}
		
		public void info(String info){		
			LOG.info(LOG_IDENTITY + info);
		}
	}
	
	public String showLogInfo(String jobId, Configuration conf, ShowLog show){
		try {
			Thread.sleep(3000);			
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		String pathStr = LOG_DIR + "application" + jobId.substring(3) + "/master_35132";		
		Path logPath = new Path(pathStr);
		try {
			PrintStream ps = new PrintStream(System.out);
			InputStream is = logPath.getFileSystem(conf).open(logPath);
			IOUtils.copyBytes(is, ps, conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return pathStr;
	}
	
}
