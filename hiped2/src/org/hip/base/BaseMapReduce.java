package org.hip.base;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hip.base.LogReader.ShowLogType;

public abstract class BaseMapReduce extends Configured implements Tool{
	
	private final Log LOG; 
		
	public static String jobId_;
	public static ShowLogType show_;
	
	public BaseMapReduce(Class<?> cls){
		LOG = LogFactory.getLog(cls);
	}
	
	public void info(String info){		
		LOG.info(LogReader.LOG_IDENTITY + info);
	}
	
	public static void exec(Tool tool, String[] args) throws Exception{
		int res = ToolRunner.run(new Configuration(), tool, args);
		saveJobId(jobId_, show_);
		System.exit(res);
	}
	
	public void setJobIdAndShowLog(String jobId, ShowLogType show){
		jobId_ = jobId;
		show_ = show;
	}
	
	public static void saveJobId(String jobId, ShowLogType show){
		PrintStream ps = null;
		try {
			ps = new PrintStream(LogReader.LOCAL_JOBID_FILE);
			ps.println(LogReader.getJobIdInfo(jobId, show));
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(ps);
		}		
	}	
	
	public static class BaseMapper <KEYIN, VALUEIN, KEYOUT, VALUEOUT> 
			extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>{
		private final Log LOG;
		
		public BaseMapper(Class<?> cls){
			LOG = LogFactory.getLog(cls);
		}
		
		public void info(String info){		
			LOG.info(LogReader.LOG_IDENTITY + info);
		}
	}
	
	public static class BaseReduce<KEYIN, VALUEIN, KEYOUT, VALUEOUT> 
			extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>{
		private final Log LOG;
		
		public BaseReduce(Class<?> cls){
			LOG = LogFactory.getLog(cls);
		}
		
		public void info(String info){		
			LOG.info(LogReader.LOG_IDENTITY + info);
		}
	}
}
