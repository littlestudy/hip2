package org.hip.base;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public abstract class BaseMapReduce extends Configured implements Tool{
	private final Log LOG; 
	private static final String LOG_IDENTITY = "----------";
	
	public enum ShowLog{
		NO, FULL, IDENTITY
	}
	
	public BaseMapReduce(Class<?> cls){
		LOG = LogFactory.getLog(cls);
	}
	
	public void info(String info){		
		LOG.info(LOG_IDENTITY + info);
	}
	
	public static void exec(Tool tool, String[] args, ShowLog show) throws Exception{
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
	
}
