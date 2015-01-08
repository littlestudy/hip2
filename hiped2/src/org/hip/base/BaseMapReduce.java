package org.hip.base;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public abstract class BaseMapReduce extends Configured implements Tool{
	private final Log LOG; 
	
	public BaseMapReduce(Class<?> cls){
		LOG = LogFactory.getLog(cls);
	}
	
	public void info(String info){		
		LOG.info("----------------> " + info);
	}
	
	public static void exec(Tool tool, String[] args) throws Exception{
		int res = ToolRunner.run(new Configuration(), tool, args);
		System.exit(res);
	}
	
}
