package org.hip.base;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LogReader extends Configured implements Tool{	
	
	public static final String LOG_IDENTITY = "[ --- marked info -- ]";
	public static final String LOG_DIR_ROOT = "hdfs://master:9000/tmp/logs/hadoop/logs/";
	public static final String LOG_PREIFX = "application";
	public static final String LOCAL_JOBID_FILE = "/tmp/jobInfo";
	public static final String LOCAL_JOBID_SEPATATOR = "\t";	
	
	private static final Log LOG = LogFactory.getLog(LogReader.class); 	
	private StringBuilder sb = new StringBuilder();
	
	public enum ShowLogType{
		NO, FULL, IDENTITY
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),  new LogReader(), args);
		System.exit(res);
	}
	
	public static String getJobIdInfo(String jobId, ShowLogType show){
		return jobId + LOCAL_JOBID_SEPATATOR + show.toString();
	}
	
	@Override
	public int run(String[] args) throws Exception {
		BufferedReader reader = null;		
		try {
			reader = new BufferedReader(new FileReader(LOCAL_JOBID_FILE));
			String [] parts = reader.readLine().split(LOCAL_JOBID_SEPATATOR);
			String jobId = parts[0];
			ShowLogType showType = ShowLogType.valueOf(parts[1]);
			showLog(jobId, showType);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(reader);
		}
		return 0;
	}

	private void showLog(String jobId, ShowLogType show) throws Exception {
		Path logDir = new Path(LOG_DIR_ROOT + LOG_PREIFX + jobId.substring(3));
		
		switch (show) {
		case NO:
			LOG.info("Do not show the log(s).");
			break;
		case FULL:
			showFullLog(logDir);
			break;	
		case IDENTITY:
			showIdentifidLog(logDir);
			break;
		default:
			throw new Exception("Show Type is illegal.");
		}
	}
	
	private void showIdentifidLog(Path logDir) throws IOException {
		FileSystem fileSystem = logDir.getFileSystem(getConf());
		FileStatus[] status = fileSystem.listStatus(logDir);		
		for (FileStatus statu : status){
			addInfo("Log File: " + statu.getPath().toString());
			BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(statu.getPath())));
			String line = null;
			int index = -1;
			while ((line = reader.readLine()) != null){
				index = line.indexOf(LOG_IDENTITY);
				if (index != -1){
					addInfo(line.substring(index + LOG_IDENTITY.length()));
				}
			}
			addInfo();
		}
		setAllInfo();
	}

	private void showFullLog(Path logDir) throws IOException {
		FileSystem fileSystem = logDir.getFileSystem(getConf());
		FileStatus[] status = fileSystem.listStatus(logDir);		
		for (FileStatus statu : status){
			addInfo("Log File: " + statu.getPath().toString());
			BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(statu.getPath())));
			String line = null;
			while ((line = reader.readLine()) != null){
				addInfo(line);
			}
			addInfo();
		}
		setAllInfo();
	}

	private void addInfo(String info){
		sb.append(info + "\n");
	}	
	
	private void addInfo(){
		addInfo("");
	}
	
	private void setAllInfo(){
		LOG.info("log info \n\n" + sb.toString());
	}
}