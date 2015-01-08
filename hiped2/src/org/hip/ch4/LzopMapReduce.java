package org.hip.ch4;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.compression.lzo.LzoCodec;
import com.hadoop.compression.lzo.LzoIndex;
import com.hadoop.compression.lzo.LzoIndexer;
import com.hadoop.compression.lzo.LzopCodec;
import com.hadoop.mapreduce.LzoTextInputFormat;

public class LzopMapReduce extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        args = new String [] {
            "hdfs://master:9000/user/hadoop/data/core-site.xml",
            "hdfs://master:9000/user/hadoop/output/core-site"
        };
        
        int res = ToolRunner.run(new Configuration(), new LzopMapReduce(), args);
        System.exit(res);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        
        Configuration conf = super.getConf();
        
        Path compressedInputFile = compressAndIndex(input, conf);
        
        conf.setBoolean("mapred.compress.map.output", true);
        conf.setClass("mapred.map.output.compression.codec",
                LzoCodec.class, CompressionCodec.class);
        
        Job job = new Job(conf);
        job.setJarByClass(LzopMapReduce.class);
        
        job.setMapperClass(Mapper.class);
        job.setReducerClass(Reducer.class);
        
        job.setInputFormatClass(LzoTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().setClass("mapred.output.compression.codec",
                LzoCodec.class, CompressionCodec.class);
        
        FileInputFormat.addInputPath(job, compressedInputFile);
        FileOutputFormat.setOutputPath(job, output);
        
        if (job.waitForCompletion(true))
            return 0;
        return 1;
    }

    public static Path compressAndIndex(Path file, Configuration conf) throws IOException{
        Configuration tmpConfig = new Configuration(conf);
        tmpConfig.setLong("dfs.block.size", 1048576);
        tmpConfig.setInt(LzopCodec.LZO_BUFFER_SIZE_KEY, 1048576);
        
        Path compressedFile = LzopFileReadWrite.compress(file, tmpConfig);
        
        compressedFile.getFileSystem(tmpConfig).delete(
                new Path(compressedFile.toString() + LzoIndex.LZO_INDEX_SUFFIX), false);
        new LzoIndexer(tmpConfig).index(compressedFile);
        
        LzoIndex index = LzoIndex.readIndex(compressedFile.getFileSystem(tmpConfig),
                compressedFile);
        for (int i = 0; i < index.getNumberOfBlocks(); i++){
            System.out.println("block[" + i + "] = " + index.getPosition(i));
        }
        
        Job job = new Job(conf);
        job.setInputFormatClass(LzoTextInputFormat.class);
        LzoTextInputFormat inputFormat = new LzoTextInputFormat();
        TextInputFormat.setInputPaths(job, compressedFile);
        
        List<InputSplit> is = inputFormat.getSplits(job);
        
        System.out.println("input splits = " + is.size());
        
        return compressedFile;
    }
}
