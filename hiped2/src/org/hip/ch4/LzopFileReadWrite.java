package org.hip.ch4;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.compression.lzo.LzopCodec;

public class LzopFileReadWrite extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        args = new String[] {"hdfs://master:9000/user/hadoop/data/core-site.xml"};
        int res = ToolRunner.run(new Configuration(), new LzopFileReadWrite(), args);
        System.exit(res);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration config = super.getConf();
        
        LzopCodec codec = new LzopCodec();
        codec.setConf(config);
        
        Path srcFile = new Path(args[0]);
        Path restoredFile = new Path(args[0] + ".restored");
        Path destFile = compress(srcFile, config);
        decompress(destFile, restoredFile, config);
        
        return 0;
    }
    
    public static Path compress(Path src, Configuration config) throws IOException{
        Path destFile = new Path(src.toString() + new LzopCodec().getDefaultExtension());
        LzopCodec codec = new LzopCodec();
        codec.setConf(config);
        
        FileSystem hdfs = FileSystem.get(config);
        InputStream is = null;
        OutputStream os = null;
        try {
            is = hdfs.open(src);
            os = codec.createOutputStream(hdfs.create(destFile));
            IOUtils.copyBytes(is, os, config);
        } finally {
            IOUtils.closeStream(os);
            IOUtils.closeStream(is);
        }
        return destFile;
    }
    
    public static void decompress(Path src, Path dest, Configuration config) throws IOException{
        LzopCodec codec = new LzopCodec();
        codec.setConf(config);
        
        FileSystem hdfs = FileSystem.get(config);
        InputStream is = null;
        OutputStream os = null;
        try {
            is = codec.createInputStream(hdfs.open(src));
            os = hdfs.create(dest);
            IOUtils.copyBytes(is, os, config);
        } finally {
            IOUtils.closeStream(os);
            IOUtils.closeStream(is);
        }
    }

}








































