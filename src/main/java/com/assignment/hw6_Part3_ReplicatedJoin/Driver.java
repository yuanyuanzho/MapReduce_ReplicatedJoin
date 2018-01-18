package com.assignment.hw6_Part3_ReplicatedJoin;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Hello world!
 *
 */
public class Driver 
{	
    public static void main( String[] args ) throws Exception
    {
    	if(args.length != 3){
            System.err.println("Usage: rejoin <input path> <output path>");
            System.exit(1);
        }
    	
    	Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "join");
        
        Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);
        
        
//        DistributedCache.addCacheFile(new Path(args[2]).toUri(), conf);
        
        DistributedCache.addCacheFile(new URI(args[2]), job.getConfiguration());
        job.setJarByClass(Driver.class);
        job.setMapperClass(ReplicatedJoinMapper.class);

        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputDir);

    	// Delete output if exis
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);
        
        job.waitForCompletion(true);
    }
}
