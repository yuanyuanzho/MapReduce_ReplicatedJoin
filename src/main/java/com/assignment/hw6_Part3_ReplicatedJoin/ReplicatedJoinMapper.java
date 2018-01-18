package com.assignment.hw6_Part3_ReplicatedJoin;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import javax.xml.transform.OutputKeys;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class ReplicatedJoinMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	private HashMap<String, String> userIdToInfo = new HashMap<String, String>();
	
	private Text outvalue = new Text();
	
	public void setup(Context context) throws IOException, InterruptedException{
		
        try {
            Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            if (files != null && files.length > 0) {
		        for (Path file : files) {
		
		            try {
		                File myFile = new File(file.toUri());
		                BufferedReader bufferedReader = new BufferedReader(new FileReader(myFile.toString()));
					
					
						String line;
						
						 while ((line = bufferedReader.readLine()) != null) {
							String[] tokens = line.split(";");
							String userId = tokens[0];
							userIdToInfo.put(userId, line);
						 }
		            } catch (IOException ex) {
		                System.err.println("Exception while reading  file: " + ex.getMessage());
		            }
		        }
            }
		} catch (IOException ex) {
		    System.err.println("Exception in mapper setup: " + ex.getMessage());
		}
	}
	
	 public void map(LongWritable key, Text value, Context context) 
			 throws IOException, InterruptedException {
		
		 String[] fields = value.toString().split(";");
		 String userId = fields[0];
		 String bookInformation = userIdToInfo.get(userId);
		 
		 if(bookInformation != null){
			 outvalue.set(bookInformation);
			 context.write(value,outvalue);
		 } 

	 }
}



