package com.test.hadoop.mapredtrial;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapredTrial {
	
	public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		
	}

}
