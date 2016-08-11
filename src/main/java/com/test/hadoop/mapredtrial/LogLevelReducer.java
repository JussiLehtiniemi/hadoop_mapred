package com.test.hadoop.mapredtrial;

import java.io.IOException;

import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.orc.mapred.OrcValue;

public class LogLevelReducer extends Reducer<DateWritable, OrcValue, Text, IntWritable> {
	
	@Override
	public void reduce(DateWritable date, Iterable<OrcValue> values, Context context) throws IOException, InterruptedException {
		
		int cnt = 0;
		
		for(@SuppressWarnings("unused") OrcValue value : values) {
			cnt++;
		}
		
		context.write(new Text(date.toString()), new IntWritable(cnt));
	}

}
