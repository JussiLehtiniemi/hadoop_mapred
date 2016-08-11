package com.test.hadoop.mapredtrial;

import java.io.IOException;
import java.sql.Date;

import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;

public class LogLevelMapper extends Mapper<NullWritable, OrcStruct, DateWritable, OrcValue> {
	
	private LogEntryParser parser = new LogEntryParser();
	private OrcValue valueWrapper = new OrcValue();
	
	@Override
	public void map(NullWritable key, OrcStruct entry, Context context) throws IOException, InterruptedException {
		
		this.parser.parseEntry(entry);
		
		if(parser.isInfo()) {
			DateWritable date = new DateWritable(new Date(parser.getTimestamp().getTime()));
			valueWrapper.value = entry;
			
			context.write(date, valueWrapper);
		}
	}
}
