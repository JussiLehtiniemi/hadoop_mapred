package com.test.hadoop.mapredtrial.test;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcTimestamp;
import org.apache.orc.mapred.OrcValue;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.test.hadoop.mapredtrial.LogLevelReducer;

public class TestLogLevelReducer {
	
	private static final Logger LOG = LoggerFactory.getLogger(TestLogLevelReducer.class);
	
	private static final String SCHEMA = "struct<eventTime:timestamp,level:string,source:string,message:string>";
	
	private JobConf conf = new JobConf();
	
	private List<Pair<DateWritable, List<OrcValue>>> input;

	@Before
	public void setUp() throws Exception {
		
		LOG.info("Setting up tests...");
		
		// Generate input
		
		TypeDescription schema = TypeDescription.fromString(SCHEMA);

		OrcStruct entry = (OrcStruct) OrcStruct.createValue(schema);

		((OrcTimestamp) entry.getFieldValue("eventTime")).set("2016-08-01 11:52:23.042000000");
		((Text) entry.getFieldValue("level")).set("ERROR");
		((Text) entry.getFieldValue("source")).set("Thread 0");
		((Text) entry.getFieldValue("message")).set("Unforeseen error!");
		
		SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd");
		DateWritable key1 = new DateWritable(new Date(fmt.parse("2016-07-30").getTime()));
		DateWritable key2 = new DateWritable(new Date(fmt.parse("2016-08-01").getTime()));
		
		List<OrcValue> values1 = new ArrayList<>();
		List<OrcValue> values2 = new ArrayList<>();
		
		for(int i=0; i<4; i++) {
			values1.add(new OrcValue(entry));
		}
		
		for(int i=0; i<8; i++) {
			values2.add(new OrcValue(entry));
		}
		
		this.input = new ArrayList<>();
		this.input.add(new Pair<DateWritable, List<OrcValue>>(key1, values1));
		this.input.add(new Pair<DateWritable, List<OrcValue>>(key2, values2));
		
		// Config
		this.conf.set("io.serializations",
				OrcStructSerialization.class.getName() + "," +
				WritableSerialization.class.getName());
		
		OrcConf.MAPRED_SHUFFLE_VALUE_SCHEMA.setString(this.conf, "struct<eventTime:timestamp,level:string,source:string,message:string>");
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testReduce() throws Exception {
		
		LOG.info("Starting testReduce test...");
		
		ReduceDriver<DateWritable, OrcValue, Text, IntWritable> driver = new ReduceDriver<>();
		
		driver.withReducer(new LogLevelReducer())
			.withConfiguration(this.conf)
			.addAll(this.input);
		
		List<Pair<Text, IntWritable>> output = driver.run();
		
		assertThat(output.size(), is(2));
		assertThat(output.get(0).getFirst().toString(), is("2016-07-30"));
		assertThat(output.get(0).getSecond().get(), is(4));
		assertThat(output.get(1).getFirst().toString(), is("2016-08-01"));
		assertThat(output.get(1).getSecond().get(), is(8));
	}
}
