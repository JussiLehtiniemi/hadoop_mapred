package com.test.hadoop.mapredtrial.test;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.orc.OrcConf;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acn.hadoop.tools.orctools.OrcReader;
import com.test.hadoop.mapredtrial.LogLevelMapper;
import com.test.hadoop.mapredtrial.LogLevelReducer;

public class TestMapReduce {
	
	private static final Logger LOG = LoggerFactory.getLogger(TestMapReduce.class);
	
	private static final String TESTFILE = "test/test_apr-2016.orc";
	private static final String SCHEMA = "struct<eventTime:timestamp,level:string,source:string,message:string>";
	
	private JobConf conf;
	
	private List<Pair<NullWritable, OrcStruct>> input;

	@Before
	public void setUp() throws Exception {
		
		LOG.info("Setting up tests...");
		
		// Read test data from ORC file
		
		OrcReader rdr = new OrcReader();
		rdr.setFilePath(TESTFILE);
		
		List<OrcStruct> data = rdr.readRecords();
		
		this.input = new ArrayList<>();
		
		for(OrcStruct record : data) {
			Pair<NullWritable, OrcStruct> pair = new Pair<>(NullWritable.get(), record);
			this.input.add(pair);
		}
		
		LOG.info("Running test with " + this.input.size() + " records");
		
		// Configuration
		this.conf = new JobConf();
		
		this.conf.set("io.serializations",
				OrcStructSerialization.class.getName() + "," +
				WritableSerialization.class.getName());
		
		OrcConf.MAPRED_INPUT_SCHEMA.setString(conf, SCHEMA);
		OrcConf.MAPRED_SHUFFLE_VALUE_SCHEMA.setString(conf, SCHEMA);
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testMapReduce() throws Exception {
		
		MapReduceDriver<NullWritable, OrcStruct, DateWritable, OrcValue, Text, IntWritable> driver = new MapReduceDriver<>(new LogLevelMapper(), new LogLevelReducer());
		
		driver.withConfiguration(conf)
			.addAll(this.input);
		
		List<Pair<Text, IntWritable>> output = driver.run();
		
		assertThat(output.size(), is(not(0)));
		
		// Print output
		
		System.out.println("Results:");
		System.out.println("[Date]\t\t[Number of entries]");
		
		for(Pair<Text, IntWritable> p : output) {
			System.out.println(p.getFirst().toString() + "\t" + p.getSecond().get());
		}
	}

}
