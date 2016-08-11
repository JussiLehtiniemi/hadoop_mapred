package com.test.hadoop.mapredtrial.test;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.util.List;

import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
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

import com.test.hadoop.mapredtrial.LogLevelMapper;

public class TestLogLevelMapper {

	private static final Logger LOG = LoggerFactory.getLogger(TestLogLevelMapper.class);

	private static final String SCHEMA = "struct<eventTime:timestamp,level:string,source:string,message:string>";
	
	private OrcStruct entry;

	private JobConf conf = new JobConf();

	@Before
	public void setUp() throws Exception {
		// Set up ORC input
		LOG.info("Setting up tests...");

		TypeDescription schema = TypeDescription.fromString(SCHEMA);

		this.entry = (OrcStruct) OrcStruct.createValue(schema);

		((OrcTimestamp) this.entry.getFieldValue("eventTime")).set("2016-08-01 11:52:23.042000000");
		((Text) this.entry.getFieldValue("level")).set("ERROR");
		((Text) this.entry.getFieldValue("source")).set("Thread 0");
		((Text) this.entry.getFieldValue("message")).set("Unforeseen error!");
		
		conf.set("io.serializations",
				OrcStructSerialization.class.getName() + "," +
				WritableSerialization.class.getName());
		
		OrcConf.MAPRED_INPUT_SCHEMA.setString(conf, SCHEMA);
		OrcConf.MAPRED_SHUFFLE_VALUE_SCHEMA.setString(conf, SCHEMA);
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testMap() throws Exception {

		LOG.info("Starting testMap test...");

		MapDriver<NullWritable, OrcStruct, DateWritable, OrcValue> driver = new MapDriver<>();
		
		driver.withMapper(new LogLevelMapper())
			.withConfiguration(conf)
			.withInput(NullWritable.get(), this.entry);
		
		List<Pair<DateWritable, OrcValue>> output = driver.run();
		
		assertThat(output.get(0).getFirst().toString(), is("2016-08-01"));
		assertThat((OrcStruct) output.get(0).getSecond().value, equalTo(this.entry));
		
		LOG.info("Test complete");
	}
}
