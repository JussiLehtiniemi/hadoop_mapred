package com.test.hadoop.mapredtrial;

import org.apache.hadoop.io.Text;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcTimestamp;

public class LogEntryParser {
	
	private static final String INFO = "INFO";
	private static final String WARN = "WARN";
	private static final String ERROR = "ERROR";
	private static final String FATAL = "FATAL";
	private static final String DEBUG = "DEBUG";
	
	private OrcTimestamp timestamp;
	private Text level;
	private Text source;
	private Text message;
	
	public LogEntryParser() {}

	public OrcTimestamp getTimestamp() {
		return timestamp;
	}

	public Text getLevel() {
		return level;
	}

	public Text getSource() {
		return source;
	}

	public Text getMessage() {
		return message;
	}
	
	public boolean isInfo() {
		return this.level.toString().equals(INFO);
	}
	
	public boolean isWarn() {
		return this.level.toString().equals(WARN);
	}
	
	public boolean isError() {
		return this.level.toString().equals(ERROR);
	}
	
	public boolean isFatal() {
		return this.level.toString().equals(FATAL);
	}
	
	public boolean isDebug() {
		return this.level.toString().equals(DEBUG);
	}
	
	public void parseEntry(OrcStruct entry) {
		this.timestamp = (OrcTimestamp) entry.getFieldValue(0);
		this.level = (Text) entry.getFieldValue(1);
		this.source = (Text) entry.getFieldValue(2);
		this.message = (Text) entry.getFieldValue(3);
	}
}
