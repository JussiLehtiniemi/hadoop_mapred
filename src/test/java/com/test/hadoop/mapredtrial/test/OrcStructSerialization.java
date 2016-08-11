package com.test.hadoop.mapredtrial.test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;

/**
 * This class is intended to support MRUnit's object copying for input and
 * output objects.
 *
 * Real mapreduce contexts should NEVER use this class.
 *
 * The type string is serialized before each value.
 */

public class OrcStructSerialization implements Serialization<OrcStruct> {
	@Override
	public boolean accept(Class<?> cls) {
		return OrcStruct.class.isAssignableFrom(cls);
	}
	@Override
	public Serializer<OrcStruct> getSerializer(Class<OrcStruct> aClass) {
		return new Serializer<OrcStruct>() {
			DataOutputStream dataOut;
			public void open(OutputStream out) {
				if(out instanceof DataOutputStream) {
					dataOut = (DataOutputStream)out;
				} else {
					dataOut = new DataOutputStream(out);
				}
			}
			public void serialize(OrcStruct w) throws IOException {
				Text.writeString(dataOut, w.getSchema().toString());
				w.write(dataOut);
			}
			public void close() throws IOException {
				dataOut.close();
			}
		};
	}
	@Override
	public Deserializer<OrcStruct> getDeserializer(Class<OrcStruct> aClass) {
		return new Deserializer<OrcStruct>() {
			DataInputStream input;
			@Override
			public void open(InputStream inputStream) throws IOException {
				if(inputStream instanceof DataInputStream) {
					input = (DataInputStream)inputStream;
				} else {
					input = new DataInputStream(inputStream);
				}
			}
			@Override
			public OrcStruct deserialize(OrcStruct orcStruct) throws IOException {
				String typeStr = Text.readString(input);
				OrcStruct result = new OrcStruct(TypeDescription.fromString(typeStr));
				result.readFields(input);
				return result;
			}
			@Override
			public void close() throws IOException {
				// PASS
			}
		};
	}
}

