package org.hip.ch3.avro;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.Writable;

public class AvroBytesRecord {

	public static final String BYTES_FIELD = "b";
	  private static final String SCHEMA_JSON =
          "{\"type\": \"record\", \"name\": \"Bytes\", "
          + "\"fields\": ["
          + "{\"name\":\"" + BYTES_FIELD
          + "\", \"type\":\"bytes\"}]}";
	  
	  //public static final Schema SCHEMA = Schema.parse(SCHEMA_JSON);
	  public static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_JSON);
	  
	  public static GenericRecord toGenericRecord(byte[] bytes){
		  GenericRecord record = new GenericData.Record(SCHEMA);
		  record.put(BYTES_FIELD, ByteBuffer.wrap(bytes));
		  return record;
	  }
	  
	  public static GenericRecord toGenericRecord(Writable writable) throws IOException{
		  ByteArrayOutputStream baos = new ByteArrayOutputStream();
		  DataOutputStream dao = new DataOutputStream(baos);
		  writable.write(dao);
		  dao.close();
		  return toGenericRecord(baos.toByteArray());
	  }
	  
	  public static void fromGenericRecord(GenericRecord r, Writable w) throws IOException{
		  ByteArrayInputStream bais = new ByteArrayInputStream(fromGenericRecord(r));
		  DataInputStream dis = new DataInputStream(bais);
		  w.readFields(dis);
	  }
	  
	  public static byte[] fromGenericRecord(GenericRecord record){
		  ByteBuffer bb = (ByteBuffer)record.get(BYTES_FIELD);
		  byte[] buf = new byte[bb.remaining()];
		  bb.get(buf, 0, bb.remaining());
		  return buf;
	  }
}
