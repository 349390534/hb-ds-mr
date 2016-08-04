package com.howbuy.hadoop.mr.online;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TaggedKey implements  WritableComparable<TaggedKey> {
	
	
	private String guid;
	
	private String timestamp;
	
	

	@Override
	public int compareTo(TaggedKey o) {
		
		if(PVAnly.isEmpty(o.getGuid()) || PVAnly.isEmpty(o.getTimestamp()) 
					|| PVAnly.isEmpty(this.guid) || PVAnly.isEmpty(this.timestamp))
			return 0;
		
		int compareVal = this.guid.compareTo(o.getGuid());
		if(compareVal == 0){
			compareVal = (int)(Long.parseLong(this.timestamp) - Long.parseLong(o.getTimestamp()));
		}
		
		return compareVal;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		
		guid = Text.readString(in);
		timestamp = Text.readString(in);
//		guid = in.readUTF();
//		timestamp = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		
		Text.writeString(out, guid);
		Text.writeString(out, timestamp);
//		out.writeUTF(guid);
//		out.writeUTF(timestamp);
	}

	public String getGuid() {
		return guid;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setGuid(String guid) {
		this.guid = guid;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

}
