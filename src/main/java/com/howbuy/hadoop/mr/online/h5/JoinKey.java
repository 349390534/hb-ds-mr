package com.howbuy.hadoop.mr.online.h5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * pv,event联合Key
 * @author yichao.song
 *
 */
@SuppressWarnings("rawtypes")
public class JoinKey implements WritableComparable {
	
	
	private Text joinkey;
	
	private IntWritable joinorder;
	

	public Text getJoinkey() {
		return joinkey;
	}

	public void setJoinkey(Text joinkey) {
		this.joinkey = joinkey;
	}

	public IntWritable getJoinorder() {
		return joinorder;
	}

	public void setJoinorder(IntWritable joinorder) {
		this.joinorder = joinorder;
	}

	
	@Override
	public void write(DataOutput out) throws IOException {
		
		Text.writeString(out, joinkey.toString());
		
		out.writeInt(joinorder.get());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		this.joinkey = new Text(Text.readString(in));
		this.joinorder = new IntWritable(in.readInt());
	}

	@Override
	public int compareTo(Object o) {
		
		JoinKey joinkey = (JoinKey)o;
		
		int compareVal = this.joinkey.compareTo(joinkey.getJoinkey());
		
		if(compareVal == 0){
			
			compareVal = this.joinorder.compareTo(joinkey.getJoinorder());
		}
		return compareVal;
	}
	
	public void set(String joinkey,int order){
		
		this.joinkey = new Text(joinkey);
		this.joinorder = new IntWritable(order);
	}

}
