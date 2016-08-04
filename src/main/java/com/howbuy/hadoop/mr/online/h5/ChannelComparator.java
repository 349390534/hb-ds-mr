package com.howbuy.hadoop.mr.online.h5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class ChannelComparator extends WritableComparator {

	public ChannelComparator(){
		
		super(Text.class,true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
//		String t1 = ((Text)a).toString();
//		
//		String t2 = ((Text)b).toString();
//		
//		String channel1 = t1.substring(0,t1.indexOf(":"));
//		
//		String channel2 = t2.substring(0,t2.indexOf(":"));
//		
//		return channel1.compareTo(channel2);
		
		return a.compareTo(b);
	}

//	@Override
//	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
//		
//		
//		// TODO Auto-generated method stub
//		return super.compare(b1, s1, l1, b2, s2, l2);
//	}
	
	
}
