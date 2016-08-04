package com.howbuy.hadoop.mr.online;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ChannelTagClickComparator extends WritableComparator {

	protected ChannelTagClickComparator() {
		super(ChannelTagClickKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		ChannelTagClickKey c1 = (ChannelTagClickKey)a;
		ChannelTagClickKey c2 = (ChannelTagClickKey)b;
		int compareVal = c1.compareTo(c2);
		return compareVal;
	}

	
}
