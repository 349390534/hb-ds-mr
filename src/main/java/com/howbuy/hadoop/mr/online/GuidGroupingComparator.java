package com.howbuy.hadoop.mr.online;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GuidGroupingComparator extends WritableComparator {


	protected GuidGroupingComparator() {
		super(TaggedKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		TaggedKey k1 = (TaggedKey)a;
		TaggedKey k2 = (TaggedKey)b;
		
		int compareVal = k1.getGuid().compareTo(k2.getGuid());
//		
//		if(compareVal == 0)
//			compareVal = (int)(Long.parseLong(k1.getTimestamp()) - Long.parseLong(k2.getTimestamp()));
		
		return compareVal;
	}

	
}
