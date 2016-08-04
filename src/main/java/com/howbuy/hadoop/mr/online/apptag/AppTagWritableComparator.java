package com.howbuy.hadoop.mr.online.apptag;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class AppTagWritableComparator extends WritableComparator {
	
	protected AppTagWritableComparator() {
		
		super(AppTag.class,true);
		
	}
	
	
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		AppTag k1 = (AppTag)a;
		
		AppTag k2 = (AppTag)b;
		
		int compVal = k1.getGuid().compareTo(k2.getGuid());
		
		if(compVal == 0)
			compVal = k1.getCustno().compareTo(k2.getCustno());
		
		return compVal;
	}
	
	



//	static {  
//        WritableComparator.define(AppTagWritableComparator.class, new AppTagWritableComparator());  
//    } 

//	@Override
//	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		
//		try {
//			int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
//			int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
//			
//			return txComparator.compare(b1, s1, firstL1, b2, s2, firstL2);
//			
//		} catch (IOException e) {
//			
//			throw new IllegalArgumentException(e);
//		}
		
//		return txComparator.compare(b1, s1, l1, b2, s2, l2);
//	}



	
	

}
