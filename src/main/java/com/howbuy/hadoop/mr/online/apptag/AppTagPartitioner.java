package com.howbuy.hadoop.mr.online.apptag;

import org.apache.hadoop.mapreduce.Partitioner;

public class AppTagPartitioner extends Partitioner<AppTag,AppTag> {


	@Override
	public int getPartition(AppTag key, AppTag value, int numPartitions) {
		
		return (key.getGuid().hashCode() & 0x7FFFFFFF) % numPartitions;
		
	}
	
	
	public static void main(String[] args){
		
		String s = "EE60803E-B15F-48E4-BBB0-E6D970CCC623";
		
		System.out.println(s.hashCode());
		
		System.out.println(s.hashCode() & 0x7FFFFFFF);
		
		System.out.println(s.hashCode() & 1l);
		
		
	}

}
