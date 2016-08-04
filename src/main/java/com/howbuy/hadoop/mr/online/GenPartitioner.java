package com.howbuy.hadoop.mr.online;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class GenPartitioner extends Partitioner<TaggedKey, Text> {


	@Override
	public int getPartition(TaggedKey tag, Text arg1, int numPartitions) {
		
		return tag.getGuid().hashCode() % numPartitions;
	}

}
