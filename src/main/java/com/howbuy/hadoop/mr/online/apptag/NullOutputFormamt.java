package com.howbuy.hadoop.mr.online.apptag;

import java.io.IOException;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NullOutputFormamt<K,V> extends FileOutputFormat<K, V> {

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		
		return null;
	}

	@Override
	public void checkOutputSpecs(JobContext job)
			throws FileAlreadyExistsException, IOException {
	}

}
