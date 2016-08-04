package com.howbuy.hadoop.mr.online.h5;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.howbuy.hadoop.mr.online.BaseAnly;
import com.howbuy.hadoop.mr.online.GenPartitioner;
import com.howbuy.hadoop.mr.online.GuidGroupingComparator;
import com.howbuy.hadoop.mr.online.TaggedKey;
import com.howbuy.hadoop.mr.online.h5.H5EventDailyAnly.EventMapper;
import com.howbuy.hadoop.mr.online.h5.H5EventDailyAnly.EventReducer;
import com.howbuy.hadoop.mr.online.h5.H5PVDailyAnly.PVMapper;
import com.howbuy.hadoop.mr.online.h5.H5PVDailyAnly.PVReducer;

public class H5pvAndEventMerger extends BaseAnly implements Tool{
	
	
	
	
	
	//合并pv，event同channel数据 
	public static class DataMergerMapper extends Mapper<H5MergedRecord,NullWritable,Text,H5MergedRecord> {

		
		private Text channel = new Text();
		
		@Override
		protected void map(H5MergedRecord key, NullWritable value, Context context)
				throws IOException, InterruptedException {
			
			
			channel.set(key.getChannel());
			
			context.write(channel, key);
			
		}
	}
	
	public static class DataMegerReducer extends Reducer<Text,H5MergedRecord,H5MergedRecord,NullWritable> {

	    private static NullWritable nullValue = NullWritable.get();
	    
	    private static Logger logger = Logger.getLogger(H5pvAndEventMerger.class);
		
		
		private static String GROUP_NAME = "howbuy";
	    
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
		}

		
		@Override
		protected void reduce(Text key, Iterable<H5MergedRecord> values,Context context)
				throws IOException, InterruptedException {
			
			
			context.getCounter(GROUP_NAME,"invoke times").increment(1);
			
			
			H5MergedRecord eventrecord = null;
			
			H5MergedRecord pvrecord = null;
			
			
			for(H5MergedRecord value : values){
				
				if("1".equals(value.getFlag())){
					
					pvrecord = new H5MergedRecord();
					
					try {
						BeanUtils.copyProperties(pvrecord, value);
					} catch (Exception e) {
						e.printStackTrace();
					} 
					
					context.getCounter(GROUP_NAME,"pv size").increment(1);
					
				}
				else if("2".equals(value.getFlag())){
					
					eventrecord = new H5MergedRecord();
					
					try {
						BeanUtils.copyProperties(eventrecord, value);
					} catch (Exception e) {
						e.printStackTrace();
					} 
					
					context.getCounter(GROUP_NAME,"event size").increment(1);
					
				}
				
				context.getCounter(GROUP_NAME,"merge size").increment(1);
			}
			
			
			try {
				if(pvrecord != null && eventrecord != null){
					
					
					pvrecord.setOpenacctnum(eventrecord.getOpenacctnum());
					
					context.getCounter(GROUP_NAME,"pairs").increment(1);
					
					context.write(pvrecord,nullValue);
					
				}
				else if (pvrecord != null){
					
					
					context.getCounter(GROUP_NAME,"pv copy").increment(1);
					
					context.write(pvrecord,nullValue);
					
				}else if (eventrecord != null){
					
					context.getCounter(GROUP_NAME,"event copy").increment(1);
					
					context.write(eventrecord,nullValue);
				}
			} catch (Exception e) {
				
				logger.error("", e);
			}
			
			
		}
		
	}
	
	

	public static void main(String[] args) {
		
		try {
			
			ToolRunner.run(new Configuration(), new H5pvAndEventMerger(), args);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public int run(String[] args) throws Exception {
		
		String pvpath = args[0];
		
		String eventpath = args[1];
		
		String dt = args[2];
		
		String eventOutputDir = "/tmp/mapred/eventoutput";
		
		String pvOutputDir = "/tmp/mapred/pvoutput";
		
//		event日志
		Job job1 = new Job(getConf(),"channel_event_stat_h5_" + dt);
		
		job1.setJarByClass(H5EventDailyAnly.class);

		job1.setOutputFormatClass(SequenceFileOutputFormat.class);

		job1.setOutputKeyClass(H5MergedRecord.class);
		job1.setOutputValueClass(NullWritable.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		
		job1.setMapperClass(EventMapper.class);
		job1.setReducerClass(EventReducer.class);

		FileInputFormat.setInputPaths(job1, eventpath);
		
		FileOutputFormat.setOutputPath(job1, new Path(eventOutputDir));
		
		job1.getConfiguration().set("dt",StringUtils.isEmpty(dt) ? null : dt);
		
	    //pv日志
	    Job job2 = new Job(getConf(), "channel_view_stat_h5_" + dt);
	    
	    job2.setJarByClass(H5PVDailyAnly.class);

		job2.setOutputFormatClass(SequenceFileOutputFormat.class);

		job2.setOutputKeyClass(H5MergedRecord.class);
		job2.setOutputValueClass(NullWritable.class);
		
		job2.setMapOutputKeyClass(TaggedKey.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setMapperClass(PVMapper.class);
		job2.setReducerClass(PVReducer.class);

		job2.setPartitionerClass(GenPartitioner.class);
		job2.setGroupingComparatorClass(GuidGroupingComparator.class);
		
		job2.getConfiguration().set("dt",StringUtils.isEmpty(dt) ? null : dt);
		
		FileInputFormat.addInputPath(job2, new Path(pvpath));
		
		FileOutputFormat.setOutputPath(job2, new Path(pvOutputDir));
		
		
		//merger
		
//		DBConfiguration.configureDB(getConf(), "com.mysql.jdbc.Driver",
//				 "jdbc:mysql://192.168.220.157:3306/uaa_2", "admin", "123");
		
		DBConfiguration.configureDB(getConf(), "com.mysql.jdbc.Driver",
				getMySqlConnectionURL(), "uaa", "uaa_20150108");
		
		Job job3 = new Job(getConf(), "channel_view_event_merged_h5_" + dt);
		
		DBOutputFormat.setOutput(job3, "channel_view_trade_stat_h5", new String[] { "dt",
				"channel", "pv", "uv","enter", "huodong_index", "openacct_index", "auth_index", "open_result_index","openacct_num",
				"channel_type", "channel_parent", "channel_level","create_time" });
		
		job3.setInputFormatClass(SequenceFileInputFormat.class);
				
		job3.setOutputFormatClass(DBOutputFormat.class);
		
		job3.setOutputKeyClass(H5MergedRecord.class);
		job3.setOutputValueClass(NullWritable.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(H5MergedRecord.class);
		
		job3.setMapperClass(DataMergerMapper.class);
		job3.setReducerClass(DataMegerReducer.class);
		job3.setJarByClass(H5pvAndEventMerger.class);
		job3.setGroupingComparatorClass(ChannelComparator.class);
		
		FileInputFormat.setInputPaths(job3, new Path(eventOutputDir),new Path(pvOutputDir));
		
		ControlledJob ctJob2 = new ControlledJob(getConf());
		ctJob2.setJob(job2);
		
		ControlledJob ctJob1 = new ControlledJob(getConf());
		ctJob1.setJob(job1);
		
		ControlledJob ctJob3 = new ControlledJob(getConf());
		ctJob3.setJob(job3);
		
		ctJob3.addDependingJob(ctJob1);
		ctJob3.addDependingJob(ctJob2);
		
		
		JobControl jobCtrl=new JobControl("myctrl");
		jobCtrl.addJob(ctJob1);
		jobCtrl.addJob(ctJob2);
		jobCtrl.addJob(ctJob3);
		
		Thread jcThread = new Thread(jobCtrl);
		jcThread.start();
		
		while(!jobCtrl.allFinished()){
			
			TimeUnit.SECONDS.sleep(2);
		}
		
		jobCtrl.stop();
		
		FileSystem fs = FileSystem.get(getConf());
		
		if(fs.exists(new Path(pvOutputDir))){
			
			fs.delete(new Path(pvOutputDir), true);
		}
		
		if(fs.exists(new Path(eventOutputDir))){
			
			fs.delete(new Path(eventOutputDir), true);
		}

		
		return 0;
	}

}
