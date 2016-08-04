package com.howbuy.hadoop.mr.online;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;


public class WEbEventAnly_totalAmt extends Configured implements Tool{

	private static Logger logger = Logger.getLogger(WEbEventAnly_totalAmt.class);
	
	private static String FIELD_SEPARATOR = "\u0001";
	
	private static final String GROUP_NAME = "HOW_BUY";
	
	
	public static  class PVMapper extends  
	    Mapper<LongWritable, Text, TaggedKey, Text>{  
		
		private Splitter splitter;
		
		
		    public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{  
		        
		    	String line = value.toString();
		        
		        List<String> values = Lists.newArrayList(splitter.split(line));
		        
		        if(values.size() != 32)
		        	return;
		        
		        String guid = values.get(15);
		        String timestamp = values.get(1);
		        String type = values.get(2);
		        
		        if(!ActionType.buyFund.getValue().equals(type) && !ActionType.piggyBuy.getValue().equals(type))
		        	return;
		        
		        context.getCounter(GROUP_NAME,"ALL").increment(1);
		        
		        
//		        if(isEmpty(guid,timestamp)){
//		        	logger.warn("guid or timestamp is \"null\"");
//		        	return;
//		        }
//		        
//		        if(!guid.startsWith("0x")){
//		        	logger.warn("valid guid :" + guid);
//		        	return;
//		        }
		        
		        	
	        	TaggedKey tag = new TaggedKey();
	        	tag.setGuid(guid);
	        	tag.setTimestamp(timestamp);
	        	
	        	context.write(tag, value);
		        
		    }

			@Override
			protected void setup(
					org.apache.hadoop.mapreduce.Mapper.Context context)
					throws IOException, InterruptedException {
				
				splitter = Splitter.on(FIELD_SEPARATOR).trimResults();
				
				context.getCounter(GROUP_NAME,"ALL").increment(1);
			}  
	   	}
	
	public static  class PVReducer extends  Reducer<TaggedKey, Text, NullWritable, Text>{
		
		private NullWritable nullValue = NullWritable.get();
		
		private Text word = new Text();
		StringBuilder builder = new StringBuilder();
		
		BigDecimal pigamt = BigDecimal.ZERO;
		
		BigDecimal buyamt = BigDecimal.ZERO;
		
		BigDecimal other = BigDecimal.ZERO;
		
		private Splitter splitter = Splitter.on(FIELD_SEPARATOR).trimResults();
		
		
	    public void reduce(TaggedKey key, Iterable<Text>values, Context context)
	    											throws IOException, InterruptedException{ 
	    	
	    	
//	        for (Text str : values){
//	        	
//	           builder.append(str.toString()).append("\n");
//	        }
//	        builder.setLength(builder.length()-1);
//	        word.set(builder.toString());
//	        context.write(nullValue, word);
//	        builder.setLength(0);
	    	
	    	
	    	for(Text str : values){
	    		
	    		List<String> list = Lists.newArrayList(splitter.split(str.toString()));
	    		
		    	String type = list.get(2);
	    		
	    		String stramt = list.get(7);
	    		
	    		if(ActionType.piggyBuy.getValue().equals(type)){
	    			
	    			pigamt = pigamt.add(new BigDecimal(stramt));
	    			
	    		}else if(ActionType.buyFund.getValue().equals(type)){
	    			
	    			buyamt = buyamt.add(new BigDecimal(stramt));
	    		}else{
	    			other = other.add(new BigDecimal(stramt));
	    		}
	    	}
	    }

		@Override
		protected void cleanup(
				org.apache.hadoop.mapreduce.Reducer.Context context)
				throws IOException, InterruptedException {
		
			NullWritable nullValue = NullWritable.get();
			
			Text word = new Text();
			
			StringBuilder builder = new StringBuilder();
			
			builder.append("pigamt:").append(pigamt.toString()).append(",")
			.append("buyamt:").append(buyamt.toEngineeringString()).append(",")
			.append("other:").append(other.toEngineeringString());
			
			word.set(builder.toString());
			
			context.write(nullValue, word);
		}  
	}
	
	
	public static boolean isEmpty(String guid,String timestmap){
		
		if(StringUtils.isEmpty(guid) || StringUtils.isEmpty(timestmap) 
						|| "null".equals(guid) || "null".equals(timestmap))
			return true;
		return false;
	}
	

	@Override
	public int run(String[] args) throws Exception {
		
        Job job = new Job(getConf(), "webeventtest"); 
        
        job.setJarByClass(WEbEventAnly_totalAmt.class);  
          
        job.setInputFormatClass(TextInputFormat.class); 
        job.setOutputFormatClass(TextOutputFormat.class);
          
        job.setOutputKeyClass(TaggedKey.class); 
        job.setOutputValueClass(Text.class); 
        
        job.setMapperClass(PVMapper.class);  
        job.setReducerClass(PVReducer.class);  
//        job.setCombinerClass(PVReducer.class);
        
        job.setPartitionerClass(GenPartitioner.class);
        job.setGroupingComparatorClass(GuidGroupingComparator.class);
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //开始运行  
        job.waitForCompletion(true); 
        
		return 0;
	}
	
	
	public static  void main(String args[])throws Exception{ 
		
		ToolRunner.run(new Configuration(), new WEbEventAnly_totalAmt(), args);
		
	}
}
