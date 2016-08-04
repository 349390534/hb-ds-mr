package com.howbuy.hadoop.mr.online;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;


/**
 * 私募页面统计pv、UV
 * @author qiankun.li
 *
 */
public class WebChannelPvSimu extends BaseAnly implements Tool{
	private static Logger logger = Logger.getLogger(WebChannelPvSimu.class);
	
	private static String FIELD_SEPARATOR = "\u0001";
	
	//私募pageId匹配规则 pageid基金结尾是12,公司结尾22，经理结尾32
	private static Pattern PAGEID_PATTERN = Pattern.compile("^\\d+(1|2|3)2$");
	
	private static final String GROUP_NAME = "HOW_BUY_SIMU";

	private static Pattern HOWBUY_DOMAIN_PATTERN = Pattern.compile("(?:http|https)://.*(?<=simu\\.howbuy)\\.com.*");
	//定义模块 私募网站
	private static final ProidType proidType = ProidType.PROID_SIMU;
	
	public static  class PVMapper extends Mapper<LongWritable, Text, TaggedKey, Text>{
		
			private Splitter splitter = Splitter.on(FIELD_SEPARATOR).trimResults();
		
		
		    public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
				
				context.getCounter(GROUP_NAME,"ALL").increment(1);

				String line = value.toString();

				List<String> values = Lists.newArrayList(splitter.split(line));
				

				if (values.size() == 27) {

					String guid = values.get(0);
					
					
					String timestamp = values.get(1);

					if (isEmpty(guid) || isEmpty(timestamp) || !guid.startsWith("0x")) {
						
						context.getCounter(GROUP_NAME,"INVALID_DATA").increment(1);
						return;
					}
					
					String proid = values.get(22);
					if(!proidType.getIndex().startsWith(proid)){
						context.getCounter(GROUP_NAME,"INVALID_DATA").increment(1);
						return;
					}
					
					String pageid = values.get(18);
					Matcher pidmatcher = PAGEID_PATTERN.matcher(pageid);
					if(!pidmatcher.matches()){
						context.getCounter(GROUP_NAME,"INVALID_PAGEID").increment(1);
						return;
					}
					
					String desturl = values.get(12);
					
					Matcher destmatcher = HOWBUY_DOMAIN_PATTERN.matcher(desturl);
					
					if (!destmatcher.matches()){
						context.getCounter(GROUP_NAME,"INVALID_URL").increment(1);
						return;
					}

					TaggedKey tag = new TaggedKey();
					tag.setGuid(pageid);
					tag.setTimestamp(timestamp);
					context.write(tag, value);
				}else
					context.getCounter(GROUP_NAME, "NO_COMPLETE").increment(1);
			
		    }
	   	}
	
	public static  class PVReducer extends  Reducer<TaggedKey, Text, ChannelPageRecordSimu, NullWritable>{
		
		private Splitter splitter = Splitter.on(FIELD_SEPARATOR).trimResults();
		
		private NullWritable nullValue = NullWritable.get();
		
		Text word = new Text();
		
	    public void reduce(TaggedKey key, Iterable<Text> values, Context context)
	    											throws IOException, InterruptedException{
	    	String pageid = key.getGuid();
	    	int count = 0;
	    	Map<String, String> guidMap = new HashMap<String, String>(0);
			for (Text str : values) {
				String line = str.toString();
				List<String> valuesTemp = Lists.newArrayList(splitter.split(line));
				if(valuesTemp.size()==27){
					String guid = valuesTemp.get(0);
					guidMap.put(guid, guid);
				}
				count++;
			}
			int pv = count;
			int uv = guidMap.size();
			
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			String dt = context.getConfiguration().get("dt");
			Date statDt = null;
			long t = Calendar.getInstance().getTimeInMillis();
			try {
				if (isEmpty(dt))
					statDt = new Date(t);
				else
					statDt = new Date(df.parse(dt).getTime());
			} catch (ParseException e) {
				logger.error(e);
				return;
			}

			Timestamp ct = new Timestamp(t);
			int len = pageid.length();
			int pageType = Integer.valueOf(pageid.substring(len-2));
			int proidId = Integer.valueOf(proidType.getIndex());
			ChannelPageRecordSimu channelPageRecord = 
					new ChannelPageRecordSimu(statDt,proidId,Long.valueOf(pageid), pageType, pv, uv, ct);
			context.write(channelPageRecord, nullValue);
	    }

	}
	
	
	
	public static boolean isEmpty(String val) {

		if (StringUtils.isEmpty(val) || "null".equals(val))
			return true;
		return false;
	}
	

	@Override
	public int run(String[] args) throws Exception {
		DBConfiguration.configureDB(getConf(), "com.mysql.jdbc.Driver",
				getMySqlConnectionURL(), "uaa", "uaa_20150108");
		/*DBConfiguration.configureDB(getConf(), "com.mysql.jdbc.Driver",
				"jdbc:mysql://192.168.220.157:3306/uaa_2", "admin", "123");*/

		//Job job = new Job(getConf(), "channel_page_pv_uv_job" + args[1]);
		Job job = Job.getInstance(getConf(), "channel_page_pv_uv_job_simu_" + args[1]);
		DBOutputFormat.setOutput(job, "channel_page_pv_uv_simu", new String[] { "dt",
				"proid", "page_id","page_type","pv", "uv", "create_time"});
        
        job.setJarByClass(WebChannelPvSimu.class);  
          
        job.setInputFormatClass(TextInputFormat.class); 
        job.setOutputFormatClass(DBOutputFormat.class);
          
        job.setOutputKeyClass(ChannelPageRecordSimu.class);
        job.setOutputValueClass(NullWritable.class); 
        
        job.setMapOutputKeyClass(TaggedKey.class);
		job.setMapOutputValueClass(Text.class);
        
        job.setMapperClass(PVMapper.class);  
        job.setReducerClass(PVReducer.class);  
        
        job.setPartitionerClass(GenPartitioner.class);
        job.setGroupingComparatorClass(GuidGroupingComparator.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
		job.getConfiguration().set("dt",
				StringUtils.isEmpty(args[1]) ? null : args[1]);
        //开始运行  
        job.waitForCompletion(true);
		return 0;
	}  
	public static  void main(String args[])throws Exception{
		ToolRunner.run(new Configuration(), new WebChannelPvSimu(), args);
	}
}
