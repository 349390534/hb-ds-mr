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
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;


/**
 * 资讯流量统计,每个资讯页面的流量明细数据
 * 支持day、hour入参执行
 * @author qiankun.li
 */
public class WebChannelPvNews extends BaseAnly implements Tool{

	private static Logger logger = Logger.getLogger(WebChannelPvNews.class);
	
	private static String FIELD_SEPARATOR = "\u0001";
	
	private static final String GROUP_NAME = "HOW_BUY_NEWS";

	//资讯URL匹配
	//private static Pattern HOWBUY_ZIXUN_PATTERN = Pattern.compile("(?:http|https)://.*/[0-9]{4}-[0-9]{2}-[0-9]{2}/\\d+\\.html");

	//资讯pageId匹配规则  ID+3位newstype+4位subtype+5位作者+1位pageId(3结尾)
	private static Pattern PAGEID_PATTERN = Pattern.compile("^\\d{13,}3$");
	
	public static  class PVMapper extends  
	    Mapper<LongWritable, Text, TaggedKey, Text>{
		
			private Splitter splitter = Splitter.on(FIELD_SEPARATOR).trimResults();
			
			private final ProidType proidType = ProidType.PROID_ZIXUN;
			
		    public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
		    	InputSplit inputSplit = context.getInputSplit(); 
		    	String fileName = ((FileSplit) inputSplit).getPath().toString();
		    	System.out.println("map fileName is:"+fileName);
		    	
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
					/*String desturl = values.get(12);
					
					Matcher destmatcher = HOWBUY_ZIXUN_PATTERN.matcher(desturl);
					
					if (!destmatcher.matches()){
						context.getCounter(GROUP_NAME,"INVALID_URL").increment(1);
						return;
					}*/
					
					TaggedKey tag = new TaggedKey();
					tag.setGuid(pageid);
					tag.setTimestamp(timestamp);
					context.write(tag, value);
				}else
					context.getCounter(GROUP_NAME, "NO_COMPLETE").increment(1);
			
		    }

	   	}
	
	public static  class PVReducer extends  Reducer<TaggedKey, Text, ChannelPageRecordNews, NullWritable>{
		
		private Splitter splitter = Splitter.on(FIELD_SEPARATOR).trimResults();
		
		private NullWritable nullValue = NullWritable.get();
		
		Text word = new Text();
		
		private final String proid =ProidType.PROID_ZIXUN.getIndex();//定义模块 资讯模块
		
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
			// N位id
			long zid = Long.valueOf(StringUtils.left(pageid, len-13));
			//3位newstype
			String newstype = StringUtils.substring(StringUtils.right(pageid, 13), 0,3);
			//4位subtype 
			String subtype = StringUtils.substring(StringUtils.right(pageid, 13), 3,7);
			//5位作者
			String authorId = StringUtils.substring(StringUtils.right(pageid, 13), 7,12);
			ChannelPageRecordNews channelPageRecord = new ChannelPageRecordNews(statDt,Integer.valueOf(proid).intValue(),
					pageid,zid,newstype,subtype,authorId,pv, uv, ct);
			context.write(channelPageRecord, nullValue);
	    }

	}
	
	
	
	public static boolean isEmpty(String val) {

		if (StringUtils.isEmpty(val) || "null".equals(val))
			return true;
		return false;
	}
	

	/**
	 *key 
	 */
	static final String WEBPV_HOUR_FILTER_REGEX = "webpv.hour.filter.regex";
	
	/**
	 * 过滤每小时的正则表达式
	 */
	static final String WEBPV_HOUR_FILTER_REGEX_PATTERN_PRE = "web_pv\\.\\d{1,3}\\.[0-9]{4}-[0-9]{2}-[0-9]{2}\\.";
	/**
	 * webpv小时文件的过滤filter
	 * @author qiankun.li
	 *
	 */
	public static class WebPvPathHourFilter extends Configured implements PathFilter{
		public WebPvPathHourFilter() {}

		@Override
		public boolean accept(Path path) {
			int hour = getConf().getInt(WEBPV_HOUR_FILTER_REGEX, getPreHour());
			String dt = getConf().get("dt");
			String regex = dt+"."+hour;
			String pathValue = path.toString();
			return pathValue.endsWith(regex);
		}
		
	} 
	
	public static int getPreHour(){
		Calendar now = Calendar.getInstance();
		now.set(Calendar.HOUR_OF_DAY, now.get(Calendar.HOUR_OF_DAY)-1);
		int hour = now.get(Calendar.HOUR_OF_DAY);
		return hour;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		int aLen = args.length;
		Configuration conf =getConf();
		
		DBConfiguration.configureDB(getConf(), "com.mysql.jdbc.Driver",
				getMySqlConnectionURL(), "uaa", "uaa_20150108");
		/*DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver",
				"jdbc:mysql://192.168.220.157:3306/uaa", "admin", "123");*/
		String tableName= "channel_page_pv_uv_news_today";
		int hour = getPreHour();//默认是取上一小时
		Job job = null;
		if(3==aLen){
			if("d".equalsIgnoreCase(args[2])){
				//按天汇总
				job = Job.getInstance(conf, "channel_page_pv_uv_job_news_" + args[1]);
				tableName="channel_page_pv_uv_news_his";
			}else {
				String hourVar = args[2];
				if(StringUtils.isNumeric(hourVar)){
					//接收小时入参
					hour = Integer.valueOf(hourVar);
					//按小时汇总
					job =Job.getInstance(conf, "channel_page_pv_uv_job_news_" + args[1]+"."+hour);
					FileInputFormat.setInputPathFilter(job, WebPvPathHourFilter.class);
				}
			}
		}else if(2==aLen){
			//按小时汇总
			job =Job.getInstance(conf, "channel_page_pv_uv_job_news_" + args[1]+"."+hour);
			FileInputFormat.setInputPathFilter(job, WebPvPathHourFilter.class);
		}
		job.getConfiguration().setInt(WEBPV_HOUR_FILTER_REGEX, hour);
		job.setJarByClass(WebChannelPvNews.class);

		DBOutputFormat.setOutput(job, tableName, new String[] { "dt",
				"proid", "page_id" ,"zid","newstype","subtype","author_id","pv", "uv", "create_time"});
        
          
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);
          
        job.setOutputKeyClass(ChannelPageRecordNews.class);
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
		
		ToolRunner.run(new Configuration(), new WebChannelPvNews(), args);
		
	}
}
