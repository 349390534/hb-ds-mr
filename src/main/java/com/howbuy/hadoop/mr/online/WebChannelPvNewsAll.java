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
 * 资讯流量统计,所有资讯页面的流量明细数据（每天）
 * @author qiankun.li
 */
public class WebChannelPvNewsAll extends BaseAnly implements Tool{

	private static Logger logger = Logger.getLogger(WebChannelPvNewsAll.class);
	
	private static String FIELD_SEPARATOR = "\u0001";
	
	private static final String GROUP_NAME = "HOW_BUY_NEWS_ALL";

	//资讯URL匹配
	//private static Pattern HOWBUY_ZIXUN_PATTERN = Pattern.compile("(?:http|https)://.*/[0-9]{4}-[0-9]{2}-[0-9]{2}/\\d+\\.html");

	//资讯pageId匹配规则  ID+3位newstype+4位subtype+5位作者+1位pageId(3结尾)
	private static Pattern PAGEID_PATTERN = Pattern.compile("^\\d{13,}3$");
	
	private static final ProidType proidType = ProidType.PROID_ZIXUN;
	
	public static  class PVMapper extends  
	    Mapper<LongWritable, Text, TaggedKey, Text>{
		
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
		
		private  Map<String, String> guidMap = new HashMap<String, String>(0);
		
		private long pvCount;

	    public void reduce(TaggedKey key, Iterable<Text> values, Context context)
	    											throws IOException, InterruptedException{
			for (Text str : values) {
				String line = str.toString();
				List<String> valuesTemp = Lists.newArrayList(splitter.split(line));
				if(valuesTemp.size()==27){
					String guid = valuesTemp.get(0);
					guidMap.put(guid, guid);
				}
				pvCount++;
			}
	    }

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Override
		protected void cleanup(
				org.apache.hadoop.mapreduce.Reducer.Context context)
				throws IOException, InterruptedException {
			long pv = pvCount;
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
			String pageId="-1";//标识为资讯模块总PV、UV的pageId，以供接口查询
			int proidValue = Integer.valueOf(proidType.getIndex()).intValue();
			ChannelPageRecordNews channelPageRecord = new ChannelPageRecordNews(statDt,proidValue,
					pageId,-1l,"-1","-1","-1",pv, uv, ct);
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
				"jdbc:mysql://192.168.220.157:3306/uaa", "admin", "123");*/

		Job job = Job.getInstance(getConf(), "channel_page_pv_uv_job_news_all_" + args[1]);

		DBOutputFormat.setOutput(job, "channel_page_pv_uv_news_his", new String[] { "dt",
				"proid", "page_id" ,"zid","newstype","subtype","author_id","pv", "uv", "create_time"});
        
        job.setJarByClass(WebChannelPvNewsAll.class);
          
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
		
		ToolRunner.run(new Configuration(), new WebChannelPvNewsAll(), args);
	}
}
