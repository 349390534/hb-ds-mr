package com.howbuy.hadoop.mr.online.h5;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.howbuy.hadoop.mr.online.BaseAnly;

public class H5EventDailyAnly extends BaseAnly implements Tool {

	private static Logger logger = LoggerFactory.getLogger(H5EventDailyAnly.class);

	public static class EventMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		
		private Splitter splitter;

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();

			List<String> values = Lists.newArrayList(splitter.split(line));

			if (values.size() == 21) {

				String productid = values.get(19);
				
				String type = values.get(1);

				//渠道错误，或者类型错误
				if (!"5002".equals(productid) || !"4".equals(type)) {

					String otrack = values.get(20);
					return;
				}
				
				Text productidText = new Text(productid);

				context.write(productidText, value);
			}

		}

		@Override
		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {

			splitter = Splitter.on(FIELD_SEPARATOR).trimResults();

		}
	}

	public static class EventReducer extends
			Reducer<Text, Text, H5MergedRecord,NullWritable> {
		
		private static String OPEN_ACCT = "openacct";
		
		//渠道
		private static String[] channels = new String[] { ALL_SEARCH_CHANNEL,
			ALL_TUIGUANG_CHANNEL, ALL_OTHER_CHANNEL, ALL_DIRECT_CHANNEL };
		
		//指标
		private static String[] BIZS = new String[] {OPEN_ACCT};
		
		/*
		 * 业务指标映射
		 */
		private static Map<String,String> TYPE_BIZ = new HashMap<String,String>(){
			
			{
				
				put("4",OPEN_ACCT);
			}
		};
		
		/**
		 * 指标渠道明细
		 * @return
		 */
		private static Map<String,Map<String,Long>> buildBizMap(){
			
			Map<String,Map<String,Long>> map = new HashMap<String, Map<String,Long>>();
			
			for(int i = 0; i < BIZS.length; i++){
				
				map.put(BIZS[i], new HashMap<String,Long>());
			}
			
			return map;
		}
		
		/**
		 * 构建渠道指标映射
		 * @return
		 */
		private static Map<String,Map<String,Long>> buildChannelMap(Map<String,Map<String,Long>> source){
			
			Map<String,Map<String,Long>> s = source;
			
			if(s == null)
				s = new HashMap<String,Map<String,Long>>();
			
			for (int i = 0; i < channels.length; i++) {

				Map<String,Long> stat = new HashMap<String,Long>();

				s.put(channels[i], stat);

				for (int j = 0; j < BIZS.length; j++) {

					stat.put(BIZS[j], new Long(0));
				}
			}
			
			return s;
		}
		
		//“全部”渠道汇总
		private static Map<String,Long> allbizChannelMap = new HashMap<String,Long>(){

			{
				for(int i = 0; i < BIZS.length; i++){
					
					put(BIZS[i], 0l);
				}
			}
		};
		
		//渠道汇总指标映射
		private static Map<String,Map<String,Long>> channel2bizMap = buildChannelMap(null);
		
		//业务渠道明细指标映射
		private static Map<String,Map<String,Long>> biz2channelMap = buildBizMap();

		private static Splitter splitter = Splitter.on(FIELD_SEPARATOR).trimResults();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			
			
			for(Text value : values){
				
				//当前渠道
				String currChannel = "";
				
				List<String> val = Lists.newArrayList(splitter.split(value.toString()));

				String type = val.get(1);
				
				String otrack = val.get(20);

				if (!isEmpty(otrack)) {//有otrack

					Matcher m = OTRACK_PATTERN.matcher(otrack);

					if (!m.matches()) {// otrack格式不合法

						currChannel = OTHER_CHANNEL;

						logger.warn("invalid otrack:{}",otrack);

					} else {//合法otrack

						String[] arr = otrack.split("\\.");

						String[] body = arr[0].split("-");

						String htag = body[0];

						if (htag.equals("0")) {// 站内直接访问

							htag = DIRECT_CHANNEL;

						}
						
						// htag合法性判断
						else if(!TUI_GUANG_HTAG_PATTERN.matcher(htag).matches() &&
								!SEARCH_PATTERN.matcher(htag).matches() &&
								!DIRECT_CHANNEL.equals(htag) &&
								!OTHER_CHANNEL.equals(htag))
							
							currChannel = OTHER_CHANNEL;
						else
							currChannel = htag;
					}


				} else{//无otrack

					if(isEmpty(currChannel))
						currChannel = DIRECT_CHANNEL;

				}
				
				String biz = TYPE_BIZ.get(type);
				
				Map<String,Long> bizMap = null;
				
				if (DIRECT_CHANNEL.equals(currChannel)){// 直接渠道
					
					String firstClass = currChannel;
					
					Map<String,Long> channelmap = biz2channelMap.get(biz);
					
					getAndIncr(channelmap, firstClass, new Long(1));
					
					bizMap = channel2bizMap.get(ALL_DIRECT_CHANNEL);
					
				}else if(SEARCH_PATTERN.matcher(currChannel).matches()){ // 搜索渠道
					
					String firstClass = currChannel;
					
					Map<String,Long> channelmap = biz2channelMap.get(biz);
					
					getAndIncr(channelmap, firstClass, new Long(1));
					
					bizMap = channel2bizMap.get(ALL_SEARCH_CHANNEL);
					
					
				}else if(OTHER_CHANNEL.equals(currChannel)){//其他渠道
					
					String firstClass = currChannel;
					
					Map<String,Long> channelmap = biz2channelMap.get(biz);
					
					getAndIncr(channelmap, firstClass, new Long(1));
					
					bizMap = channel2bizMap.get(ALL_OTHER_CHANNEL);
					
					
				}else if (TUI_GUANG_HTAG_PATTERN.matcher(currChannel).matches()) {// 其他渠道
					
					
					Map<String,Long> channelmap = biz2channelMap.get(biz);
					
					// 解析三级渠道
					String firstClass = currChannel.substring(0, 3);

					String secondClass = currChannel.substring(0, 6);

					String thirdClass = currChannel.substring(0, 10);
					
					getAndIncr(channelmap, firstClass, new Long(1));
					getAndIncr(channelmap, secondClass, new Long(1));
					getAndIncr(channelmap, thirdClass, new Long(1));
					
					bizMap = channel2bizMap.get(ALL_TUIGUANG_CHANNEL);
				}
				
				getAndIncr(bizMap, biz, new Long(1));
				
			}
			
			/*********************汇总*****************************/
			
			//“全部”渠道汇总
			for(String channel : channel2bizMap.keySet()){
				
				Map<String,Long> bizmap = channel2bizMap.get(channel);
				
				for(int i = 0; i < BIZS.length; i++){
					
					Long openacctnum = bizmap.get(BIZS[i]) == null ? new Long(0) : bizmap.get(BIZS[i]);
					
					getAndIncr(allbizChannelMap, BIZS[i], openacctnum);
					
				}
				
			}
			
			/*********************汇总结束 *****************************/
			
		}
			

		@SuppressWarnings("unchecked")
		@Override
		protected void cleanup(
				org.apache.hadoop.mapreduce.Reducer.Context context)
				throws IOException, InterruptedException {

			String dt = context.getConfiguration().get("dt");

			DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			
			Date statDt = null;

			long t = Calendar.getInstance().getTimeInMillis();

			try {
				if (isEmpty(dt))
					statDt = new Date(t);
				else
					statDt = new Date(df.parse(dt).getTime());
			} catch (ParseException e) {
				logger.error("",e);
				return;
			}
			
			//明细入库
			Map<String,Long> openacctMap = biz2channelMap.get(OPEN_ACCT);
			
			for(String channel : openacctMap.keySet()){
				
				long openacctnum = openacctMap.get(channel) == null ? 0 : openacctMap.get(channel);
				
				int type = getType(channel);

				String root = "-1";

				String level = null;

				switch (type) {
				case 1:
					level = "1";
					break;

				case 2:
					level = "1";
					break;

				case 3:
					level = "1";
					break;

				case 4:
					level = channel.length() == 3 ? "1"
							: channel.length() == 6 ? "2"
									: channel.length() == 10 ? "3" : "-1";

					root = level.equals("1") ? "-1"
							: level.equals("2") ? channel.substring(0, 3)
									: channel.substring(0, 6);
					break;

				default:
					root = "-1";
					level = "-1";

				}
				
				
//				context.write(new H5EventRecord(statDt, channel,
//						openacctnum, type + "",root, level,
//						new Timestamp(t)), NullWritable.get());
//				
				context.write(new H5MergedRecord(statDt,channel,0,0,0,0,0,0,0,openacctnum,type+"",root,level,new Timestamp(t),"2"), 
						NullWritable.get());
			}
			
			//各个渠道汇总入库
			for(String channel : channel2bizMap.keySet()){
				
				Map<String,Long> bizMap = channel2bizMap.get(channel);
				
				Long openacctnum = bizMap.get(OPEN_ACCT) == null ? 0 : bizMap.get(OPEN_ACCT);
				
//				context.write(new H5EventRecord(statDt, channel,
//						openacctnum, "5", "-1", "1",
//						new Timestamp(t)), NullWritable.get());
				
				context.write(new H5MergedRecord(statDt,channel,0,0,0,0,0,0,0,openacctnum,"5","-1","1",new Timestamp(t),"2"), 
						NullWritable.get());
			}
			
			
			//“全部”渠道入库

			Long openacctnum = allbizChannelMap.get(OPEN_ACCT)==null ? 0 :allbizChannelMap.get(OPEN_ACCT);
			
//			context.write(new H5EventRecord(statDt, ALL_CHANNEL,
//					openacctnum, "5","-1","1",new Timestamp(t)), NullWritable.get());
			
			context.write(new H5MergedRecord(statDt,ALL_CHANNEL,0,0,0,0,0,0,0,openacctnum,"5","-1","1",new Timestamp(t),"2"),
					NullWritable.get());

		}
	}

	@Override
	public int run(String[] args) throws Exception {
		
		 DBConfiguration.configureDB(getConf(), "com.mysql.jdbc.Driver",
		 "jdbc:mysql://192.168.220.157:3306/uaa_2", "admin", "123");

//		DBConfiguration.configureDB(getConf(), "com.mysql.jdbc.Driver",
//				"jdbc:mysql://10.70.70.27:3306/uaa", "uaa", "uaa_20150108");

		Job job = new Job(getConf(), "channel_trade_stat_h5_" + args[1]);

		DBOutputFormat.setOutput(job, "channel_trade_stat_h5", new String[] {
				"dt", "channel", "openacct_num","channel_type", "channel_parent",
				"channel_level,create_time" });

		job.setJarByClass(H5EventDailyAnly.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(DBOutputFormat.class);

		job.setOutputKeyClass(H5EventRecord.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setMapperClass(EventMapper.class);
		job.setReducerClass(EventReducer.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));

		job.getConfiguration().set("dt",
				StringUtils.isEmpty(args[1]) ? null : args[1]);
		// 开始运行
		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String args[]) throws Exception {

		ToolRunner.run(new Configuration(), new H5EventDailyAnly(), args);

	}
}
