package com.howbuy.hadoop.mr.online;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
import org.apache.log4j.Logger;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

public class PVAnly extends BaseAnly implements Tool {

	private static Logger logger = Logger.getLogger(PVAnly.class);


	public static class PVMapper extends
			Mapper<LongWritable, Text, TaggedKey, Text> {

		private Splitter splitter = Splitter.on(FIELD_SEPARATOR).trimResults();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
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
				
				if("5001".equals(proid))
					return;
				
				String desturl = values.get(12);
				
				/*过滤以下url:
				 * data.howbuy.com/hws/
				 * m.howbuy.com
				   mzt.howbuy.com
				   zt.howbuy.com
				 */
				Matcher destmatcher = HOWBUY_DOMAIN_PATTERN.matcher(desturl);
				
				if (!destmatcher.find()){
					
					context.getCounter(GROUP_NAME,"INVALID_URL").increment(1);
					
					return;
				}

				String htag = values.get(10);

				// 0.0010020003900000
				if (!isEmpty(htag)) {

					Matcher m = HTAG_PATTERN.matcher(htag);

					if (!m.matches()){
						context.getCounter(GROUP_NAME,"INVLID_HTAG").increment(1);
					}
				}

				TaggedKey tag = new TaggedKey();
				tag.setGuid(guid);
				tag.setTimestamp(timestamp);

				context.write(tag, value);
			}else
				context.getCounter(GROUP_NAME, "NO_COMPLETE").increment(1);
		}

		@Override
		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {

		}
	}

	public static class PVReducer extends
			Reducer<TaggedKey, Text, ChannelRecord, NullWritable> {

		private Map<String, Long> validUvStat = new HashMap<String, Long>();

		private Map<String, Long> pvStat = new HashMap<String, Long>();
		
		private Map<String, Long> uvStat = new HashMap<String, Long>();
		
		private Map<String, Long> channelEnters = new HashMap<String, Long>();
		
		private Map<String,Long> smUVStat = new HashMap<String,Long>();

		private Splitter splitter = Splitter.on(FIELD_SEPARATOR).trimResults();

		private NullWritable nullValue = NullWritable.get();
		/**
		 * 总汇总渠道统计
		 */
		int total_pv = 0,
				total_uv = 0,
				total_validuv = 0,
				total_sm_uv = 0;
		
		/**
		 * pv各渠道汇总统计
		 */
		int channel_other_pv = 0,
				channel_search_pv = 0,
				channel_direct_pv = 0,
				channel_tuiguang_pv = 0;
		/**
		 * uv各渠道汇总统计
		 */
		int channel_other_uv = 0,
				channel_search_uv = 0,
				channel_direct_uv = 0,
				channel_tuiguang_uv = 0;
		/**
		 * 合法uv各渠道汇总统计
		 */
		int channel_other_validuv = 0,
				channel_search_validuv = 0,
				channel_direct_validuv = 0,
				channel_tuiguang_validuv = 0;
		/**
		 * 私募pv个渠道汇总统计
		 */
		int channel_other_sm_uv = 0,
				channel_search_sm_uv = 0,
				channel_direct_sm_uv = 0,
				channel_tuiguang_sm_uv = 0;

		public void reduce(TaggedKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			/*
			 * 用于统计单个人pv
			 */
			Map<String, Long> thirdPv = new HashMap<String, Long>();

			Map<String, Long> firstPv = new HashMap<String, Long>();

			Map<String, Long> secondPv = new HashMap<String, Long>();
			
			/*
			 * 用于统计单个人私募pv
			 */
			Map<String, Long> firstSm = new HashMap<String, Long>();
			
			Map<String, Long> secondSm = new HashMap<String, Long>();
			
			Map<String, Long> thirdSm = new HashMap<String, Long>();
			
			boolean alreadyDirect = false;

			String currChannel = "";

			String firstClass = "";

			String secondClass = "";

			String thirdClass = "";

			for (Text str : values) {
				
				context.getCounter(GROUP_NAME,"ALL").increment(1);

				List<String> list = Lists.newArrayList(splitter.split(str
						.toString()));

				String htag = list.get(10);

				String srcurl = list.get(2);
				
				String pageid = list.get(18);
				
				/*
				 * 判断当前页面是否是私募
				 */
				boolean isSm = !isEmpty(pageid) && pageid.endsWith("2");

				if (!isEmpty(htag)) {//有htag
					
					
					Matcher m = HTAG_PATTERN.matcher(htag);

					if (!m.matches()){//不合法格式htag
						
						context.getCounter(GROUP_NAME,"INVLID_HTAG").increment(1);
						
						
						if(isEmpty(srcurl)){//没有ref
							
							
							context.getCounter(GROUP_NAME,"INVLID_HTAG1").increment(1);
							
							getAndIncr(channelEnters, DIRECT_CHANNEL, new Long(1));
							
							if(alreadyDirect){
								
								getAndIncr(firstPv, DIRECT_CHANNEL, new Long(1));
								
								if(isSm){
									
									getAndIncr(firstSm, DIRECT_CHANNEL, new Long(1));
								}
								
								continue;
							}
							else{
								
								currChannel = DIRECT_CHANNEL;
								
								firstClass = DIRECT_CHANNEL;
								
								alreadyDirect = true;
							}
							
						}else{//有ref
							
							String tmpRef = srcurl.trim();
							
							try {
								
								if(!INTERNAL_HOWBUY_DOMAIN_PATTERN.matcher(tmpRef).find()){//ref站外 
									
									String tempchannel = getChannel(tmpRef);
									
									if(tempchannel != null){
										
										boolean isFromSearch = searches.containsKey(tempchannel);
										
										if(isFromSearch){
											
											getAndIncr(channelEnters, ALL_SEARCH_CHANNEL, new Long(1));
											
										}else{
											
											getAndIncr(channelEnters, ALL_OTHER_CHANNEL, new Long(1));
										}
										
										
									}else{
										
										// 其他渠道
										tempchannel = OTHER_CHANNEL;
										
										getAndIncr(channelEnters, ALL_OTHER_CHANNEL, new Long(1));
										
									}
									
									currChannel = tempchannel;
									
									firstClass = tempchannel;
									
									getAndIncr(channelEnters, tempchannel, new Long(1));
									
								}
								
								
							} catch (URISyntaxException e) {
								
								context.getCounter(GROUP_NAME,"INVLID_REF1").increment(1);
								
								getAndIncr(channelEnters, OTHER_CHANNEL, new Long(1));
								
								firstClass = OTHER_CHANNEL;
								
								currChannel = OTHER_CHANNEL;
								
							}
							
						}
						
						
					}else{//htag合法
						
						if (!INTERNAL_HOWBUY_DOMAIN_PATTERN.matcher(srcurl).find()) {//ref非站内

							
							String flag = htag.split("\\.")[0];
							
							if(!flag.equals("0")){//非站外htag
								
								if(isEmpty(srcurl)){//ref为空，归属直接
									
									context.getCounter(GROUP_NAME,"NON_ZERO_HTAG").increment(1);
									
									getAndIncr(channelEnters, DIRECT_CHANNEL, new Long(1));
									
									if(alreadyDirect){
										
										getAndIncr(firstPv, DIRECT_CHANNEL, new Long(1));
										
										if(isSm){
											
											getAndIncr(firstSm,DIRECT_CHANNEL,new Long(1));
										}
										
										continue;
									}else{
										
										currChannel = DIRECT_CHANNEL;
										
										firstClass = DIRECT_CHANNEL;
										
										alreadyDirect = true;
									}
									
								}else{//按ref对应list
									
									String tmpRef = srcurl.trim();
									
									try {
										
										String tempchannel = getChannel(tmpRef);
										
										if(tempchannel != null){
											
											boolean isFromSearch = searches.containsKey(tempchannel);
											
											if(isFromSearch){
												
												getAndIncr(channelEnters, ALL_SEARCH_CHANNEL, new Long(1));
												
											}else{
												
												getAndIncr(channelEnters, ALL_OTHER_CHANNEL, new Long(1));
											}
											
											currChannel = tempchannel;

											firstClass = tempchannel;
											
											
										}else{

											// 其他渠道
											currChannel = OTHER_CHANNEL;

											firstClass = OTHER_CHANNEL;

											getAndIncr(channelEnters, ALL_OTHER_CHANNEL, new Long(1));
											
										}
										
										getAndIncr(channelEnters, firstClass, new Long(1));
										
									} catch (URISyntaxException e) {//uri解析错误归属其他渠道
										
										context.getCounter(GROUP_NAME,"INVLID_REF2").increment(1);
										
										// 其他渠道
										currChannel = OTHER_CHANNEL;

										firstClass = OTHER_CHANNEL;

										getAndIncr(channelEnters, ALL_OTHER_CHANNEL, new Long(1));
									}
									
								}
								
							}else{//0级htag
								
								String entire = htag.split("\\.")[1];
								
								if (TUI_GUANG_HTAG_PATTERN.matcher(entire)
										.matches()) {// 推广
									
									
									firstClass = entire.substring(0, 3);
									
									secondClass = entire.substring(0, 6);
									
									thirdClass = entire.substring(0, 10);
									
									getAndIncr(channelEnters, firstClass, new Long(1));
									getAndIncr(channelEnters, secondClass, new Long(1));
									getAndIncr(channelEnters, thirdClass, new Long(1));
									
									getAndIncr(channelEnters, ALL_TUIGUANG_CHANNEL, new Long(1));
									
									currChannel = entire;
								}else{
									
									context.getCounter(GROUP_NAME,"INVLID_HTAG2").increment(1);

									continue;
								}

							}

						}
						
					}

					
				} else {//没有htag

					Matcher howbuym = INTERNAL_HOWBUY_DOMAIN_PATTERN.matcher(srcurl);
					
					if (!isEmpty(srcurl) && !howbuym.find()) {//有ref且站外ref
						
						String tmpRef = srcurl.trim();
							
						try {
							
							String tempchannel = getChannel(tmpRef);
							
							if(tempchannel != null){
								
								boolean isFromSearch = searches.containsKey(tempchannel);
								
								if(isFromSearch){
									
									getAndIncr(channelEnters, ALL_SEARCH_CHANNEL, new Long(1));
									
								}else{
									
									getAndIncr(channelEnters, ALL_OTHER_CHANNEL, new Long(1));
								}
								
								currChannel = tempchannel;

								firstClass = tempchannel;
								
								
							}else{

								// 其他渠道
								currChannel = OTHER_CHANNEL;

								firstClass = OTHER_CHANNEL;

								getAndIncr(channelEnters, ALL_OTHER_CHANNEL, new Long(1));
								
							}
							
							getAndIncr(channelEnters, firstClass, new Long(1));
							
						} catch (URISyntaxException e) {//uri解析错误归属其他渠道
							
							context.getCounter(GROUP_NAME,"INVLID_REF2").increment(1);
							
							// 其他渠道
							currChannel = OTHER_CHANNEL;

							firstClass = OTHER_CHANNEL;

							getAndIncr(channelEnters, ALL_OTHER_CHANNEL, new Long(1));
						}

					}

				}

				if (isEmpty(currChannel)) {

					// 直接访问
					currChannel = DIRECT_CHANNEL;

					firstClass = DIRECT_CHANNEL;

					getAndIncr(channelEnters, firstClass, new Long(1));
					
					//首次直接访问
					alreadyDirect = true;

				}

				if (currChannel.equals(DIRECT_CHANNEL)) {// 直接渠道

					getAndIncr(firstPv, firstClass, new Long(1));
					
					if(isSm){
						
						getAndIncr(firstSm,firstClass,new Long(1));
					}

				} else if (SEARCH_PATTERN.matcher(currChannel).matches()) {// 搜索渠道/其他

					getAndIncr(firstPv, firstClass, new Long(1));
					
					if(isSm){
						
						getAndIncr(firstSm,firstClass,new Long(1));
					}
					
				} else if (currChannel.equals(OTHER_CHANNEL)) {// 其他渠道

					getAndIncr(firstPv, firstClass, new Long(1));
					
					if(isSm){
						
						getAndIncr(firstSm,firstClass,new Long(1));
					}
					
				} else if(TUI_GUANG_HTAG_PATTERN.matcher(currChannel).matches()){// 推广渠道

					getAndIncr(firstPv, firstClass, new Long(1));
					getAndIncr(secondPv, secondClass, new Long(1));
					getAndIncr(thirdPv, thirdClass, new Long(1));
					
					if(isSm){
						
						getAndIncr(firstSm,firstClass,new Long(1));
						getAndIncr(secondSm,secondClass,new Long(1));
						getAndIncr(thirdSm,thirdClass,new Long(1));
					}
					
				}

			}

			
			/************私募分渠道归总pv********/
			
			for(String thirdChannel : thirdSm.keySet()){
				
				getAndIncr(smUVStat, thirdChannel, new Long(1));
			}
			
			for(String secondChannel : secondSm.keySet()){
				
				getAndIncr(smUVStat, secondChannel, new Long(1));
			}
			
			
			int temDirectsmpv = 0,temothersmpv = 0 ,temsearchsmpv = 0,temtuiguangsmpv = 0;
			
			for(String firstChannel : firstSm.keySet()){
				
				getAndIncr(smUVStat, firstChannel, new Long(1));
				
				//私募访问页面pv
				long smNum = firstSm.get(firstChannel);
				
				
				if(DIRECT_CHANNEL.equals(firstChannel)){
					
					temDirectsmpv += smNum;
					
				}else if(OTHER_CHANNEL.equals(firstChannel)){
					
					temothersmpv += smNum;
					
				}else if(SEARCH_PATTERN.matcher(firstChannel).matches()){
					
					if(searches.containsKey(firstChannel)){
						
						temsearchsmpv += smNum;
						
					}else if(other_searches.containsKey(firstChannel)){
						
						temothersmpv += smNum;
						
					}else{
						
						context.getCounter(GROUP_NAME,"INVALID_PREV_CHANNEL1").increment(1);
					}
					
				}else if(firstChannel.matches("\\d{3}")){//推广first 3个数字
					
					temtuiguangsmpv += smNum;
					
				}else{
					
					context.getCounter(GROUP_NAME,"INVALID_PREV_CHANNEL3").increment(1);
					
				}
			}
			
			/*
			 * 私募渠道uv汇总
			 */
			if(temDirectsmpv > 0)
				channel_direct_sm_uv++;
			if(temothersmpv > 0)
				channel_other_sm_uv++;
			if(temsearchsmpv > 0)
				channel_search_sm_uv++;
			if(temtuiguangsmpv > 0)
				channel_tuiguang_sm_uv++;
			
			//私募uv汇总
			if((temDirectsmpv + temothersmpv + temsearchsmpv + temtuiguangsmpv) > 0)
				total_sm_uv++;

			/************私募分渠道归总uv结束********/
			
			
			
			/***********所有渠道pv汇总***************/
			
			for (String thirdchannel : thirdPv.keySet()) {

				long num = thirdPv.get(thirdchannel);

				getAndIncr(pvStat, thirdchannel, num);

				getAndIncr(uvStat, thirdchannel, new Long(1));

				if (num > 1)
					getAndIncr(validUvStat, thirdchannel, new Long(1));

			}

			for (String secondchannel : secondPv.keySet()) {

				long num = secondPv.get(secondchannel);

				getAndIncr(pvStat, secondchannel, new Long(num));

				getAndIncr(uvStat, secondchannel, new Long(1));

				if (num > 1)
					getAndIncr(validUvStat, secondchannel, new Long(1));

			}
			
			int temdirectpv = 0,temotherpv = 0,temsearchpv = 0,temtuiguangpv = 0;
			
			for (String firstchannel : firstPv.keySet()) {

				long num = firstPv.get(firstchannel);

				getAndIncr(pvStat, firstchannel, num);

				getAndIncr(uvStat, firstchannel, new Long(1));

				if (num > 1)
					getAndIncr(validUvStat, firstchannel, new Long(1));
				
				/************分渠道归总pv********/
				
				if(DIRECT_CHANNEL.equals(firstchannel)){
					
					temdirectpv += num;
					
				}else if(OTHER_CHANNEL.equals(firstchannel)){
					
					temotherpv += num;
					
				}else if(SEARCH_PATTERN.matcher(firstchannel).matches()){
					
					if(searches.containsKey(firstchannel)){
						
						temsearchpv += num;
						
					}else if(other_searches.containsKey(firstchannel)){
						
						temotherpv += num;
						
					}else{
						
						context.getCounter(GROUP_NAME,"INVALID_PREV_CHANNEL1").increment(1);
					}
					
				}else if(firstchannel.matches("\\d{3}")){//推广first 3个数字
					
					temtuiguangpv += num;
					
				}else{
					
					context.getCounter(GROUP_NAME,"INVALID_PREV_CHANNEL2").increment(1);
					
				}
				
			}
			
			//公募pv
			channel_direct_pv += temdirectpv;
			channel_other_pv += temotherpv;
			channel_search_pv += temsearchpv;
			channel_tuiguang_pv += temtuiguangpv;
			
			
			//渠道去重统计uv
			if(temdirectpv > 0)
				channel_direct_uv++;
			if(temdirectpv > 1)
				channel_direct_validuv++;
			//其他渠道去重统计
			if(temotherpv > 0)
				channel_other_uv++;
			if(temotherpv > 1)
				channel_other_validuv++;
			//搜索渠道去重统计
			if(temsearchpv > 0)
				channel_search_uv++;
			if(temsearchpv > 1)
				channel_search_validuv++;
			//推广渠道去重统计
			if(temtuiguangpv > 0)
				channel_tuiguang_uv++;
			if(temtuiguangpv > 1)
				channel_tuiguang_validuv++;
			
			
			/*****************所有渠道去重汇总******************/
			
			int individualpv = temdirectpv + temotherpv + temsearchpv + temtuiguangpv;
			total_pv += individualpv;
			if(individualpv > 0)
				total_uv++;		
			if(individualpv > 1)
				total_validuv++;
			
		}

		@Override
		protected void cleanup(
				org.apache.hadoop.mapreduce.Reducer.Context context)
				throws IOException, InterruptedException {

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
			

			for (String channel : pvStat.keySet()) {

				int type = getType(channel);

				String parent = null;

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
						
						parent = level.equals("1") ? null : level.equals("2") ? channel.substring(0, 3) : channel.substring(0, 6);
						
						break;
	
					default:
						parent = "-1";
						level = "-1";

				}

				/*
				 * 明细数据
				 */
				context.write(
						new ChannelRecord(statDt, channel, pvStat.get(channel),
								uvStat.get(channel),
								validUvStat.get(channel) == null ? 0 : validUvStat.get(channel),
								channelEnters.get(channel) == null ? 0 : channelEnters.get(channel),
								0, smUVStat.get(channel) == null ? 0 : smUVStat.get(channel), type, parent,
								level,new Timestamp(t)), nullValue);
			}
			
			/* 按渠道汇总*/
			
			//直接
			long direct_enter = channelEnters.get(DIRECT_CHANNEL) == null ? 0 : channelEnters.get(DIRECT_CHANNEL);
			context.write(
					new ChannelRecord(statDt, ALL_DIRECT_CHANNEL, channel_direct_pv,
							channel_direct_uv,
							channel_direct_validuv,
							direct_enter,
							0, channel_direct_sm_uv, 5, null,
							"1",new Timestamp(t)), nullValue);
			
			//其他
			long other_enter = channelEnters.get(ALL_OTHER_CHANNEL) == null ? 0 : channelEnters.get(ALL_OTHER_CHANNEL);
			context.write(
					new ChannelRecord(statDt, ALL_OTHER_CHANNEL, channel_other_pv,
							channel_other_uv,
							channel_other_validuv,
							other_enter,
							0, channel_other_sm_uv, 5, null,
							"1",new Timestamp(t)), nullValue);
			
			//搜索
			long search_enter = channelEnters.get(ALL_SEARCH_CHANNEL) == null ? 0 : channelEnters.get(ALL_SEARCH_CHANNEL);
			context.write(
					new ChannelRecord(statDt, ALL_SEARCH_CHANNEL, channel_search_pv,
							channel_search_uv,
							channel_search_validuv,
							search_enter,
							0, channel_search_sm_uv, 5, null,
							"1",new Timestamp(t)), nullValue);
			
			//推广
			long guiguang_enter = channelEnters.get(ALL_TUIGUANG_CHANNEL) == null ? 0 : channelEnters.get(ALL_TUIGUANG_CHANNEL);
			context.write(
					new ChannelRecord(statDt, ALL_TUIGUANG_CHANNEL, channel_tuiguang_pv,
							channel_tuiguang_uv,
							channel_tuiguang_validuv,
							guiguang_enter,
							0, channel_tuiguang_sm_uv, 5, null,
							"1",new Timestamp(t)), nullValue);
			
			
			
			/* 所有渠道汇总*/
			context.write(
					new ChannelRecord(statDt, ALL_CHANNEL, total_pv,
							total_uv,
							total_validuv,
							direct_enter + other_enter + search_enter + guiguang_enter,
							0, total_sm_uv,
							5, null,
							"1",new Timestamp(t)), nullValue);
			
		}

	}

	

	@Override
	public int run(String[] args) throws Exception {

//		DBConfiguration.configureDB(getConf(), "com.mysql.jdbc.Driver",
//				"jdbc:mysql://192.168.220.157:3306/uaa_2", "admin", "123");
		
		DBConfiguration.configureDB(getConf(), "com.mysql.jdbc.Driver",
				getMySqlConnectionURL(), "uaa", "uaa_20150108");

		Job job = new Job(getConf(), "web_pv_anly_" + args[1]);

		DBOutputFormat.setOutput(job, "channel_view_stat", new String[] { "dt",
				"channel", "pv", "uv", "validuv", "enter", "gmuv", "simuuv",
				"channel_type", "channel_parent", "channel_level","createtime" });

		job.setJarByClass(PVAnly.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(DBOutputFormat.class);

		job.setOutputKeyClass(ChannelRecord.class);
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
		// 开始运行
		job.waitForCompletion(true);

		return 0;
	}
	
	public static boolean isEmpty(String val) {

		if (StringUtils.isEmpty(val) || "null".equals(val))
			return true;
		return false;
	}

	public static void main(String args[]) throws Exception {

		ToolRunner.run(new Configuration(), new PVAnly(), args);
	}
}
