package com.howbuy.hadoop.mr.online.h5;

import java.io.IOException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.howbuy.hadoop.mr.online.BaseAnly;
import com.howbuy.hadoop.mr.online.GenPartitioner;
import com.howbuy.hadoop.mr.online.GuidGroupingComparator;
import com.howbuy.hadoop.mr.online.TaggedKey;

public class H5PVDailyAnly extends BaseAnly implements Tool {

	private static Logger logger = LoggerFactory.getLogger(H5PVDailyAnly.class);


	public static class PVMapper extends
			Mapper<LongWritable, Text, TaggedKey, Text> {

		private static Splitter splitter = Splitter.on(FIELD_SEPARATOR).trimResults();

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
				
				//保留H5
				if(!"5002".equals(proid))
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
			Reducer<TaggedKey, Text, H5MergedRecord, NullWritable> {
		
		private static NullWritable nullValue = NullWritable.get();

		private static String PV = "pv";
		
		private static String UV = "uv";
		
		private static String HUODONG_INDEX = "huodong_index";
		
		private static String OPENACCT_INDEX = "openacct_index";
		
		private static String AUTH_INDEX = "auth_index";
		
		private static String OPENACCT_RESULT = "openacct_result";
		
		private static String ENTER = "enter";
		
		private static Splitter splitter = Splitter.on(FIELD_SEPARATOR).trimResults();
		
		//pageid位数与页面映射
		private static Map<String,String> pageid_suffix_map = new HashMap<String, String>(){{
			put("6", HUODONG_INDEX);
			put("21000", OPENACCT_INDEX);
			put("21010", AUTH_INDEX);
			put("21020", OPENACCT_RESULT);
		}};
		
		//渠道
		private static String[] channels = new String[] { ALL_SEARCH_CHANNEL,
			ALL_TUIGUANG_CHANNEL, ALL_OTHER_CHANNEL, ALL_DIRECT_CHANNEL };
		
		//指标
		private static String[] bizs = new String[] { PV,UV,HUODONG_INDEX, OPENACCT_INDEX,
			AUTH_INDEX, OPENACCT_RESULT,ENTER };
		
		/**
		 * 指标渠道明细
		 * @return
		 */
		private Map<String,Map<String,Long>> buildBizMap(){
			
			Map<String,Map<String,Long>> map = new HashMap<String, Map<String,Long>>();
			
			for(int i = 0; i < bizs.length; i++){
				
				map.put(bizs[i], new HashMap<String,Long>());
			}
			
			return map;
		}
		

		/**
		 * 渠道指标映射
		 * @return
		 */
		private Map<String,Map<String,Long>> buildChannelMap(Map<String,Map<String,Long>> source){
			
			Map<String,Map<String,Long>> s = source;
			
			if(s == null)
				s = new HashMap<String,Map<String,Long>>(0);
			
			for (int i = 0; i < channels.length; i++) {
				Map<String,Long> stat = new HashMap<String,Long>(bizs.length);
				s.put(channels[i], stat);
				for (int j = 0; j < bizs.length; j++) {
					stat.put(bizs[j], new Long(0));
				}
			}
			return s;
		}
		
		
		//“全部”渠道汇总
		private Map<String,Long> allbizChannelMap = new HashMap<String,Long>(){

			{
				for(int i = 0; i < bizs.length; i++){
					
					put(bizs[i], new Long(0));
				}
			}
		};
		
		//渠道汇总指标映射
		Map<String,Map<String,Long>> channel2bizMap = buildChannelMap(null);
		
		//业务渠道明细指标映射
		Map<String,Map<String,Long>> biz2channelMap = buildBizMap();
		
		//进入次数
		private Map<String, Long> channelEntersMap = new HashMap<String, Long>();
		

		public void reduce(TaggedKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			
			//临时渠道、指标映射
			Map<String,Map<String,Long>> tempchannel2bizMap = buildChannelMap(null);
			
			//临时指标、明细渠道映射
			Map<String,Map<String,Long>> tempbiz2channelMap = buildBizMap();
			
			//临时 总渠道指标
			Map<String,Long> tempallChannebizlMap = new HashMap<String,Long>(){

				{
					for(int i = 0; i < bizs.length; i++){
						
						put(bizs[i], new Long(0));
					}
				}
			};
			
			boolean alreadyDirect = false;

			String currChannel = "";

			String firstClass = "";

			String secondClass = "";

			String thirdClass = "";

			for (Text str : values) {
				
				List<String> list = Lists.newArrayList(splitter.split(str
						.toString()));

				String htag = list.get(10);

				String srcurl = list.get(2);

				String pageid = list.get(18);
				
				//分析开始
				if (!isEmpty(htag)) {// 有htag

					Matcher m = HTAG_PATTERN.matcher(htag);

					if (!m.matches()) {// 不合法格式htag

						logger.warn("invalid htag:{}", htag);

						if (isEmpty(srcurl)) {//没有ref
							
							// 直接访问
							currChannel = DIRECT_CHANNEL;

							firstClass = DIRECT_CHANNEL;

							getAndIncr(channelEntersMap, DIRECT_CHANNEL,
									new Long(1));

							getAndIncr(channelEntersMap,
									ALL_DIRECT_CHANNEL, new Long(1));
							
						} else {

							// 走ref
							String tmpRef = srcurl.trim();

							try {

								if (!INTERNAL_HOWBUY_DOMAIN_PATTERN
										.matcher(tmpRef).find()) {// ref站外

									String tempchannel = getChannel(tmpRef);

									if (tempchannel == null) {

										// 其他渠道
										currChannel = OTHER_CHANNEL;

										firstClass = OTHER_CHANNEL;

										getAndIncr(channelEntersMap,
												ALL_OTHER_CHANNEL,
												new Long(1));

									} else {

										boolean isFromSearch = searches
												.containsKey(tempchannel);

										if (isFromSearch) {

											currChannel = tempchannel;

											getAndIncr(
													channelEntersMap,
													ALL_SEARCH_CHANNEL,
													new Long(1));

										} else {

											currChannel = OTHER_CHANNEL;

											getAndIncr(
													channelEntersMap,
													ALL_OTHER_CHANNEL,
													new Long(1));
										}

										firstClass = tempchannel;
									}

									getAndIncr(channelEntersMap,
											firstClass, new Long(1));

								}

							} catch (URISyntaxException e) {

								logger.warn("invalid ref:{}" + tmpRef);

								firstClass = OTHER_CHANNEL;

								currChannel = OTHER_CHANNEL;

								getAndIncr(channelEntersMap,
										OTHER_CHANNEL, new Long(1));

								getAndIncr(channelEntersMap,
										ALL_OTHER_CHANNEL, new Long(1));

							}

						}

					} else {// htag合法
						
						if(isEmpty(srcurl)){//ref为空
							
							String flag = htag.split("\\.")[0];
							
							if (!flag.equals("0")) {// 非0级htag
								
								// 直接访问
								currChannel = DIRECT_CHANNEL;

								firstClass = DIRECT_CHANNEL;

								getAndIncr(channelEntersMap, DIRECT_CHANNEL,
										new Long(1));

								getAndIncr(channelEntersMap,
										ALL_DIRECT_CHANNEL, new Long(1));
								
							}else{
								
								//渠道
								String entire = htag.split("\\.")[1];
								
								if (TUI_GUANG_HTAG_PATTERN.matcher(entire)
										.matches()) {// 推广
									
									firstClass = entire.substring(0, 3);
									
									secondClass = entire.substring(0, 6);
									
									thirdClass = entire.substring(0, 10);
									
									getAndIncr(channelEntersMap, firstClass,
											new Long(1));
									getAndIncr(channelEntersMap, secondClass,
											new Long(1));
									getAndIncr(channelEntersMap, thirdClass,
											new Long(1));
									
									getAndIncr(channelEntersMap,
											ALL_TUIGUANG_CHANNEL, new Long(1));
									
									currChannel = entire;
									
								} else {// 非法0级htag
									
									logger.warn("invalid zero htag:{}:", htag);
								}
								
							}


						}else /*if (!INTERNAL_HOWBUY_DOMAIN_PATTERN.matcher(
										srcurl).find()) */{//站外ref

							String flag = htag.split("\\.")[0];

							if (!flag.equals("0")) {// 站内htag

								// 走ref

								String tmpRef = srcurl.trim();

								try {

									String tempchannel = getChannel(tmpRef);

									if (tempchannel != null) {

										boolean isFromSearch = searches
												.containsKey(tempchannel);

										if (isFromSearch) {

											currChannel = tempchannel;

											getAndIncr(channelEntersMap,
													ALL_SEARCH_CHANNEL,
													new Long(1));

										} else {

											currChannel = OTHER_CHANNEL;

											getAndIncr(channelEntersMap,
													ALL_OTHER_CHANNEL,
													new Long(1));
										}

										firstClass = tempchannel;

									} else {

										// 其他渠道
										currChannel = OTHER_CHANNEL;

										firstClass = OTHER_CHANNEL;

										getAndIncr(channelEntersMap,
												ALL_OTHER_CHANNEL, new Long(1));

									}

									getAndIncr(channelEntersMap, firstClass,
											new Long(1));

								} catch (URISyntaxException e) {// uri解析错误归属其他渠道

									logger.warn("valid ref:{}", tmpRef);

									// 其他渠道
									currChannel = OTHER_CHANNEL;

									firstClass = OTHER_CHANNEL;

									getAndIncr(channelEntersMap, OTHER_CHANNEL,
											new Long(1));

									getAndIncr(channelEntersMap,
											ALL_OTHER_CHANNEL, new Long(1));

								}

							} else {// 0级htag

								String entire = htag.split("\\.")[1];

								if (TUI_GUANG_HTAG_PATTERN.matcher(entire)
										.matches()) {// 推广

									firstClass = entire.substring(0, 3);

									secondClass = entire.substring(0, 6);

									thirdClass = entire.substring(0, 10);

									getAndIncr(channelEntersMap, firstClass,
											new Long(1));
									getAndIncr(channelEntersMap, secondClass,
											new Long(1));
									getAndIncr(channelEntersMap, thirdClass,
											new Long(1));

									getAndIncr(channelEntersMap,
											ALL_TUIGUANG_CHANNEL, new Long(1));

									currChannel = entire;

								} else {// 非法0级htag

									logger.warn("invalid zero htag:{}:", htag);
								}

							}

						}

					}

				} else {// 没有htag
					
					if(isEmpty(srcurl)){//ref空
						
						// 直接访问
						currChannel = DIRECT_CHANNEL;

						firstClass = DIRECT_CHANNEL;

						getAndIncr(channelEntersMap, DIRECT_CHANNEL,
								new Long(1));

						getAndIncr(channelEntersMap,
								ALL_DIRECT_CHANNEL, new Long(1));
						
						
					}else if (!INTERNAL_HOWBUY_DOMAIN_PATTERN.matcher(srcurl).find()) {// 站外ref

						String tmpRef = srcurl.trim();

						try {

							String tempchannel = getChannel(tmpRef);

							if (tempchannel != null) {

								boolean isFromSearch = searches
										.containsKey(tempchannel);

								if (isFromSearch) {

									currChannel = tempchannel;

									getAndIncr(channelEntersMap,
											ALL_SEARCH_CHANNEL, new Long(1));

								} else {

									currChannel = OTHER_CHANNEL;

									getAndIncr(channelEntersMap,
											ALL_OTHER_CHANNEL, new Long(1));
								}

								firstClass = tempchannel;

							} else {

								// 其他渠道
								currChannel = OTHER_CHANNEL;

								firstClass = OTHER_CHANNEL;

								getAndIncr(channelEntersMap, ALL_OTHER_CHANNEL,
										new Long(1));

							}

							getAndIncr(channelEntersMap, firstClass,
									new Long(1));

						} catch (URISyntaxException e) {// uri解析错误归属其他渠道

							logger.warn("valid ref:{}", tmpRef);

							// 其他渠道
							currChannel = OTHER_CHANNEL;

							firstClass = OTHER_CHANNEL;

							getAndIncr(channelEntersMap, OTHER_CHANNEL,
									new Long(1));

							getAndIncr(channelEntersMap, ALL_OTHER_CHANNEL,
									new Long(1));

						}

					}

				}

				if (isEmpty(currChannel)) {

					// 直接访问
					currChannel = DIRECT_CHANNEL;

					firstClass = DIRECT_CHANNEL;
					
					getAndIncr(channelEntersMap, DIRECT_CHANNEL, new Long(1));
					
					getAndIncr(channelEntersMap, ALL_DIRECT_CHANNEL, new Long(1));

					//首次直接访问
					alreadyDirect = true;

				}
				
				//当前渠道指标
				Map<String,Long> tempchanneldetailMap = null;
				
				Map<String,Long> pvdetailmap = biz2channelMap.get(PV);
				
				Map<String,Long> tempuvdetailmap = tempbiz2channelMap.get(UV);
				
				//判断pageid位数属于那个指标(biz)
				
				String currBiz = null;
				
				if(!isEmpty(pageid)){
					
					for(String sufixkey : pageid_suffix_map.keySet()){
						
						if(pageid.endsWith(sufixkey)){
							
							currBiz = pageid_suffix_map.get(sufixkey);
							
							tempchanneldetailMap = tempbiz2channelMap.get(currBiz);
							
							break;
						}
					}
				}
				
				/*
				 * 渠道指标汇总
				 */
				Map<String,Long> tempbizMap = null;

				if (currChannel.equals(DIRECT_CHANNEL)) {// 直接渠道
					
					tempbizMap = tempchannel2bizMap.get(ALL_DIRECT_CHANNEL);
					
					getAndIncr(pvdetailmap, firstClass, new Long(1));
					
					getAndIncr(tempuvdetailmap, firstClass, new Long(1));
					
					if(null != tempchanneldetailMap)
						getAndIncr(tempchanneldetailMap, firstClass, new Long(1));
					

				} else if (SEARCH_PATTERN.matcher(currChannel).matches()) {// 搜索渠道/其他
					
					tempbizMap = tempchannel2bizMap.get(ALL_SEARCH_CHANNEL);
					
					getAndIncr(pvdetailmap, firstClass, new Long(1));
					
					getAndIncr(tempuvdetailmap, firstClass, new Long(1));
					
					if(null != tempchanneldetailMap)
						getAndIncr(tempchanneldetailMap, firstClass, new Long(1));
					
				} else if (currChannel.equals(OTHER_CHANNEL)) {// 其他渠道
					
					tempbizMap = tempchannel2bizMap.get(ALL_OTHER_CHANNEL);
					
					getAndIncr(pvdetailmap, firstClass, new Long(1));
					
					getAndIncr(tempuvdetailmap, firstClass, new Long(1));
					
					if(null != tempchanneldetailMap)
						getAndIncr(tempchanneldetailMap, firstClass, new Long(1));
					
				} else if(TUI_GUANG_HTAG_PATTERN.matcher(currChannel).matches()){// 推广渠道
					
					tempbizMap = tempchannel2bizMap.get(ALL_TUIGUANG_CHANNEL);
					
					getAndIncr(pvdetailmap, firstClass, new Long(1));
					getAndIncr(pvdetailmap, secondClass, new Long(1));
					getAndIncr(pvdetailmap, thirdClass, new Long(1));
					
					getAndIncr(tempuvdetailmap, firstClass, new Long(1));
					getAndIncr(tempuvdetailmap, secondClass, new Long(1));
					getAndIncr(tempuvdetailmap, thirdClass, new Long(1));
					
					if(null != tempchanneldetailMap){
						
						getAndIncr(tempchanneldetailMap, firstClass, new Long(1));
						getAndIncr(tempchanneldetailMap, secondClass, new Long(1));
						getAndIncr(tempchanneldetailMap, thirdClass, new Long(1));
					}
				}
				
				getAndIncr(tempbizMap, PV, new Long(1));
				
				getAndIncr(tempbizMap, UV, new Long(1));
				
				if(null != currBiz){
					
					getAndIncr(tempbizMap, currBiz, new Long(1));
				}
				
			
			}
			
			/*************************单个用户信息汇总*******************************************/
			
			//明细汇总
			for(String biz : tempbiz2channelMap.keySet()){
				
				if(biz.equals(PV))
					continue;
				
				Map<String,Long> tempchannelmap = tempbiz2channelMap.get(biz);
				
				Map<String,Long> channelmap = biz2channelMap.get(biz);
				
				for(String channel : tempchannelmap.keySet()){
					
					//一个指标多个pv，放入一个uv
					getAndIncr(channelmap, channel, new Long(1));
				}
			}
			
			
			//渠道汇总
			for(String channel : tempchannel2bizMap.keySet()){
				
				Map<String,Long> tempbizsmap = tempchannel2bizMap.get(channel);
				
				Map<String,Long> bizsmap = channel2bizMap.get(channel);
				
				for(String biz : tempbizsmap.keySet()){
					
					//汇总各渠道指标到"临时总渠道指标"中
					getAndIncr(tempallChannebizlMap,biz,tempbizsmap.get(biz));
					
					if(PV.equals(biz)){
						
						//pv原数量添加
						getAndIncr(bizsmap, PV, tempbizsmap.get(biz));
						
					}else if (tempbizsmap.get(biz) > 0){//其余指标有数据增加1
						
						getAndIncr(bizsmap, biz, new Long(1));
					}
				}
				
			}
			
			//全部汇总
			for(String biz : tempallChannebizlMap.keySet()){
				
				if(PV.equals(biz)){
					
					getAndIncr(allbizChannelMap, biz, tempallChannebizlMap.get(biz));
					
				}else{
					
					getAndIncr(allbizChannelMap, biz, tempallChannebizlMap.get(biz) > 0 ? 1l : 0l);
				}
			}
			
			/*************************单个用户信息汇总结束 *******************************************/
						
		}

		@SuppressWarnings("unchecked")
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
				logger.error("",e);;
				return;
			}
			
			/*******************入库参数***********************/
			
			
			//渠道汇总入库
			long totalenter = 0;
			for(String channel : channel2bizMap.keySet()){
				
				Map<String,Long> bizMap = channel2bizMap.get(channel);
				
				long enter = channelEntersMap.get(channel) == null ? 0l : channelEntersMap.get(channel);
				
				totalenter += enter;
				
//				context.write(
//						new H5PVRecord(
//								statDt,
//								channel,
//								bizMap.get(PV) == null ? 0 : bizMap.get(PV),
//								bizMap.get(UV) == null ? 0 : bizMap.get(UV),
//								enter,
//								bizMap.get(HUODONG_INDEX) == null ? 0 : bizMap.get(HUODONG_INDEX),
//								bizMap.get(OPENACCT_INDEX) == null ? 0 : bizMap.get(OPENACCT_INDEX),
//								bizMap.get(AUTH_INDEX) == null ? 0 : bizMap.get(AUTH_INDEX),
//								bizMap.get(OPENACCT_RESULT) == null ? 0 : bizMap.get(OPENACCT_RESULT),
//								"5","-1","1",
//								new Timestamp(t)),
//								nullValue);
				
				context.write(new H5MergedRecord(
						statDt,
						channel,
						bizMap.get(PV) == null ? 0 : bizMap.get(PV),
						bizMap.get(UV) == null ? 0 : bizMap.get(UV),
						enter,
						bizMap.get(HUODONG_INDEX) == null ? 0 : bizMap.get(HUODONG_INDEX),
						bizMap.get(OPENACCT_INDEX) == null ? 0 : bizMap.get(OPENACCT_INDEX),
						bizMap.get(AUTH_INDEX) == null ? 0 : bizMap.get(AUTH_INDEX),
						bizMap.get(OPENACCT_RESULT) == null ? 0 : bizMap.get(OPENACCT_RESULT),0,
						"5","-1","1",
						new Timestamp(t),"1"), nullValue);
				
			}
			
			//“全部”渠道入库
//			context.write(
//					new H5PVRecord(
//							statDt,
//							ALL_CHANNEL,
//							allbizChannelMap.get(PV),
//							allbizChannelMap.get(UV),
//							totalenter,
//							allbizChannelMap.get(HUODONG_INDEX),
//							allbizChannelMap.get(OPENACCT_INDEX),
//							allbizChannelMap.get(AUTH_INDEX),
//							allbizChannelMap.get(OPENACCT_RESULT),
//							"5","-1","1",
//							new Timestamp(t)),
//							nullValue);
			
			context.write(new H5MergedRecord(
							statDt,
							ALL_CHANNEL,
							allbizChannelMap.get(PV),
							allbizChannelMap.get(UV),
							totalenter,
							allbizChannelMap.get(HUODONG_INDEX),
							allbizChannelMap.get(OPENACCT_INDEX),
							allbizChannelMap.get(AUTH_INDEX),
							allbizChannelMap.get(OPENACCT_RESULT),0,
							"5","-1","1",
							new Timestamp(t),"1"), nullValue);
			
			
			
			//明细渠道入库
			
			Map<String,Long> pv_map = biz2channelMap.get(PV);
			
			Map<String,Long> uv_map = biz2channelMap.get(UV);
			
			Map<String,Long> huuodongindex_map = biz2channelMap.get(HUODONG_INDEX);
			
			Map<String,Long> openacctindex_map = biz2channelMap.get(OPENACCT_INDEX);
			
			Map<String,Long> authindex_map = biz2channelMap.get(AUTH_INDEX);
			
			Map<String,Long> openacctresult_map = biz2channelMap.get(OPENACCT_RESULT);
			
			for(String channel : pv_map.keySet()){
				
				long pv = pv_map.get(channel);
				
				long uv = uv_map.get(channel) == null ? 0 : uv_map.get(channel);
				
				long hongdongindex = huuodongindex_map.get(channel) == null ? 0 : huuodongindex_map.get(channel);
				
				long openacctindex = openacctindex_map.get(channel) == null ? 0 : openacctindex_map.get(channel);
				
				long authindex = authindex_map.get(channel) == null ? 0 : authindex_map.get(channel);
				
				long openacctresultindex = openacctresult_map.get(channel) == null ? 0 : openacctresult_map.get(channel);
				
				long enter = channelEntersMap.get(channel) == null ? 0 : channelEntersMap.get(channel);
				
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
				
//				context.write(
//						new H5PVRecord(
//								statDt,
//								channel,
//								pv,
//								uv,
//								enter,
//								hongdongindex,
//								openacctindex,
//								authindex,
//								openacctresultindex,
//								type + "",root,level,
//								new Timestamp(t)),
//								nullValue);
				
				context.write(new H5MergedRecord(
						statDt,
						channel,
						pv,
						uv,
						enter,
						hongdongindex,
						openacctindex,
						authindex,
						openacctresultindex,
						0,
						type + "",root,level,
						new Timestamp(t),"1"), nullValue);
				
		}

	}

	
}
	public static void main(String args[]) throws Exception {

		ToolRunner.run(new Configuration(), new H5PVDailyAnly(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		DBConfiguration.configureDB(getConf(), "com.mysql.jdbc.Driver",
				"jdbc:mysql://192.168.220.157:3306/uaa_2", "admin", "123");
		
//		DBConfiguration.configureDB(getConf(), "com.mysql.jdbc.Driver",
//				"jdbc:mysql://10.70.70.27:3306/uaa", "uaa", "uaa_20150108");

		Job job = new Job(getConf(), "channel_view_stat_h5_" + args[1]);

		DBOutputFormat.setOutput(job, "channel_view_stat_h5", new String[] { "dt",
				"channel", "pv", "uv","enter", "huodong_index", "openacct_index", "auth_index", "open_result_index",
				"channel_type", "channel_parent", "channel_level","create_time" });

		job.setJarByClass(H5PVDailyAnly.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(DBOutputFormat.class);

		job.setOutputKeyClass(H5PVRecord.class);
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
}
