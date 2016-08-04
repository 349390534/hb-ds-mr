package com.howbuy.hadoop.mr.online;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

public class WebTradeEventAnly extends BaseAnly implements Tool {

	private static Logger logger = Logger.getLogger(WebTradeEventAnly.class);

	public static class EventMapper extends
			Mapper<LongWritable, Text, TaggedKey, Text> {

		private Splitter splitter;

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();

			List<String> values = Lists.newArrayList(splitter.split(line));

			if (values.size() == 32) {

				String guid = values.get(15);
				String timestamp = values.get(1);
				String type = values.get(2);
				String url = values.get(3);
				String txRslt = values.get(13);

				if (!ActionType.register.getValue().equals(type)
						&& !ActionType.bindCard.getValue().equals(type)
						&& !ActionType.buyFund.getValue().equals(type)
						&& !ActionType.piggyBuy.getValue().equals(type)
						&& !ActionType.savingPlan.getValue().equals(type))
					return;

				if ("0".equals(txRslt)) {// 失败统计
					if (ActionType.buyFund.getValue().equals(type)) {

						context.getCounter(GROUP_NAME, "BUY_FAILED").increment(
								1);

					} else if (ActionType.piggyBuy.getValue().equals(type)) {

						context.getCounter(GROUP_NAME, "SAVING_FAILED")
								.increment(1);

					} else if (ActionType.savingPlan.getValue().equals(type)) {

						context.getCounter(GROUP_NAME, "SAVINGPLAN_FAILED")
								.increment(1);

					}

					return;
				}

				context.getCounter(GROUP_NAME, "ALL").increment(1);

				Matcher m = HOWBUY_DOMAIN_PATTERN.matcher(url);
				if (!m.find()) {

					context.getCounter(GROUP_NAME, "INVALID_URL").increment(1);

					return;
				}

				if (isEmpty(guid) || isEmpty(timestamp)) {

					context.getCounter(GROUP_NAME, "INVALID_DATA").increment(1);

					return;
				}

				TaggedKey tag = new TaggedKey();
				tag.setGuid(guid);
				tag.setTimestamp(timestamp);

				context.write(tag, value);

			} else
				context.getCounter(GROUP_NAME, "NO_COMPLETE").increment(1);

		}

		@Override
		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {

			splitter = Splitter.on(FIELD_SEPARATOR).trimResults();
			
			
		}
	}

	public static class EventReducer extends
			Reducer<TaggedKey, Text, NullWritable, Text> {

		private Splitter splitter = Splitter.on(FIELD_SEPARATOR).trimResults();

		// 交易笔数统计
		private Map<String, Long> trade_num_stat = new HashMap<String, Long>();

		// 交易金额
		private Map<String, BigDecimal> trade_amt_stat = new HashMap<String, BigDecimal>();

		// 交易人数
		private Map<String, Long> trade_person_stat = new HashMap<String, Long>();

		// 绑卡人数
		private Map<String, Long> bind_card_stat = new HashMap<String, Long>();

		// 注册人数
		private Map<String, Long> open_acct_stat = new HashMap<String, Long>();

		// 所有渠道
		private Set<String> allChannels = new HashSet<String>();

		private static String BIND_CARD = "bind_card";

		private static String OPEN_ACCT = "open_acct";

		private static String TRADE_AMT = "trade_amt";

		private static String TRADE_BILLS = "trade_bills";

		private static String TRADE_PERONS = "trade_perons";

		private static Map<String, Map> CHANNEL_MAP = new HashMap<String, Map>();

		private static String[] channels = new String[] { ALL_SEARCH_CHANNEL,
				ALL_TUIGUANG_CHANNEL, ALL_OTHER_CHANNEL, ALL_DIRECT_CHANNEL };

		private static String[] bizs = new String[] { BIND_CARD, OPEN_ACCT,
				TRADE_AMT, TRADE_BILLS, TRADE_PERONS };

		{

			buildChannelMap(CHANNEL_MAP);
		}
		
		private Map buildChannelMap(Map source){
			
			Map s = source;
			
			if(s == null)
				s = new HashMap();
			
			for (int i = 0; i < channels.length; i++) {

				Map stat = new HashMap();

				s.put(channels[i], stat);

				for (int j = 0; j < bizs.length; j++) {

					stat.put(bizs[j], null);
				}
			}
			
			return s;
		}

		// 所有渠道汇总指标
		long total_bind_card = 0, total_open_acct = 0, total_trade_persons = 0,
				total_trade_bills = 0;

		BigDecimal total_trade_amt = BigDecimal.ZERO;

		public void reduce(TaggedKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			//当前渠道
			String currChannel = "";

			Map<String, Integer> temp_stat = new HashMap<String, Integer>();
			
			Map<String, Map> tempChannelMap = buildChannelMap(null);
			
			

			for (Text value : values) {

				context.getCounter(GROUP_NAME, "ALL").increment(1);

				String line = value.toString();

				List<String> list = Lists.newArrayList(splitter.split(line));

				String type = list.get(2);

				String otrack = list.get(24);

				String amtStr = list.get(7);

				if (!isEmpty(otrack)) {//有otrack

					Matcher m = OTRACK_PATTERN.matcher(otrack);

					
					if (!m.matches()) {// otrack格式不合法

						currChannel = OTHER_CHANNEL;

						context.getCounter(GROUP_NAME,
								"INVALID_FORMAT_TUIGUANG_OTAG").increment(1);

					} else {//合法otrack

						String[] arr = otrack.split("\\.");

						String[] body = arr[0].split("-");

						String htag = body[0];

						if (htag.equals("0")) {// 站内直接访问

							htag = DIRECT_CHANNEL;

						}
						
						// htag合法性判断
						if(!TUI_GUANG_HTAG_PATTERN.matcher(htag).matches() &&
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
				

				boolean istrade = false;

				if (ActionType.buyFund.getValue().equals(type)
						|| ActionType.piggyBuy.getValue().equals(type)
						|| ActionType.savingPlan.getValue().equals(type))
					istrade = true;

				Map bizmap = null;
				Map tempbizmap = null;

				if (DIRECT_CHANNEL.equals(currChannel) // 直接渠道
						|| SEARCH_PATTERN.matcher(currChannel).matches() // 搜索渠道
						|| OTHER_CHANNEL.equals(currChannel))// 其他渠道
				{

					// 查找对应渠道统计map
					if (DIRECT_CHANNEL.equals(currChannel)){
						
						bizmap = CHANNEL_MAP.get(ALL_DIRECT_CHANNEL);
						tempbizmap = tempChannelMap.get(ALL_DIRECT_CHANNEL);
					}
					
					else if (SEARCH_PATTERN.matcher(currChannel).matches()) {

						if (searches.containsKey(currChannel)){
							
							bizmap = CHANNEL_MAP.get(ALL_SEARCH_CHANNEL);
							tempbizmap = tempChannelMap.get(ALL_SEARCH_CHANNEL);
						}
						
						else if (other_searches.containsKey(currChannel)){
							
							bizmap = CHANNEL_MAP.get(ALL_OTHER_CHANNEL);
							tempbizmap = tempChannelMap.get(ALL_OTHER_CHANNEL);
						}
						
						else {

							context.getCounter(GROUP_NAME, "INVALID_HTAG").increment(1);
							
							bizmap = CHANNEL_MAP.get(ALL_OTHER_CHANNEL);
							tempbizmap = tempChannelMap.get(ALL_OTHER_CHANNEL);
						}
					} else if (OTHER_CHANNEL.equals(currChannel)) {

						bizmap = CHANNEL_MAP.get(ALL_OTHER_CHANNEL);
						tempbizmap = tempChannelMap.get(ALL_OTHER_CHANNEL);

					}

					String firstClass = currChannel;

					if (istrade) {

						BigDecimal amt = new BigDecimal(amtStr);

						// 累加金额
						getAndIncr4trade(bizmap, TRADE_AMT, amt);
						getAndIncr4trade(tempbizmap, TRADE_AMT, amt);
						

						// 累加笔数
						getAndIncr(bizmap, TRADE_BILLS, 1l);
						getAndIncr(tempbizmap, TRADE_BILLS, 1l);

						temp_stat.put(firstClass, 1);

						getAndIncr4trade(trade_amt_stat, firstClass, amt);

						getAndIncr(trade_num_stat, firstClass, 1l);

					} else if (ActionType.bindCard.getValue().equals(type)) {

						// 累加帮卡数
						getAndIncr(bizmap, BIND_CARD, 1l);
						getAndIncr(tempbizmap, BIND_CARD, 1l);

						getAndIncr(bind_card_stat, firstClass, 1l);

					} else if (ActionType.register.getValue().equals(type)) {

						// 累加 开户数
						getAndIncr(bizmap, OPEN_ACCT, 1l);
						getAndIncr(tempbizmap, OPEN_ACCT, 1l);

						getAndIncr(open_acct_stat, firstClass, 1l);
					}

					allChannels.add(firstClass);

				} else if (TUI_GUANG_HTAG_PATTERN.matcher(currChannel).matches()) {// 推广

					// 解析三级渠道
					String firstClass = currChannel.substring(0, 3);

					String secondClass = currChannel.substring(0, 6);

					String thirdClass = currChannel.substring(0, 10);
					
					allChannels.add(firstClass);
					allChannels.add(secondClass);
					allChannels.add(thirdClass);
					
					bizmap = CHANNEL_MAP.get(ALL_TUIGUANG_CHANNEL);
					tempbizmap = tempChannelMap.get(ALL_TUIGUANG_CHANNEL);

					if (istrade) {

						BigDecimal amt = new BigDecimal(amtStr);

						// 累加金额
						getAndIncr4trade(bizmap, TRADE_AMT, amt);
						getAndIncr4trade(tempbizmap, TRADE_AMT, amt);

						// 累加笔数
						getAndIncr(bizmap, TRADE_BILLS, 1l);
						getAndIncr(tempbizmap, TRADE_BILLS, 1l);

						temp_stat.put(firstClass, 1);
						temp_stat.put(secondClass, 1);
						temp_stat.put(thirdClass, 1);

						getAndIncr4trade(trade_amt_stat, firstClass, amt);
						getAndIncr4trade(trade_amt_stat, secondClass, amt);
						getAndIncr4trade(trade_amt_stat, thirdClass, amt);

						getAndIncr(trade_num_stat, firstClass, 1l);
						getAndIncr(trade_num_stat, secondClass, 1l);
						getAndIncr(trade_num_stat, thirdClass, 1l);

					} else if (ActionType.register.getValue().equals(type)) {

						// 累加 开户数
						getAndIncr(bizmap, OPEN_ACCT, 1l);
						getAndIncr(tempbizmap, OPEN_ACCT, 1l);

						getAndIncr(open_acct_stat, firstClass, 1l);
						getAndIncr(open_acct_stat, secondClass, 1l);
						getAndIncr(open_acct_stat, thirdClass, 1l);

					} else if (ActionType.bindCard.getValue().equals(type)) {

						// 累加帮卡数
						getAndIncr(bizmap, BIND_CARD, 1l);
						getAndIncr(tempbizmap, BIND_CARD, 1l);

						getAndIncr(bind_card_stat, firstClass, 1l);
						getAndIncr(bind_card_stat, secondClass, 1l);
						getAndIncr(bind_card_stat, thirdClass, 1l);
					}

				} else {

					context.getCounter(GROUP_NAME, "INVALID_CURR_CHANNEL")
							.increment(1);

					continue;
				}

			}

			// 统计推广渠道 1,2,3级渠道交易人数
			for (String channel : temp_stat.keySet()) {

				long num = temp_stat.get(channel);

				Long curnum = trade_person_stat.get(channel);

				if (curnum != null) {

					num += curnum;
				}

				trade_person_stat.put(channel, num);

			}


			// 累加所有渠道交易金额，根据金额是否大于0，累加交易人数
			BigDecimal tempAmt = BigDecimal.ZERO;
			
			// 计算各渠道交易人数
			for (String channel : CHANNEL_MAP.keySet()) {

				Map bizmap = (Map) CHANNEL_MAP.get(channel);
				
				Map tempbizmap = (Map)tempChannelMap.get(channel);

				BigDecimal amt = (BigDecimal) tempbizmap.get(TRADE_AMT);

				// 根据金额判断是否有过交易
				if (amt != null && amt.compareTo(BigDecimal.ZERO) > 0) {

					getAndIncr(bizmap, TRADE_PERONS, 1l);

					tempAmt = tempAmt.add(amt);
				}

				//累加当前渠道交易笔数
				Long bills = (Long) tempbizmap.get(TRADE_BILLS);
				if (bills != null)
					total_trade_bills += bills;

				//累加当前渠道开户人数
				Long openacct = (Long) tempbizmap.get(OPEN_ACCT);
				if (openacct != null)
					total_open_acct += openacct;

				//累加当前渠道绑卡人数
				Long bindcard = (Long) tempbizmap.get(BIND_CARD);
				if (bindcard != null)
					total_bind_card += bindcard;
			}

			//所有渠道交易金额/交易人数 
			if (tempAmt.compareTo(BigDecimal.ZERO) > 0) {

				total_trade_amt = total_trade_amt.add(tempAmt);

				total_trade_persons += 1;
			}
		}

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
				logger.error(e);
				return;
			}

			//非汇总数据入库
			for (String channel : allChannels) {

				int type = getType(channel);

				String root = null;

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

					root = level.equals("1") ? null
							: level.equals("2") ? channel.substring(0, 3)
									: channel.substring(0, 6);
					break;

				default:
					root = "-1";
					level = "-1";

				}

				long trade_persons = trade_person_stat.get(channel) == null ? 0
						: trade_person_stat.get(channel);

				long trade_num = trade_num_stat.get(channel) == null ? 0
						: trade_num_stat.get(channel);

				BigDecimal amt = trade_amt_stat.get(channel) == null ? BigDecimal.ZERO
						: trade_amt_stat.get(channel);

				long bindcards = bind_card_stat.get(channel) == null ? 0
						: bind_card_stat.get(channel);

				long openaccts = open_acct_stat.get(channel) == null ? 0
						: open_acct_stat.get(channel);

				context.write(new TradeRecord(statDt, channel, trade_persons,
						trade_num, amt, bindcards, openaccts, type, root,
						level, new Timestamp(t)), NullWritable.get());

			}

			// 单个渠道数据汇总入库
			for (String channel : CHANNEL_MAP.keySet()) {

				Map bizmap = CHANNEL_MAP.get(channel);

				BigDecimal channel_trade_amt = (BigDecimal) bizmap.get(TRADE_AMT);

				Long channel_trade_person = (Long) bizmap.get(TRADE_PERONS);

				Long channel_trade_bills = (Long) bizmap.get(TRADE_BILLS);

				Long channel_open_acct = (Long) bizmap.get(OPEN_ACCT);

				Long channel_bind_card = (Long) bizmap.get(BIND_CARD);

				context.write(new TradeRecord(statDt, channel,
						channel_trade_person == null ? 0 : channel_trade_person,
						channel_trade_bills == null ? 0 : channel_trade_bills,
						channel_trade_amt == null ? BigDecimal.ZERO : channel_trade_amt,
						channel_bind_card == null ? 0 : channel_bind_card,
						channel_open_acct == null ? 0 : channel_open_acct,
						5, null, "1", new Timestamp(t)),
						NullWritable.get());
			}

			// 所有渠道汇总
			context.write(new TradeRecord(statDt, ALL_CHANNEL,
					total_trade_persons, total_trade_bills, total_trade_amt,
					total_bind_card, total_open_acct, 5, null, "1",
					new Timestamp(t)), NullWritable.get());

		}
	}

	@Override
	public int run(String[] args) throws Exception {

//		DBConfiguration.configureDB(getConf(), "com.mysql.jdbc.Driver",
//				"jdbc:mysql://192.168.220.157:3306/uaa_2", "admin", "123");
		
		DBConfiguration.configureDB(getConf(), "com.mysql.jdbc.Driver",
				getMySqlConnectionURL(), "uaa", "uaa_20150108");

		Job job = new Job(getConf(), "web_trade_anly_" + args[1]);

		DBOutputFormat.setOutput(job, "channel_trade_stat", new String[] {
				"dt", "channel", "persons", "bills", "amt", "bind_card",
				"open_acct", "channel_type", "channel_parent",
				"channel_level,createtime" });

		job.setJarByClass(WebTradeEventAnly.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(DBOutputFormat.class);

		job.setOutputKeyClass(TaggedKey.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(EventMapper.class);
		job.setReducerClass(EventReducer.class);

		job.setPartitionerClass(GenPartitioner.class);
		job.setGroupingComparatorClass(GuidGroupingComparator.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));

		job.getConfiguration().set("dt",
				StringUtils.isEmpty(args[1]) ? null : args[1]);
		// 开始运行
		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String args[]) throws Exception {

		ToolRunner.run(new Configuration(), new WebTradeEventAnly(), args);

	}
}
