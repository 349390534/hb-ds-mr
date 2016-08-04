package com.howbuy.hadoop.mr.online;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 * 广告点击量统计 网站使用
 * 统计每个模块下，每个页面中，每个广告位每天的点击量
 * @author qiankun.li
 * 
 */
public class WebAdvertisementClick extends BaseAnly implements Tool {

	private static Logger logger = Logger
			.getLogger(WebAdvertisementClick.class);

	private static String FIELD_SEPARATOR = "\u0001";

	// 广告位banner，由致力根据cms系统规则，a标签内tag值从10001开始自增，到19999结束
	private static Pattern TAG_PATTERN = Pattern.compile("^1\\d{4}$");

	private static final String GROUP_NAME = "HOW_BUY_GG";

	public static class PVMapper extends
			Mapper<LongWritable, Text, ChannelTagClickKey, Text> {

		private Splitter splitter = Splitter.on(FIELD_SEPARATOR).trimResults();
		private ChannelTagClickKey clickKey = new ChannelTagClickKey();
		private Text word = new Text();
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			context.getCounter(GROUP_NAME, "ALL").increment(1);

			String line = value.toString();

			List<String> values = Lists.newArrayList(splitter.split(line));

			if (values.size() == 17) {

				String guid = values.get(0);
				String timestamp = values.get(1);

				if (isEmpty(guid) || isEmpty(timestamp)
						|| !guid.startsWith("0x")) {

					context.getCounter(GROUP_NAME, "INVALID_DATA").increment(1);
					return;
				}
				
				String tag = values.get(14);
				if (!isEmpty(tag)) {
					Matcher m = TAG_PATTERN.matcher(tag);
					if ("10000".equals(tag) || !m.matches()) {
						context.getCounter(GROUP_NAME, "INVLID_GG_TAG")
								.increment(1);
						return;
					}
				} else {
					context.getCounter(GROUP_NAME, "INVLID_GG_DATA").increment(1);
					return;
				}
				String pageId = values.get(12);
				if(isEmpty(pageId)){
					pageId="";//默认给个""
				}
				String proid = values.get(16);
				// 为空则 标识为网站广告
				if (isEmpty(proid))
					proid = ProidType.PROID_AD.getIndex();//后续处理其他站点广告
				clickKey.setTag(tag);
				clickKey.setProid(proid);
				clickKey.setPageId(pageId);
				context.write(clickKey, word);
			} else
				context.getCounter(GROUP_NAME, "NO_COMPLETE").increment(1);

		}
	}

	public static class PVReducer extends
			Reducer<ChannelTagClickKey, Text, ChannelTagClickRecord, NullWritable> {

		private NullWritable nullValue = NullWritable.get();

		public void reduce(ChannelTagClickKey key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			String tag = key.getTag();
			String proid = key.getProid();
			String pageid = key.getPageId();
			long clicknum = 0;
			Iterator<Text> it = values.iterator();
			while (it.hasNext()) {
				it.next();//必须调用，改变指针
				clicknum++;
			}
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
			ChannelTagClickRecord channelPageRecord = new ChannelTagClickRecord(statDt,tag,proid, pageid, clicknum, ct);
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
		/*
		 * DBConfiguration.configureDB(getConf(), "com.mysql.jdbc.Driver",
		 * "jdbc:mysql://10.70.70.27/uaa", "uaa", "uaa_20150108");
		 */
		DBConfiguration.configureDB(getConf(), "com.mysql.jdbc.Driver",
				getMySqlConnectionURL(), "admin", "123");

		Job job = new Job(getConf(), "ad_click_job_" + args[1]);
		
		DBOutputFormat.setOutput(job, "channel_ad_click", new String[] { "dt",
				"tag","proid", "page_id","click_num", "create_time"});
		
		job.setJarByClass(WebAdvertisementClick.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(DBOutputFormat.class);

		job.setOutputKeyClass(ChannelTagClickRecord.class);
		job.setOutputValueClass(NullWritable.class);

		job.setMapOutputKeyClass(ChannelTagClickKey.class);
		job.setMapOutputValueClass(Text.class);

		job.setMapperClass(PVMapper.class);
		job.setReducerClass(PVReducer.class);

		job.setGroupingComparatorClass(ChannelTagClickComparator.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		job.getConfiguration().set("dt", args[1]);
		// 开始运行
		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String args[]) throws Exception {

		ToolRunner.run(new Configuration(), new WebAdvertisementClick(), args);
	}
}
