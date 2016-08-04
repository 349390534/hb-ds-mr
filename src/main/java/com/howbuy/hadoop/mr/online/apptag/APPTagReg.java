package com.howbuy.hadoop.mr.online.apptag;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.howbuy.hadoop.hive.CustNoEncodeUtils;
import com.howbuy.hadoop.mr.online.BaseAnly;

public class APPTagReg extends BaseAnly implements Tool{
	
	private static Logger logger = Logger.getLogger(APPTagReg.class);
	
	private static String GROUP = "howbuy";
	
	public static class TagMapper extends Mapper<LongWritable, Text, AppTag,AppTag>{

		private Splitter splitter;
		
		private AppTag tag = new AppTag();
		
		private Pattern isInValidGuid = Pattern.compile("0+|-+|^\\s*$");
		
		String procfile;
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			
			splitter = Splitter.on(FIELD_SEPARATOR).trimResults();
			
			
		}
		
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			
			
			List<String> values = Lists.newArrayList(splitter.split(value.toString()));
			
			if(values.size() != 13){
				
				context.getCounter(GROUP,"invalid data").increment(1);
				
				return;
			}
			
			
			Timestamp ts;
			try {
				ts = new Timestamp(Long.parseLong(values.get(0)));
			} catch (NumberFormatException e) {
				logger.warn("invalid time");
				context.getCounter(GROUP,"invalid time").increment(1);
				return;
			}
			
			//3001、3002掌基，2001、2002储蓄罐
			String proid = values.get(1);
			
			int type;
			try {
				type = Integer.parseInt(values.get(5));
				
//				if(type == 2){//后台再次激活忽略，只关注首次激活，首次登陆
//					
//					context.getCounter(GROUP,"type=2 data").increment(1);
//					return;
//				}
			} catch (NumberFormatException e) {
				context.getCounter(GROUP,"invalid type").increment(1);
				return;
			}
			
			String custno = values.get(6);
			//客户号解密
			if(!"".equals(custno) && null != custno && !"0".equals(custno))
				custno = CustNoEncodeUtils.getDecMsg(custno);
			if(null == custno){
				
				custno = "0";
				context.getCounter(GROUP, "invalid custno");
			}
			
			String guid = "";
			
			int flag = -1;
			
			if("2002".equals(proid) || "3002".equals(proid)){//android取值imei
				
				guid = values.get(2);
				flag = 1;
				
			}else if("2001".equals(proid) || "3001".equals(proid)){//ios取值guid
				
				guid = values.get(3);
				flag = 2;
				
			}
			//过滤非法guid
			if(isInValidGuid.matcher(guid).matches() && "0".equals(custno)){
				
				context.getCounter(GROUP,"invalid guid").increment(1);
				return;
			}
			
			tag.setTag(proid, guid, custno, ts,type,flag);
			
			context.write(tag, tag);
			
		}

	}
	
	
	public static class TagReducer extends Reducer<AppTag, AppTag, NullWritable, NullWritable>{
		
		private static String SELECT_STATEMEMT = "select * from raw_user_login_log where deviceid = ? and custno = ?";
		
		private static String UPDATE_STATEMENT_PREFIX = "update raw_user_login_log set custno=?,chuxuguan_firsttime=?,zhangji_firsttime=?,flag=?,ts=? where deviceid=? and custno=?";
		
		private static String INSERT_STATEMENT = "insert into raw_user_login_log (deviceid,custno,chuxuguan_firsttime,zhangji_firsttime,flag,ts) values (?,?,?,?,?,?)";
		
		private static ConnectionDB conDB;
		
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration();
			
			Properties prop = new Properties();
			prop.setProperty("driver", conf.get("driver"));
			prop.setProperty("url", conf.get("url"));
			prop.setProperty("username", conf.get("username"));
			prop.setProperty("password", conf.get("password"));
			
			conDB = new ConnectionDB(prop);
			
		}
		

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
		}

		
		@Override
		protected void reduce(AppTag key, Iterable<AppTag> values,Context context)
				throws IOException, InterruptedException {
			
			
			String guid = key.getGuid();	
			
			String custno = key.getCustno();
			
			int flag = key.getFlag();
			
			Date chuxuguanTS = null;
			Date zhangjiTS = null;
			String dbcustno = null;
			
			
			
			List<Map<String, Object>> list =  conDB.executeQuery(SELECT_STATEMEMT, 
					new Object[]{guid,custno});
			
			if(list.size() > 0){//有对应数据,查看是否有掌基，储蓄罐，客户号数据。
				
				//一个guid，一个custno对应至多一条记录
				Map<String,Object> rec = list.get(0);
				
				chuxuguanTS = (Timestamp)rec.get("CHUXUGUAN_FIRSTTIME");
				zhangjiTS = (Timestamp)rec.get("ZHANGJI_FIRSTTIME");
				dbcustno = (String)rec.get("CUSTNO");
				
				//都有数据表示无需更新
				if(chuxuguanTS != null && zhangjiTS != null && dbcustno != null){
					context.getCounter(GROUP,"compleleted data").increment(1);
					return;
				}
				
			}
			
			
			
			//循环values，找到缺少的储蓄罐首次时间，掌基首次时间，或者客户号，并更新入库
			for(AppTag tag : values){
				
				boolean isChuxuguan = false;
				
				if("3001".equals(tag.getProid()) || "3002".equals(tag.getProid())){//掌基
					
					isChuxuguan = false;
					
				}else if("2001".equals(tag.getProid()) || "2002".equals(tag.getProid())){//储蓄罐
					
					isChuxuguan = true;
				}else{
					
					context.getCounter(GROUP, "invalid proid");
					continue;
				}
				
				if(isChuxuguan){//储蓄罐
					
					if(chuxuguanTS == null){
						
						chuxuguanTS = tag.getTs();
						
					}
					
				}else{//掌基
					
					if(zhangjiTS == null){
						
						zhangjiTS = tag.getTs();
						
					}
				}
				
				if(dbcustno == null)
					dbcustno = new String(tag.getCustno());
				
				if(chuxuguanTS != null && zhangjiTS != null && dbcustno != null)
					break;
				
			}
			
			
			List param = new ArrayList();
			
			Timestamp ts = new Timestamp(System.currentTimeMillis());
			
			if(list.size() > 0){
				
				param.add(new Object[]{dbcustno,chuxuguanTS,zhangjiTS,flag,ts,guid,custno});
				conDB.executeUpdate(UPDATE_STATEMENT_PREFIX, param);
				
			}else{//直接插入
				
				param.add(new Object[]{key.getGuid(),dbcustno,chuxuguanTS,zhangjiTS,flag,ts});
				conDB.executeUpdate(INSERT_STATEMENT, param);
			}
			
			context.write(NullWritable.get(), NullWritable.get());
			
		}

	}
	

	public static void main(String[] args) {
		
		try {
			ToolRunner.run(new Configuration(), new APPTagReg(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}


	@Override
	public int run(String[] args) throws Exception {
		
//		Configuration conf = new Configuration();
//		conf.set("fs.defaultFS", "hdfs://192.168.220.154:9000");
//		setConf(conf);
		
		Job job = new Job(getConf(), "raw_user_login_log_anly");

		job.setJarByClass(APPTagReg.class);

		job.setInputFormatClass(TextInputFormat.class);
		

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);

		job.setMapOutputKeyClass(AppTag.class);
		job.setMapOutputValueClass(AppTag.class);

		job.setMapperClass(TagMapper.class);
		job.setReducerClass(TagReducer.class);

		job.setPartitionerClass(AppTagPartitioner.class);
		job.setGroupingComparatorClass(AppTagWritableComparator.class);
		
//		FileSystem fs = FileSystem.get(getConf());
//		
//		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
//		Date start = df.parse(args[0]);
//		Date end = df.parse(args[1]);
//		
//		
//		while(start.compareTo(end) <=0){
//			
//			String p = String.format("/hive/uac/dt=%s/channel=app/activation", df.format(start));
//			
//			Path path = new Path(p);
//			
//			if(fs.exists(path)){
//				
//				FileInputFormat.addInputPath(job, path);
//			}
//			
//			
//			Calendar c = Calendar.getInstance();
//			c.setTime(start);
//			c.add(Calendar.DAY_OF_MONTH, 1);
//			
//			start = c.getTime();
//		}
//		
//		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		
//		job.getConfiguration().set("driver", "com.mysql.jdbc.Driver");
//		job.getConfiguration().set("url", "jdbc:mysql://192.168.220.157:3306/uaa_2");
//		job.getConfiguration().set("username", "admin");
//		job.getConfiguration().set("password", "123");
		

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.getConfiguration().set("driver", "oracle.jdbc.driver.OracleDriver");
		job.getConfiguration().set("url", getOraclConnectionURL());
		job.getConfiguration().set("username", "rptods");
		job.getConfiguration().set("password", "ie!QG9dv7i");
		
		
		job.setNumReduceTasks(10);
		// 开始运行
		job.waitForCompletion(true);

		return 0;
	}

}
