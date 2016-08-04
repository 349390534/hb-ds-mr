package com.howbuy.hadoop.mr.online;

import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configured;

public class BaseAnly extends Configured {
	
	protected static String FIELD_SEPARATOR = "\u0001";

	// 直接访问渠道
	protected static String DIRECT_CHANNEL = "9999999999";

	// 其他访问渠道
	protected static String OTHER_CHANNEL = "9999999998";
	
	//搜索渠道归总
	protected static String ALL_SEARCH_CHANNEL = "9999999997";
	
	//推广渠道归总
	protected static String ALL_TUIGUANG_CHANNEL = "9999999996";
	
	//所有其他渠道归总
	protected static String ALL_OTHER_CHANNEL = "9999999995";
	
	// 直接访问渠道
	protected static String ALL_DIRECT_CHANNEL = "9999999994";
	
	//所有渠道归总
	protected static String ALL_CHANNEL = "9999999993";
	
	// 通配所有htag
	protected static Pattern OTRACK_PATTERN = Pattern
			.compile("\\d+-\\d+-\\d+-\\d+\\.[0-3]");

	// 通配所有htag
	protected static Pattern HTAG_PATTERN = Pattern
//			.compile("[0-3]\\.(?:\\d{10}90{5}|\\d{1,2}80{5})");
			.compile("[0-3]\\.(?:\\d{6,})");
	// 通配推广htag
	protected static Pattern TUI_GUANG_HTAG_PATTERN = Pattern
			.compile("\\d{10}90{5}");
	// 通配搜索htag
	protected static Pattern SEARCH_PATTERN = Pattern.compile("[1-6]80{5}");
	

	//desturl howbuy匹配
	protected static Pattern HOWBUY_DOMAIN_PATTERN = Pattern
			.compile("(?:http|https)://((?!m|mzt|zt|data).*)\\.(?:howbuy|ehowbuy)");
	
	//站内匹配
		protected static Pattern INTERNAL_HOWBUY_DOMAIN_PATTERN = Pattern
				.compile("(?:http|https)://.*\\.(?:howbuy|ehowbuy)");
		
	private static String MYSQL_CONN_URL = "jdbc:mysql://10.11.102.49/uaa";
	
	private static String ORACL_CONN_URL = "jdbc:oracle:thin:@10.60.60.30:1521:rptods";
	
	
	//其他渠道
	protected static Map<String,Long> other_searches = new HashMap<String, Long>();
	
	protected static Map<String,Long> searches = new HashMap<String, Long>();
	
	protected static Map<String,String> searches_domain = new HashMap<String, String>();
		
	static{
		other_searches.put("7800000", 1l);
		other_searches.put("8800000", 1l);
		other_searches.put("9800000", 1l);
		other_searches.put("10800000", 1l);
		other_searches.put("11800000", 1l);
		other_searches.put("12800000", 1l);
		other_searches.put("13800000", 1l);
		
		searches.put("1800000", 1l);
		searches.put("2800000", 1l);
		searches.put("3800000", 1l);
		searches.put("4800000", 1l);
		searches.put("5800000", 1l);
		searches.put("6800000", 1l);
		
		searches_domain.put("sogou.com", "1800000");
		searches_domain.put("baidu.com", "2800000");
		searches_domain.put("google.com", "3800000");
		searches_domain.put("haosou.com", "4800000");
		searches_domain.put("bing.com", "5800000");
		searches_domain.put("sm.cn", "6800000");
		
		searches_domain.put("qq.com", "7800000");
		searches_domain.put("ifeng.com", "8800000");
		searches_domain.put("go.uc.cn", "9800000");
		searches_domain.put("hao123.com", "10800000");
		searches_domain.put("sina.com.cn", "11800000");
		searches_domain.put("youdao.com", "12800000");
		searches_domain.put("maxthon.cn", "13800000");
		
		
	}
	
	protected static final String GROUP_NAME = "HOW_BUY";
	
	
	protected static boolean isEmpty(String val) {

		if (StringUtils.isEmpty(val) || "null".equalsIgnoreCase(val))
			return true;
		return false;
	}
	
	protected static Map<String, Long> getAndIncr(Map<String, Long> map,
			String key, Long num) {
		
		if(null == num)
			return map;

		Long cnum = map.get(key);
		if (null == cnum) {
			map.put(key, num);
		} else {
			cnum += num;
			map.put(key, cnum);
		}
		return map;
	}
	
	protected static Map<String, BigDecimal> getAndIncr4trade(Map<String, BigDecimal> map,
			String key, BigDecimal amt) {

		BigDecimal curramt = map.get(key);
		if (null == curramt) {
			map.put(key, amt);
		} else {
			curramt = curramt.add(amt);
			map.put(key, curramt);
		}
		return map;
	}
	
	/*
	 * 根据渠道名，返回所属渠道类型
	 *  1:直接访问，2：其他渠道(包含搜索引擎列表部分)，3：搜索引擎，4：推广 ，5:汇总
	 */
	protected static int getType(String channel){
		
		return  DIRECT_CHANNEL.equals(channel) ? 1 : 
			OTHER_CHANNEL.equals(channel) ? 2 : 
			channel.matches("\\d{3}|\\d{6}|\\d{10}") ? 4 :
			searches.containsKey(channel) ? 3 : other_searches.containsKey(channel) ? 2 : -1;
	}
	
	public static String getChannel(String url) throws URISyntaxException{
		
		URI uri = new URI(url);
		String domainurl = uri.getHost();
		
		String tempchannel = null;
		
		if(null != domainurl)
			for(String channel : searches_domain.keySet()){
				if(domainurl.contains(channel)){
					
					tempchannel = searches_domain.get(channel);
					break;
				}
			}
		
		return tempchannel;
	}
	
	protected String getMySqlConnectionURL(){
		return MYSQL_CONN_URL;
	}
	
	protected String getOraclConnectionURL(){
		
		return ORACL_CONN_URL;
	}

//	public static void main(String[] args) {
//		
//		try {
//			String channnel = getChannel("https://www.ifeng.com");
//			
//			System.out.println(channnel);
//			
//		} catch (URISyntaxException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
}
