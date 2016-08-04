/**
 * 
 */
package com.howbuy.hadoop.mr.online.apptag;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.log4j.Logger;

/**
 * @author qiankun.li
 * 
 */
public class ConnectionDB {

	private static final Logger LOGGER = Logger.getLogger(ConnectionDB.class);

	private static String driver = null;

	private static String url = null;

	private static String userName = null;

	private static String password = null;

	private static DataSource dataSource;

	public ConnectionDB(Properties jdbc) {
		if(null==dataSource){
			Properties properties = new Properties();
			try {
				driver = jdbc.getProperty("driver");
				url = jdbc.getProperty("url");
				userName = jdbc.getProperty("username");
				password = jdbc.getProperty("password");
				properties.put("driverClassName", driver);
				properties.put("initialSize",5);
				properties.put("maxTotal", 20);
				properties.put("maxIdle", 10);
				properties.put("minIdle", 5);
				//The indication of whether objects will be validated by the idle object evictor (if any). If an object fails to validate, it will be dropped from the pool
				properties.put("testWhileIdle", true);
				//The minimum amount of time an object may sit idle in the pool before it is eligable for eviction by the idle object evictor (if any). 
				properties.put("minEvictableIdleTimeMillis", 1000 * 60 * 5);//五分钟检查一次
				//properties.put("user", userName);
				properties.put("username", userName);
				properties.put("password", password);
				properties.put("url", url);
				dataSource = BasicDataSourceFactory.createDataSource(properties);
				//PoolingDataSource.getDataSource().setUpDriver(url, properties);
				//dataSource = PoolingDataSource.getDataSource().setupDataSource(url, properties);
			} catch (Exception e) {
				LOGGER.error("init PoolDataSource error", e);
			}
		}
	}

	/**
	 * 获取数据库连接
	 * 
	 * @return
	 */
	public Connection getConnection() {
		Connection connectionTmp = null;
		// 获取连接
		try {
			connectionTmp = dataSource.getConnection();
		} catch (SQLException e) {
			LOGGER.error("getConnection error,{}", e);
		}
		return connectionTmp;
	}

	/**
	 * insert update delete SQL语句的执行的统一方法
	 * 
	 * @param sql
	 *            SQL语句
	 * @param params
	 *            参数数组，若没有参数则为null
	 * @return 受影响的行数
	 */
	public int executeUpdate(String sql, List<Object[]> list) {
		long start = System.currentTimeMillis();
		// 受影响的行数
		int affectedLine = 0;
		int num = 0;
		PreparedStatement preparedStatement = null;
		Connection connection = null;
		try {
			// 获得连接
			connection = this.getConnection();
			connection.setAutoCommit(false);
			if (null == connection) {
				LOGGER.error("connnection is null");
				return -1;
			}
			// 调用SQL
			LOGGER.debug("the sql is :"+sql);
			preparedStatement = connection.prepareStatement(sql);
			// 参数赋值

			if(null!=list){
				num = list.size();
				int object_index = 1;
				for(Object[] params:list){
					for (int i = 0; i < params.length; i++) {
						preparedStatement.setObject(object_index, params[i]);
						object_index ++ ;
					}
//					preparedStatement.addBatch();//添加一个批量执行,每个批处理sql都是一个可以单独执行的sql语句
				}
				// 执行
				affectedLine = preparedStatement.executeUpdate();
				
				connection.commit();
			}

		} catch (SQLException e) {
			
			LOGGER.error("insert error",e);
			try {
				connection.rollback();
			} catch (SQLException e1) {
				LOGGER.error("executeUpdate connnection.rollback() exception", e);
			}
		} finally {
			// 释放资源
			closeAll(null, preparedStatement, null, connection);
		}
		long end = System.currentTimeMillis();
		LOGGER.info("sql:" + sql + " in milsecs " + (System.currentTimeMillis()-start));
		return affectedLine;
	}

	/**
	 * 获取结果集，并将结果放在List中
	 * 
	 * @param sql
	 *            SQL语句
	 * @return List 结果集
	 */
	public List<Map<String, Object>> executeQuery(String sql, Object[] params) {
		
		long start = System.currentTimeMillis();
		
		List<Map<String, Object>> list = new LinkedList<Map<String, Object>>();
		
		// 执行SQL获得结果集
		ResultSet rs = null;
		
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		try {
			// 获得连接
			connection = this.getConnection();
			if (null == connection) {
				LOGGER.error("connnection is null");
				return list;
			}
			// 调用SQL
			preparedStatement = connection.prepareStatement(sql);

			// 参数赋值
			if (params != null) {
				for (int i = 0; i < params.length; i++) {
					preparedStatement.setObject(i + 1, params[i]);
				}
			}
			// 执行
			rs = preparedStatement.executeQuery();
		}catch(SQLException e){
			LOGGER.error("sql is "+sql);
		}
		
		if (rs == null) {
			return list;
		}
		// 创建ResultSetMetaData对象
		ResultSetMetaData rsmd = null;

		// 结果集列数
		int columnCount = 0;
		try {
			rsmd = rs.getMetaData();

			// 获得结果集列数
			columnCount = rsmd.getColumnCount();
		} catch (SQLException e1) {
			LOGGER.error("excuteQuery exception ", e1);
		}

		

		try {
			// 将ResultSet的结果保存到List中
			while (rs.next()) {
				Map<String, Object> map = new HashMap<String, Object>();
				for (int i = 1; i <= columnCount; i++) {
					
					if(rsmd.getColumnLabel(i).endsWith("TIME")){
						
						map.put(rsmd.getColumnLabel(i), rs.getTimestamp(i) == null ? null : rs.getTimestamp(i));
						
					}else{
						
						map.put(rsmd.getColumnLabel(i), rs.getObject(i));
					}
				}
				list.add(map);
			}
		} catch (SQLException e) {
			LOGGER.error("excuteQuery exception ", e);
		}finally{
		// 释放资源
			closeAll(rs, preparedStatement, null, connection);
		}
		
		LOGGER.info("sql:" + sql + " in milsecs " + (System.currentTimeMillis()-start));
		return list;
	}
	
	public Long count(String sql, Object[] params) {
		Long count = new Long(0);
		// 执行SQL获得结果集
		ResultSet rs = null;
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		try {
			// 获得连接
			connection = this.getConnection();
			if (null == connection) {
				LOGGER.error("connnection is null");
				return null;
			}
			// 调用SQL
			preparedStatement = connection.prepareStatement(sql);

			// 参数赋值
			if (params != null) {
				for (int i = 0; i < params.length; i++) {
					preparedStatement.setObject(i + 1, params[i]);
				}
			}
			// 执行
			rs = preparedStatement.executeQuery();
		} catch (SQLException e) {
			LOGGER.error("executeQueryRS exception ", e);
		}
		if (rs == null) {
			return null;
		}
		try {
			while(rs.next()){
				count = rs.getLong(1);
				break;
			}
		} catch (SQLException e) {
			LOGGER.error("excuteQuery exception ", e);
		}finally{
		// 释放资源
			closeAll(rs, preparedStatement, null, connection);
		}
		return count;
	
	}
	/**
	 * 存储过程带有一个输出参数的方法
	 * 
	 * @param sql
	 *            存储过程语句
	 * @param params
	 *            参数数组
	 * @param outParamPos
	 *            输出参数位置
	 * @param SqlType
	 *            输出参数类型
	 * @return 输出参数的值
	 */
	public Object executeQuery(String sql, Object[] params, int outParamPos,
			int SqlType) {
		Object object = null;
		Connection connnection = null;
		CallableStatement callableStatement  = null;
		try {
			connnection = this.getConnection();
			if (null == connnection) {
				LOGGER.error("connnection is null");
				return null;
			}
			// 调用存储过程
			callableStatement= connnection.prepareCall(sql);

			// 给参数赋值
			if (params != null) {
				for (int i = 0; i < params.length; i++) {
					callableStatement.setObject(i + 1, params[i]);
				}
			}

			// 注册输出参数
			callableStatement.registerOutParameter(outParamPos, SqlType);

			// 执行
			callableStatement.execute();

			// 得到输出参数
			object = callableStatement.getObject(outParamPos);

		} catch (SQLException e) {
			LOGGER.error("excuteQuery exception ", e);
		} finally {
			// 释放资源
			closeAll(null, null, callableStatement, connnection);
		}

		return object;
	}

	/**
	 * 关闭所有资源
	 */
	private void closeAll(ResultSet resultSet,PreparedStatement preparedStatement,CallableStatement callableStatement,Connection connection) {
		// 关闭结果集对象
		if (resultSet != null) {
			try {
				resultSet.close();
			} catch (SQLException e) {
				LOGGER.error("closeAll exception ", e);
			}
		}

		// 关闭PreparedStatement对象
		if (preparedStatement != null) {
			try {
				preparedStatement.close();
			} catch (SQLException e) {
				LOGGER.error("closeAll exception ", e);
			}
		}

		// 关闭CallableStatement 对象
		if (callableStatement != null) {
			try {
				callableStatement.close();
			} catch (SQLException e) {
				LOGGER.error("closeAll exception ", e);
			}
		}

		// 关闭Connection 对象
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				LOGGER.error("closeAll exception ", e);
			}
		}
	}
	
	public static void main(String[] args) throws SQLException{
		
		Properties prop = new Properties();
		prop.setProperty("driver", "oracle.jdbc.driver.OracleDriver");
		prop.setProperty("url", "jdbc:oracle:thin:@10.60.60.30:1521:rptods");
		prop.setProperty("username", "rptods");
		prop.setProperty("password", "ie!QG9dv7i");
		
		ConnectionDB conn = new ConnectionDB(prop);
		
//		Timestamp ts = new Timestamp(Long.parseLong("1458835201973"));
//		
//		Timestamp chuxuguan = new Timestamp(Calendar.getInstance().getTimeInMillis());
//		
//		Object[] params = new Object[]{"0929938433xxxcdfgd","xxxxxdsf",chuxuguan,null};
//		
//		List list =  new ArrayList();
//		list.add(params);
//		
//		conn.executeUpdate("insert into raw_user_login_log_test (deviceid,custno,chuxuguan_firsttime,zhangji_firsttime) values (?,?,?,?)", 
//				
//				list);
		
		Object[] params = new Object[]{"0929938433xxxcdfgd","xxxxxdsf"};
		
		List<Map<String,Object>> list = conn.executeQuery("select * from raw_user_login_log_test where deviceid = ? and custno = ?",
				params);
		
//		Connection con = conn.getConnection();
//		
//		PreparedStatement preparedStatement = con.prepareStatement("select * from raw_user_login_log_test where deviceid = ? and custno = ?");
//		
//		if (params != null) {
//			for (int i = 0; i < params.length; i++) {
//				preparedStatement.setObject(i + 1, params[i]);
//			}
//		}
//		// 执行
//		
//		
//		ResultSet rs = preparedStatement.executeQuery();
//		
//		if(rs.next()){
//			
//			Timestamp tts = rs.getTimestamp("CHUXUGUAN_FIRSTTIME");
//			System.out.println(tts);
//		}
//		
		Map<String,Object> rec = list.get(0);
		
		System.out.println(rec);
		
		Object o = rec.get("CHUXUGUAN_FIRSTTIME");
		
		
		System.out.println(o);
		
		
		
		
	}

}
