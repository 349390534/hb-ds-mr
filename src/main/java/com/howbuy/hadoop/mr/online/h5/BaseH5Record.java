package com.howbuy.hadoop.mr.online.h5;

import java.sql.Date;
import java.sql.Timestamp;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public abstract class BaseH5Record implements DBWritable,Writable{

	protected Date dt;
	
	protected String channel;
	
	//渠道类型 1:直接访问，2：搜索引擎，3：推广，4：其他渠道
	protected String type;
	
	//所属渠道
	protected String parent;
	
	protected String level;
	
	protected Timestamp createtime;

	public Date getDt() {
		return dt;
	}

	public void setDt(Date dt) {
		this.dt = dt;
	}

	public String getChannel() {
		return channel;
	}

	public void setChannel(String channel) {
		this.channel = channel;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getParent() {
		return parent;
	}

	public void setParent(String parent) {
		this.parent = parent;
	}

	public String getLevel() {
		return level;
	}

	public void setLevel(String level) {
		this.level = level;
	}

	public Timestamp getCreatetime() {
		return createtime;
	}

	public void setCreatetime(Timestamp createtime) {
		this.createtime = createtime;
	}
	
}
