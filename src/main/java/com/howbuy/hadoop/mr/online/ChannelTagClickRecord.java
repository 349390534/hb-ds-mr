package com.howbuy.hadoop.mr.online;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class ChannelTagClickRecord implements DBWritable,Writable {
	
	private Date dt;
	
	private String tag;
	
	private String proid;
	
	private String pageId;
	
	private long clickNum;
	
	private Timestamp createtime;

	@Override
	public void readFields(ResultSet result) throws SQLException {
		this.dt = result.getDate(1);
		this.tag = result.getString(2);
		this.proid = result.getString(3);
		this.pageId = result.getString(4);
		this.clickNum = result.getLong(5);
		this.createtime = result.getTimestamp(6);
	}
	
	public ChannelTagClickRecord(){}

	public ChannelTagClickRecord(Date dt,String tag,String proid,String pageId, long clickNum, Timestamp createtime) {
		this.dt = dt;
		this.tag = tag;
		this.proid = proid;
		this.pageId = pageId;
		this.clickNum = clickNum;
		this.createtime = createtime;
	}

	@Override
	public void write(PreparedStatement stmt) throws SQLException {
		
		stmt.setDate(1, getDt());
		stmt.setString(2, getTag());
		stmt.setString(3, getProid());
		stmt.setString(4, getPageId());
		stmt.setLong(5, getClickNum());
		stmt.setTimestamp(6, getCreatetime());
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.dt = new Date(in.readLong());
		this.tag = Text.readString(in);
		this.proid = Text.readString(in);
		this.pageId = Text.readString(in);
		this.clickNum = in.readLong();
		this.createtime = new Timestamp(in.readLong());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeLong(getDt().getTime());
		Text.writeString(out, getTag());
		Text.writeString(out, getProid());
		Text.writeString(out, getPageId());
		out.writeLong(getClickNum());
		out.writeLong(getCreatetime().getTime());
	}

	public Date getDt() {
		return dt;
	}

	public void setDt(Date dt) {
		this.dt = dt;
	}

	public String getProid() {
		return proid;
	}

	public void setProid(String proid) {
		this.proid = proid;
	}

	public String getPageId() {
		return pageId;
	}

	public void setPageId(String pageId) {
		this.pageId = pageId;
	}

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public long getClickNum() {
		return clickNum;
	}

	public void setClickNum(long clickNum) {
		this.clickNum = clickNum;
	}

	public Timestamp getCreatetime() {
		return createtime;
	}

	public void setCreatetime(Timestamp createtime) {
		this.createtime = createtime;
	}

	 
	

	 

}
