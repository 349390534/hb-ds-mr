package com.howbuy.hadoop.mr.online;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class ChannelPageRecordSimu implements DBWritable,Writable {
	
	private Date dt;
	
	private int proid;
	
	private long pageId;
	
	private int pageType;
	
	private long pv;
	
	private long uv;
	
	private Timestamp createtime;

	@Override
	public void readFields(ResultSet result) throws SQLException {
		this.dt = result.getDate(1);
		this.proid = result.getInt(2);
		this.pageId = result.getLong(3);
		this.pageType = result.getInt(4);
		this.pv = result.getLong(4);
		this.uv = result.getLong(5);
		this.createtime = result.getTimestamp(6);
	}
	
	public ChannelPageRecordSimu(){}

	public ChannelPageRecordSimu(Date dt,int proid,long pageId,int pageType, long pv, long uv, Timestamp createtime) {
		this.dt = dt;
		this.proid = proid;
		this.pageId = pageId;
		this.pageType = pageType;
		this.pv = pv;
		this.uv = uv;
		this.createtime = createtime;
	}

	@Override
	public void write(PreparedStatement stmt) throws SQLException {
		stmt.setDate(1, getDt());
		stmt.setInt(2, getProid());
		stmt.setLong(3, getPageId());
		stmt.setLong(4, getPageType());
		stmt.setLong(5, getPv());
		stmt.setLong(6, getUv());
		stmt.setTimestamp(7, getCreatetime());
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.dt = new Date(in.readLong());
		this.proid = in.readInt();
		this.pageId = in.readLong();
		this.pageType = in.readInt();
		this.pv = in.readLong();
		this.uv = in.readLong();
		this.createtime = new Timestamp(in.readLong());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeLong(getDt().getTime());
		out.writeInt(getProid());
		out.writeLong(getPageId());
		out.writeInt(getPageType());
		out.writeLong(getPv());
		out.writeLong(getUv());
		out.writeLong(getCreatetime().getTime());
	}

	public Date getDt() {
		return dt;
	}

	public void setDt(Date dt) {
		this.dt = dt;
	}

	public int getProid() {
		return proid;
	}

	public void setProid(int proid) {
		this.proid = proid;
	}

	public long getPageId() {
		return pageId;
	}

	public void setPageId(long pageId) {
		this.pageId = pageId;
	}

	public int getPageType() {
		return pageType;
	}

	public void setPageType(int pageType) {
		this.pageType = pageType;
	}

	public long getPv() {
		return pv;
	}

	public void setPv(long pv) {
		this.pv = pv;
	}

	public long getUv() {
		return uv;
	}

	public void setUv(long uv) {
		this.uv = uv;
	}

	public Timestamp getCreatetime() {
		return createtime;
	}

	public void setCreatetime(Timestamp createtime) {
		this.createtime = createtime;
	}
	
	
	

	 

}
