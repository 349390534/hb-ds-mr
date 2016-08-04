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

public class ChannelRecord implements DBWritable,Writable {
	
	private Date dt;
	
	private String channel;
	
	private long pv;
	
	private long uv;
	
	private long validuv;
	
	private long enter;
	
	private long gmuv;
	
	private long simuuv;
	//渠道类型 1:直接访问，2：搜索引擎，3：推广，4：其他渠道
	private int type;
	
	//所属渠道
	private String parent;
	
	private String level;
	
	private Timestamp createtime;

	@Override
	public void readFields(ResultSet result) throws SQLException {
		
		this.dt = result.getDate(1);
		this.channel = result.getString(2);
		this.pv = result.getLong(3);
		this.uv = result.getLong(4);
		this.validuv = result.getLong(5);
		this.enter = result.getLong(6);
		this.gmuv = result.getLong(7);
		this.simuuv = result.getLong(8);
		this.type = result.getInt(9);
		this.parent = result.getString(10);
		this.level = result.getString(11);
		this.createtime = result.getTimestamp(12);
	}
	
	public ChannelRecord(){}

	public ChannelRecord(Date dt,String channel, long pv, long uv, long validuv,
			long enter, long gmuv, long simuuv,int type,String parent,String level,Timestamp createtime) {
		super();
		this.dt = dt;
		this.channel = channel;
		this.pv = pv;
		this.uv = uv;
		this.validuv = validuv;
		this.enter = enter;
		this.gmuv = gmuv;
		this.simuuv = simuuv;
		this.type = type;
		this.parent = parent;
		this.level = level;
		this.createtime = createtime;
	}

	@Override
	public void write(PreparedStatement stmt) throws SQLException {
		
		stmt.setDate(1, getDt());
		stmt.setString(2, getChannel());
		stmt.setLong(3, getPv());
		stmt.setLong(4, getUv());
		stmt.setLong(5, getValiduv());
		stmt.setLong(6, getEnter());
		stmt.setLong(7, getGmuv());
		stmt.setLong(8, getSimuuv());
		stmt.setInt(9, getType());
		stmt.setString(10, getParent());
		stmt.setString(11, getLevel());
		stmt.setTimestamp(12, getCreatetime());
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		
		this.dt = new Date(in.readLong());
		this.channel = Text.readString(in);
		this.pv = in.readLong();
		this.uv = in.readLong();
		this.validuv = in.readLong();
		this.enter = in.readLong();
		this.gmuv = in.readLong();
		this.simuuv = in.readLong();
		this.type = in.readInt();
		this.parent = Text.readString(in);
		this.level = Text.readString(in);
		this.createtime = new Timestamp(in.readLong());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeLong(getDt().getTime());
		Text.writeString(out, getChannel());
		out.writeLong(getPv());
		out.writeLong(getUv());
		out.writeLong(getValiduv());
		out.writeLong(getEnter());
		out.writeLong(getGmuv());
		out.writeLong(getSimuuv());
		out.writeShort(getType());
		Text.writeString(out, getParent());
		Text.writeString(out, getLevel());
		out.writeLong(getCreatetime().getTime());
	}
	
	
	

	public String getChannel() {
		return channel;
	}

	public void setChannel(String channel) {
		this.channel = channel;
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

	public long getValiduv() {
		return validuv;
	}

	public void setValiduv(long validuv) {
		this.validuv = validuv;
	}

	public long getEnter() {
		return enter;
	}

	public void setEnter(long enter) {
		this.enter = enter;
	}

	public long getGmuv() {
		return gmuv;
	}

	public void setGmuv(long gmuv) {
		this.gmuv = gmuv;
	}

	public long getSimuuv() {
		return simuuv;
	}

	public void setSimuuv(long simuuv) {
		this.simuuv = simuuv;
	}

	public Date getDt() {
		return dt;
	}

	public void setDt(Date dt) {
		this.dt = dt;
	}
	

	public int getType() {
		return type;
	}

	public void setType(int type) {
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
