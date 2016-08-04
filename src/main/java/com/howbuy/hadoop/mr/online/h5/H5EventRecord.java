package com.howbuy.hadoop.mr.online.h5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.apache.hadoop.io.Text;

/*
 * H5事件
 */
public class H5EventRecord extends BaseH5Record {
	
	
	//开户人数
	private long openaccts;
	
	public H5EventRecord(){}

	public H5EventRecord(Date dt,String channel, long openaccts,
										String type,String parent,String level,Timestamp createtime) {
		super();
		this.dt = dt;
		this.channel = channel;
		this.openaccts = openaccts;
		this.type = type;
		this.parent = parent;
		this.level = level;
		this.createtime = createtime;
	}
	

	@Override
	public void readFields(ResultSet result) throws SQLException {
		
		this.dt = result.getDate(1);
		this.channel = result.getString(2);
		this.openaccts = result.getLong(3);
		this.type = result.getString(4);
		this.parent = result.getString(5);
		this.level = result.getString(6);
		this.createtime = result.getTimestamp(7);
	}
	
	

	@Override
	public void write(PreparedStatement stmt) throws SQLException {
		
		stmt.setDate(1, getDt());
		stmt.setString(2, getChannel());
		stmt.setLong(3, getOpenaccts());
		stmt.setString(4, getType());
		stmt.setString(5, getParent());
		stmt.setString(6, getLevel());
		stmt.setTimestamp(7, getCreatetime());
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		
		this.dt = new Date(in.readLong());
		this.channel = Text.readString(in);
		this.openaccts = in.readLong();
		this.type = Text.readString(in);
		this.parent = Text.readString(in);
		this.level = Text.readString(in);
		this.createtime = new Timestamp(in.readLong());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeLong(getDt().getTime());
		Text.writeString(out, getChannel());
		out.writeLong(getOpenaccts());
		Text.writeString(out, getType());
		Text.writeString(out, getParent());
		Text.writeString(out, getLevel());
		out.writeLong(getCreatetime().getTime());
	}
	


	public long getOpenaccts() {
		return openaccts;
	}

	public void setOpenaccts(long openaccts) {
		this.openaccts = openaccts;
	}


	@Override
	public String toString() {
		return "H5EventRecord [dt=" + dt + ", channel=" + channel
				+ ", openaccts=" + openaccts + ", type=" + type + ", parent="
				+ parent + ", level=" + level + ", createtime=" + createtime
				+ "]";
	}

}
