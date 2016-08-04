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

/**
 * H5页面PV计算
 * @author yichao.song
 *
 */
public class H5PVRecord extends BaseH5Record {
	
	
	private long pv;
	
	private long uv;
	
	/*
	 * 活动首页UV
	 */
	private long huodongindex;
	
	/**
	 * 开户首页UV
	 */
	private long openacctindex;
	
	/**
	 * 身份验证首页UV
	 */
	private long authindex;
	
	/**
	 * 开户结果UV
	 */
	private long openresultindex;
	
	/**
	 * 进入次数
	 */
	private long enter;
	
	@Override
	public void readFields(ResultSet result) throws SQLException {
		
		this.dt = result.getDate(1);
		this.channel = result.getString(2);
		this.pv = result.getLong(3);
		this.uv = result.getLong(4);
		this.enter = result.getLong(5);
		this.huodongindex = result.getLong(6);
		this.openacctindex = result.getLong(7);
		this.authindex = result.getLong(8);
		this.openresultindex = result.getLong(9);
		this.type = result.getString(10);
		this.parent = result.getString(11);
		this.level = result.getString(12);
		this.createtime = result.getTimestamp(13);
	}
	
	public H5PVRecord(){}

	public H5PVRecord(Date dt,String channel, long pv, long uv,long enter, long huodongindex,
			long openacctindex, long authindex, long openresultindex,String type,String parent,String level,Timestamp createtime) {
		super();
		this.dt = dt;
		this.channel = channel;
		this.pv = pv;
		this.uv = uv;
		this.enter = enter;
		this.huodongindex = huodongindex;
		this.openacctindex = openacctindex;
		this.authindex = authindex;
		this.openresultindex = openresultindex;
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
		stmt.setLong(5, getEnter());
		stmt.setLong(6, getHuodongindex());
		stmt.setLong(7, getOpenacctindex());
		stmt.setLong(8, getAuthindex());
		stmt.setLong(9, getOpenresultindex());
		stmt.setString(10, getType());
		stmt.setString(11, getParent());
		stmt.setString(12, getLevel());
		stmt.setTimestamp(13, getCreatetime());
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		
		this.dt = new Date(in.readLong());
		this.channel = Text.readString(in);
		this.pv = in.readLong();
		this.uv = in.readLong();
		this.enter = in.readLong();
		this.huodongindex = in.readLong();
		this.openacctindex = in.readLong();
		this.authindex = in.readLong();
		this.openresultindex = in.readLong();
		this.type = Text.readString(in);
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
		out.writeLong(getEnter());
		out.writeLong(getHuodongindex());
		out.writeLong(getOpenacctindex());
		out.writeLong(getAuthindex());
		out.writeLong(getOpenresultindex());
		Text.writeString(out, getType());
		Text.writeString(out, getParent());
		Text.writeString(out, getLevel());
		out.writeLong(getCreatetime().getTime());
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

	public long getHuodongindex() {
		return huodongindex;
	}

	public void setHuodongindex(long huodongindex) {
		this.huodongindex = huodongindex;
	}

	public long getOpenacctindex() {
		return openacctindex;
	}

	public void setOpenacctindex(long openacctindex) {
		this.openacctindex = openacctindex;
	}

	public long getAuthindex() {
		return authindex;
	}

	public void setAuthindex(long authindex) {
		this.authindex = authindex;
	}

	public long getOpenresultindex() {
		return openresultindex;
	}

	public void setOpenresultindex(long openresultindex) {
		this.openresultindex = openresultindex;
	}

	public long getEnter() {
		return enter;
	}

	public void setEnter(long enter) {
		this.enter = enter;
	}

	@Override
	public String toString() {
		return "H5PVRecord [dt=" + dt + ", channel=" + channel + ", pv=" + pv
				+ ", uv=" + uv + ", huodongindex=" + huodongindex
				+ ", openacctindex=" + openacctindex + ", authindex="
				+ authindex + ", openresultindex=" + openresultindex
				+ ", enter=" + enter + ", type=" + type + ", parent=" + parent
				+ ", level=" + level + ", createtime=" + createtime + "]";
	}
	
}
