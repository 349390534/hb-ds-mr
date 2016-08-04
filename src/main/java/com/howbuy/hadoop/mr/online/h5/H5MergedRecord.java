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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

/**
 * H5页面PV计算
 * @author yichao.song
 *
 */
public class H5MergedRecord implements DBWritable,Writable {
	
	private Date dt;
	
	private String channel;
	
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
	 * 开户人数
	 */
	private long openacctnum;
	
	/**
	 * 进入次数
	 */
	private long enter;
	
	//渠道类型 1:直接访问，2：搜索引擎，3：推广，4：其他渠道
	private String type;
	
	//所属渠道
	private String parent;
	
	private String level;
	
	private Timestamp createtime;
	
	/*
	 * 1:pv
	 * 2：event
	 */
	private String flag;

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
		this.openacctnum = result.getLong(10);
		this.type = result.getString(11);
		this.parent = result.getString(12);
		this.level = result.getString(13);
		this.createtime = result.getTimestamp(14);
//		this.flag = result.getString(15);
	}
	
	public H5MergedRecord(){}

	public H5MergedRecord(Date dt,String channel, long pv, long uv,long enter, long huodongindex,
			long openacctindex, long authindex, long openresultindex,long openacctnum,String type,String parent,String level,
			Timestamp createtime,String flag) {
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
		this.openacctnum = openacctnum;
		this.type = type;
		this.parent = parent;
		this.level = level;
		this.createtime = createtime;
		this.flag = flag;
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
		stmt.setLong(10, getOpenacctnum());
		stmt.setString(11, getType());
		stmt.setString(12, getParent());
		stmt.setString(13, getLevel());
		stmt.setTimestamp(14, getCreatetime());
//		stmt.setString(15, getFlag());
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
		this.openacctnum = in.readLong();
		this.type = Text.readString(in);
		this.parent = Text.readString(in);
		this.level = Text.readString(in);
		this.createtime = new Timestamp(in.readLong());
		this.flag = Text.readString(in);
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
		out.writeLong(getOpenacctnum());
		Text.writeString(out,getType());
		Text.writeString(out, getParent());
		Text.writeString(out, getLevel());
		out.writeLong(getCreatetime().getTime());
		Text.writeString(out, getFlag());
	}

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

	public long getEnter() {
		return enter;
	}

	public void setEnter(long enter) {
		this.enter = enter;
	}

	public long getOpenacctnum() {
		return openacctnum;
	}

	public void setOpenacctnum(long openacctnum) {
		this.openacctnum = openacctnum;
	}

	public String getFlag() {
		return flag;
	}

	public void setFlag(String flag) {
		this.flag = flag;
	}

	@Override
	public String toString() {
		return "H5MergedRecord [dt=" + dt + ", channel=" + channel + ", pv="
				+ pv + ", uv=" + uv + ", huodongindex=" + huodongindex
				+ ", openacctindex=" + openacctindex + ", authindex="
				+ authindex + ", openresultindex=" + openresultindex
				+ ", openacctnum=" + openacctnum + ", enter=" + enter
				+ ", type=" + type + ", parent=" + parent + ", level=" + level
				+ ", createtime=" + createtime + ", flag=" + flag + "]";
	}

	
	
}
