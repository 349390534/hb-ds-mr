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

public class ChannelPageRecordNews implements DBWritable, Writable {

	private Date dt;

	private int proid;

	private String pageId;

	private long zid;

	private String newstype;

	private String subtype;

	private String authorId;

	private long pv;

	private long uv;

	private Timestamp createtime;

	@Override
	public void readFields(ResultSet result) throws SQLException {
		this.dt = result.getDate(1);
		this.proid = result.getInt(2);
		this.pageId = result.getString(3);
		this.zid = result.getLong(4);
		this.newstype = result.getString(5);
		this.subtype = result.getString(6);
		this.authorId = result.getString(7);
		this.pv = result.getLong(8);
		this.uv = result.getLong(9);
		this.createtime = result.getTimestamp(10);
	}

	public ChannelPageRecordNews() {
	}

	public ChannelPageRecordNews(Date dt, int proid, String pageId, long zid,
			String newstype, String subtype, String authorId, long pv, long uv,
			Timestamp createtime) {
		this.dt = dt;
		this.proid = proid;
		this.pageId = pageId;
		this.zid = zid;
		this.newstype = newstype;
		this.subtype = subtype;
		this.authorId = authorId;
		this.pv = pv;
		this.uv = uv;
		this.createtime = createtime;
	}

	@Override
	public void write(PreparedStatement stmt) throws SQLException {

		stmt.setDate(1, getDt());
		stmt.setInt(2, getProid());
		stmt.setString(3, getPageId());
		stmt.setLong(4, getZid());
		stmt.setString(5, getNewstype());
		stmt.setString(6, getSubtype());
		stmt.setString(7, getAuthorId());
		stmt.setLong(8, getPv());
		stmt.setLong(9, getUv());
		stmt.setTimestamp(10, getCreatetime());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.dt = new Date(in.readLong());
		this.proid = in.readInt();
		this.pageId = Text.readString(in);
		this.zid = in.readLong();
		this.newstype = Text.readString(in);
		this.subtype =  Text.readString(in);
		this.authorId =  Text.readString(in);
		this.pv = in.readLong();
		this.uv = in.readLong();
		this.createtime = new Timestamp(in.readLong());
	}

	@Override
	public void write(DataOutput out) throws IOException {

		out.writeLong(getDt().getTime());
		out.writeInt(getProid());
		Text.writeString(out,getPageId());
		out.writeLong(getZid());
		Text.writeString(out,getNewstype());
		Text.writeString(out,getSubtype());
		Text.writeString(out,getAuthorId());
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

	public String getPageId() {
		return pageId;
	}

	public void setPageId(String pageId) {
		this.pageId = pageId;
	}

	public long getZid() {
		return zid;
	}

	public void setZid(long zid) {
		this.zid = zid;
	}

	public String getNewstype() {
		return newstype;
	}

	public void setNewstype(String newstype) {
		this.newstype = newstype;
	}

	public String getSubtype() {
		return subtype;
	}

	public void setSubtype(String subtype) {
		this.subtype = subtype;
	}

	public String getAuthorId() {
		return authorId;
	}

	public void setAuthorId(String authorId) {
		this.authorId = authorId;
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
