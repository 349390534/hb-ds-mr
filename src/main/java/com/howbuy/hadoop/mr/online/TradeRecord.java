package com.howbuy.hadoop.mr.online;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.sqoop.lib.BigDecimalSerializer;

public class TradeRecord implements DBWritable,Writable {
	
	private Date dt;
	
	private String channel;
	
	//交易人数
	private long tradePer;
	
	//交易笔数
	private long tradeNum;
	
	//交易金额
	private BigDecimal tradeAmt;
	
	//绑卡人数
	private long bindcards;
	
	//开户人数
	private long openaccts;
	
	//渠道类型 1:直接访问，2：搜索引擎，3：推广，4：其他渠道
	private int type;
	
	//所属渠道
	private String parent;
	
	private String level;
	
	private Timestamp createtime;
	
	
	
	public TradeRecord(){}

	public TradeRecord(Date dt,String channel, long tradeper,long tradenum,BigDecimal tradeamt,
										long bindcards,long openaccts,
										int type,String parent,String level,Timestamp createtime) {
		super();
		this.dt = dt;
		this.channel = channel;
		this.tradePer = tradeper;
		this.tradeNum = tradenum;
		this.tradeAmt = tradeamt;
		this.bindcards = bindcards;
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
		this.tradePer = result.getLong(3);
		this.tradeNum = result.getLong(4);
		this.tradeAmt = result.getBigDecimal(5);
		this.bindcards = result.getLong(6);
		this.openaccts = result.getLong(7);
		this.type = result.getInt(8);
		this.parent = result.getString(9);
		this.level = result.getString(10);
		this.createtime = result.getTimestamp(11);
	}
	
	

	@Override
	public void write(PreparedStatement stmt) throws SQLException {
		
		stmt.setDate(1, getDt());
		stmt.setString(2, getChannel());
		stmt.setLong(3, getTradePer());
		stmt.setLong(4, getTradeNum());
		stmt.setBigDecimal(5, getTradeAmt());
		stmt.setLong(6, getBindcards());
		stmt.setLong(7, getOpenaccts());
		stmt.setInt(8, getType());
		stmt.setString(9, getParent());
		stmt.setString(10, getLevel());
		stmt.setTimestamp(11, getCreatetime());
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		
		this.dt = new Date(in.readLong());
		this.channel = Text.readString(in);
		this.tradePer = in.readLong();
		this.tradeNum = in.readLong();
		this.tradeAmt = BigDecimalSerializer.readFields(in);
		this.bindcards = in.readLong();
		this.openaccts = in.readLong();
		this.type = in.readInt();
		this.parent = Text.readString(in);
		this.level = Text.readString(in);
		this.createtime = new Timestamp(in.readLong());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeLong(getDt().getTime());
		Text.writeString(out, getChannel());
		out.writeLong(getTradePer());
		out.writeLong(getTradeNum());
		BigDecimalSerializer.write(getTradeAmt(), out);
		out.writeLong(getBindcards());
		out.writeLong(getOpenaccts());
		out.writeShort(getType());
		Text.writeString(out, getParent());
		Text.writeString(out, getLevel());
		out.writeLong(getCreatetime().getTime());
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

	public long getTradePer() {
		return tradePer;
	}

	public void setTradePer(long tradePer) {
		this.tradePer = tradePer;
	}

	public long getTradeNum() {
		return tradeNum;
	}

	public void setTradeNum(long tradeNum) {
		this.tradeNum = tradeNum;
	}

	public BigDecimal getTradeAmt() {
		return tradeAmt;
	}

	public void setTradeAmt(BigDecimal tradeAmt) {
		this.tradeAmt = tradeAmt;
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

	public long getBindcards() {
		return bindcards;
	}

	public void setBindcards(long bindcards) {
		this.bindcards = bindcards;
	}

	public long getOpenaccts() {
		return openaccts;
	}

	public void setOpenaccts(long openaccts) {
		this.openaccts = openaccts;
	}

	public Timestamp getCreatetime() {
		return createtime;
	}

	public void setCreatetime(Timestamp createtime) {
		this.createtime = createtime;
	}
	
}
