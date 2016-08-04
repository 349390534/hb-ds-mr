package com.howbuy.hadoop.mr.online.apptag;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Timestamp;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class AppTag implements WritableComparable<AppTag> {
	
	private String proid;
	
	private String guid;
	
	private String custno;
	
	private Timestamp ts;
	/*
	 * 1:首次激活
	 * 2：后台进程切换
	 * 3：登陆
	 */
	private int type;
	
	private int flag;
	
	public void setTag(String proid, String guid, String custno, Timestamp ts,int type,int flag) {
		this.proid = proid;
		this.guid = guid;
		this.custno = custno;
		this.ts = ts;
		this.type = type;
		this.flag = flag;
	}
	
	

	public int getType() {
		return type;
	}



	public void setType(int type) {
		this.type = type;
	}



	public Timestamp getTs() {
		return ts;
	}

	public void setTs(Timestamp ts) {
		this.ts = ts;
	}

	public String getProid() {
		return proid;
	}

	public void setProid(String proid) {
		this.proid = proid;
	}

	public String getGuid() {
		return guid;
	}

	public void setGuid(String guid) {
		this.guid = guid;
	}

	public String getCustno() {
		return custno;
	}

	public void setCustno(String custno) {
		this.custno = custno;
	}
	

	public int getFlag() {
		return flag;
	}



	public void setFlag(int flag) {
		this.flag = flag;
	}



	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, getGuid());
		Text.writeString(out, getProid());
		Text.writeString(out, getCustno() == null ? "0" : getCustno());
		out.writeLong(getTs().getTime());
		out.writeInt(getType());
		out.writeInt(getFlag());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		setGuid(Text.readString(in));
		setProid(Text.readString(in));
		setCustno(Text.readString(in));
		setTs(new Timestamp(in.readLong()));
		setType(in.readInt());
		setFlag(in.readInt());
	}

	@Override
	public int compareTo(AppTag o) {
		
		int ret = getGuid().compareTo(o.getGuid());
		
		if(ret == 0)
			ret = getCustno().compareTo(o.getCustno());
		
		if(ret == 0)
			ret = getTs().compareTo(o.getTs());
		
		return ret;
		
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((custno == null) ? 0 : custno.hashCode());
		result = prime * result + ((guid == null) ? 0 : guid.hashCode());
		return result;
	}
	

}
