package com.howbuy.hadoop.mr.online;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * 
 * @author qiankun.li 广告点击
 */
public class ChannelTagClickKey implements
		WritableComparable<ChannelTagClickKey> {

	private String tag;

	private String proid;

	private String pageId;

	@Override
	public int compareTo(ChannelTagClickKey o) {
		int compareVal = this.tag.compareTo(o.getTag());
		if(compareVal == 0){
			compareVal = this.proid.compareTo(o.getProid());
			if (compareVal == 0) {
				compareVal = this.pageId.compareTo(o.getPageId());
			}
		}
		return compareVal;
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		tag = Text.readString(in);
		proid = Text.readString(in);
		pageId = Text.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {

		Text.writeString(out, tag);
		Text.writeString(out, proid);
		Text.writeString(out, pageId);
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

}
