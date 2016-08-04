/**
 * 
 */
package com.howbuy.hadoop.mr.online;

/**
 * @author qiankun.li
 * 模块id 四位uac_proid值+自定义模块值
 */
public enum ProidType {
	/**
	 * 私募
	 */
	PROID_SIMU("10011","私募"),
	/**
	 * 资讯
	 */
	PROID_ZIXUN("10012","资讯"),
	/**
	 * 网站广告
	 */
	PROID_AD("10013","网站广告"),
	;

	private String index;
	private String name;

	private ProidType(String index, String name) {
		this.index= index;
		this.name= name;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
