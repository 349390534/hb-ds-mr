/**
 * 
 */
package com.howbuy.hadoop.hive;

import java.io.IOException;

import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

/**
 * 
 * @ClassName: Base64
 * @Description: Base64加解密
 * @author Polo Yuan lizhuan.yuan@howbuy.com
 * @date 2013-4-22 下午03:49:21
 *
 */
public class Base64 {
	public static String encode(byte[] encodeBytes) {
		return new BASE64Encoder().encode(encodeBytes);
	}

	public static byte[] decode(String s) {
		byte[] result = null;
		if (s == null) {
			return result;
		}

		BASE64Decoder base64decoder = new BASE64Decoder();
		try {
			result = base64decoder.decodeBuffer(s);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	} 
}