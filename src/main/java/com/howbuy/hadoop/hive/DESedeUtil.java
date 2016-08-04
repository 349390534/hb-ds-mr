/**
 * @Title: DESedeUtil.java
 * @Package com.howbuy.java.test
 * @Description: 
 * @author Polo Yuan lizhuan.yuan@howbuy.com
 * @date 2013-7-9 下午02:53:53
 * @version V1.0
 */
package com.howbuy.hadoop.hive;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESedeKeySpec;

import org.apache.commons.codec.digest.DigestUtils;

/**
 * @ClassName: DESedeUtil
 * @Description: 三重DES操作工具类
 * @author Polo Yuan lizhuan.yuan@howbuy.com
 * @date 2013-7-9 下午02:53:53
 * 
 */
public class DESedeUtil {
	/**
	 * 算法名称-三重DES
	 */
	public static final String KEY_ALGORITHM_DESEDE = "DESede";
	
	/**
	 * 算法名称-MD5
	 */
	public static final String KEY_ALGORITHM_MD5 = "MD5";
	
	/**
	 * 算法名称/加密模式/填充方式
	 */
	public static final String CIPHER_ALGORITHM = "DESede/ECB/PKCS5Padding";
	
	/**
	 * 生成加密KEY
	 * @return
	 * @throws NoSuchAlgorithmException 
	 * byte[]
	 */
	public static byte[] genkey() throws NoSuchAlgorithmException {
		KeyGenerator keyGenerator = KeyGenerator.getInstance(KEY_ALGORITHM_DESEDE);
		keyGenerator.init(112);
		SecretKey secretKey = keyGenerator.generateKey();
		return secretKey.getEncoded();
	}
	
	/**
	 * 获取加密KEY
	 * @param key
	 * @return
	 * @throws InvalidKeyException
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeySpecException 
	 * Key
	 */
	public static Key getKey(byte[] key) throws InvalidKeyException, NoSuchAlgorithmException, InvalidKeySpecException {
		DESedeKeySpec des = new DESedeKeySpec(key);
		SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(KEY_ALGORITHM_DESEDE);
		SecretKey secretKey = keyFactory.generateSecret(des);
		return secretKey;
	}
	
	/**
	 * 加密
	 * @param data
	 * @param key
	 * @return
	 * @throws InvalidKeyException
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeySpecException
	 * @throws NoSuchPaddingException
	 * @throws IllegalBlockSizeException
	 * @throws BadPaddingException 
	 * byte[]
	 */
	public static String encrypt(byte[] data, Key key) throws InvalidKeyException, 
			NoSuchAlgorithmException, InvalidKeySpecException, NoSuchPaddingException, IllegalBlockSizeException, BadPaddingException {
		Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
		cipher.init(Cipher.ENCRYPT_MODE, key);
		return Base64.encode(cipher.doFinal(data));
	}
	
	/**
	 * 解密
	 * @param data
	 * @param key
	 * @return
	 * @throws InvalidKeyException
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeySpecException
	 * @throws NoSuchPaddingException
	 * @throws IllegalBlockSizeException
	 * @throws BadPaddingException 
	 * byte[]
	 */
	public static byte[] decrypt(String encStr, Key key) throws InvalidKeyException, NoSuchAlgorithmException, 
			InvalidKeySpecException, NoSuchPaddingException, IllegalBlockSizeException, BadPaddingException {
		Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
		cipher.init(Cipher.DECRYPT_MODE, key);
		return cipher.doFinal(Base64.decode(encStr));
	}
	
	/**
	 * 签名
	 * @param data 待签名明文字节数组
	 * @return String 签名后的字符串
	 * @throws NoSuchAlgorithmException 
	 */
	public static String sign(byte[] data) throws NoSuchAlgorithmException {
		String encryptStr = DigestUtils.md5Hex(data);
		return Base64.encode(encryptStr.getBytes());
	}
	
	/**
	 * 签名验证
	 * @param data 解密后的明文字节数组
	 * @param signStr 签名后的字符串
	 * @return boolean 是否验证通过，true：是，false：否
	 */
	public static boolean veriSign(byte[] data, String signStr) throws NoSuchAlgorithmException {
		String encryptStr = DigestUtils.md5Hex(data);
		String plainTextSignStr = Base64.encode(encryptStr.getBytes());
		if(signStr.equals(plainTextSignStr)) {
			return true;
		}
		return false;
	}
	
	/**
	 * 测试入口
	 * @param args
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeyException
	 * @throws InvalidKeySpecException
	 * @throws NoSuchPaddingException
	 * @throws IllegalBlockSizeException
	 * @throws BadPaddingException
	 * @throws UnsupportedEncodingException 
	 * void
	 */
	public static void main(String[] args) throws NoSuchAlgorithmException, InvalidKeyException, 
			InvalidKeySpecException, NoSuchPaddingException, IllegalBlockSizeException, BadPaddingException, UnsupportedEncodingException {
		byte[] keyBytes = genkey();
		
		System.out.println("key值：" + new String(keyBytes));
		
		Key key = getKey("2pYHSwDKzBH;:N1_x2U&XA\"2%*%M!3".getBytes());
		String plaitext = "{\"idNo\":\"1988\",\"password\":\"thisispassword\"}";
		// 加密
		String encStr = encrypt(plaitext.getBytes("utf-8"), key);
		
		System.out.println("加密后：" + encStr);
		
		// 解密
		String decStr = new String(decrypt("b0bUJr96CzE6lCcC1pCNr1kcTTfF0miDbkXeaTJiLboeCekkF5SWBST3eV7sohnt", key), "utf-8");
		
		String str = "123456";
		sign(str.getBytes());
		
		System.out.println("解密后：" + decStr);
		
		String plaitext2 = "{\"idNo\":\"1988\",\"password\":\"thisispassword\"}";
		System.out.println("签名后 = " + sign(plaitext2.getBytes()));
		
	}
}
