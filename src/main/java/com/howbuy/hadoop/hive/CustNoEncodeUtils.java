package com.howbuy.hadoop.hive;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESedeKeySpec;

import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

/**
 * <pre>
 *  无线App的custNo的加密方式
 *
 *  已去掉所有的
 * </pre>
 *
 * @author ji.ma
 * @create 2015/6/2 16:55
 * @modify
 * @since JDK1.6
 */
public class CustNoEncodeUtils {

    /**
     * 客户号加密的入口方法
     * @param custNo 客户号.
     * @return  加密后的客户号.
     * @throws UnsupportedEncodingException 
     */
    public static String getEncMsg(String custNo) throws UnsupportedEncodingException {
        if (isEmpty(custNo))
            return null;
        return getDesEncodeMsg(CUSTNO_DES_KEY, custNo);
    }
    
    
    public static String getDecMsg(String enc) throws UnsupportedEncodingException, IOException {
        if (isEmpty(enc))
            return null;
        return getDesDecodeMsg(CUSTNO_DES_KEY,new String(Base64.decode(enc),"utf-8"));
    }
    

    //=================================以下为私有方法=============================================================================
    private static String base64Encode(byte[] encodeBytes) {
        return (new BASE64Encoder()).encode(encodeBytes);
        
    }
    
    public static byte[] base64Decode(String enc) throws IOException {
        return new BASE64Decoder().decodeBuffer(enc);
        
    }
    
    

    private static Key getDesSecretKey(byte[] key) throws InvalidKeyException, NoSuchAlgorithmException, InvalidKeySpecException {
        DESedeKeySpec des = new DESedeKeySpec(key);
        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DESede");
        return keyFactory.generateSecret(des);
    }

    private static String getDesEncodeMsg(String securityKey, String src) {
        if (isEmpty(securityKey) || isEmpty(src)) {
            return "";
        }

        try {
            Key key = getDesSecretKey(securityKey.getBytes());
            return encrypt(src.getBytes("UTF-8"), key);
        } catch (Exception e1) {
            e1.printStackTrace();
        }
        return "";
    }
    
    private static String getDesDecodeMsg(String securityKey, String src) {
        if (isEmpty(securityKey) || isEmpty(src)) {
            return null;
        }

        try {
            Key key = getDesSecretKey(securityKey.getBytes());
            byte[] base64decodedata = base64Decode(src);
            return new String(decrypt(base64decodedata, key),"utf-8");
        } catch (Exception e1) {
            e1.printStackTrace();
        }
        return null;
    }

    private static String encrypt(byte[] data, Key key) throws InvalidKeyException, NoSuchAlgorithmException, InvalidKeySpecException, NoSuchPaddingException, IllegalBlockSizeException, BadPaddingException {
        Cipher cipher = Cipher.getInstance("DESede/ECB/PKCS5Padding");
        cipher.init(Cipher.ENCRYPT_MODE, key);
        return base64Encode(cipher.doFinal(data));
    }
    
    private static byte[] decrypt(byte[] data, Key key) throws InvalidKeyException, NoSuchAlgorithmException, InvalidKeySpecException, NoSuchPaddingException, IllegalBlockSizeException, BadPaddingException {
        Cipher cipher = Cipher.getInstance("DESede/ECB/PKCS5Padding");
        cipher.init(Cipher.DECRYPT_MODE, key);
        return cipher.doFinal(data);
    }

    private static String CUSTNO_DES_KEY = "1234567890__howbuy__abcdefghij";
    
    
    private static boolean isEmpty(String str){
    	if(str == null || str.length() == 0)
    		return true;
    	return false;
    }


    public static void main(String[] args) throws Exception {
        //QkgrNEprdDdlZDZvMUdQQkxxeGFTUT09
        
        
//    	Key key = DESedeUtil.getKey("1234567890__howbuy__abcdefghij".getBytes());
//    	
//    	System.out.println(new String(
//    			DESedeUtil.decrypt(new String(Base64.decode("Umlaa29rNmNTN1dGTVVCOFQ4Znh6QT09")), key), "UTF-8"));

    	
//        String custNo = "1200945775";
//        
//        String encCustNo = getEncMsg(custNo);
//        
//        System.out.println("encCustNo = " + encCustNo);
    							   
        String decode = getDecMsg("0");
//        
        System.out.println("decode: " + decode);
    	
    	
//    	System.out.println(CustNoEncodeUtils.base64Decode("0"));
    	
    	
    }
}
