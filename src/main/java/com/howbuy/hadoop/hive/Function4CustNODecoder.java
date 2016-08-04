package com.howbuy.hadoop.hive;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;



public class Function4CustNODecoder extends UDF {

	public Text evaluate(Text input) throws UnsupportedEncodingException, IOException {
		
		if(input == null)
			return null;
		String decStr = CustNoEncodeUtils.getDecMsg(input.toString());
		
		if(decStr == null )
			return null;
		
		return new Text(decStr);
	}

}
