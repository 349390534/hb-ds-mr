package com.howbuy.hadoop.mr.online.h5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceFileTester {
	
	
	
	@SuppressWarnings("deprecation")
	public static void read(String sequeceFilePath,Configuration conf){
		
		List<Writable> result = null;
		
		FileSystem fs=null;  
	    SequenceFile.Reader reader=null; 
	    
	    Path path=null;
	    
	    Writable key=null;
	    
//	    H5EventRecord key=null;
	    
	    Writable value = NullWritable.get();
	    
	    try {  
	        fs=FileSystem.get(conf); 
	        result=new ArrayList<Writable>();  
	        path=new Path(sequeceFilePath);  
	        reader=new SequenceFile.Reader(fs, path, conf);   
	        key=(Writable)ReflectionUtils.newInstance(reader.getKeyClass(), conf); // 获得Key，也就是之前写入的userId  
	        while(reader.next(key, value)){
	        	
	          System.out.println(key); 
	          
	          key = new H5MergedRecord();
	          
	        }  
	          
	      } catch (IOException e) {  
	        e.printStackTrace();  
	      }catch (Exception e){  
	        e.printStackTrace();  
	      }finally{  
	          IOUtils.closeStream(reader);  
	      }  
	      System.out.println(result); 
	}

	public static void main(String[] args) {
		
		Configuration conf = new Configuration();
		
		conf.set("fs.defaultFS", "hdfs://192.168.220.154:9000"); 
		
		read("/tmp/mapred/pvoutput/part-r-00000",conf);
		
		read("/tmp/mapred/eventoutput/part-r-00000",conf);
		

	}

}
