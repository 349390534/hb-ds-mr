package com.howbuy.hadoop.mr.online;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSWrite {

	
	
	public static void  doProcess(FileSystem hdfs,String content,String path) throws IOException{
		
		System.out.println(Thread.currentThread().getName() + "enter");
		
		
//		 FileSystem hdfs = FileSystem.get(URI.create(uri),conf); 
		
//		FileSystem hdfs = FileSystem.get(conf);
		 
		Path hpath = new Path(path);
		
		FSDataOutputStream os = null;
	     
	     if(hdfs.exists(hpath)){
	    	 System.out.println("path already exist");
	    	 os = hdfs.append(hpath);
	     }else{
	    	 os = hdfs.create(hpath);
	    	 System.out.println("path create");
	     }
	     
	     os.write(content.getBytes("UTF-8"));
	        
         os.close();
        
//         hdfs.close();
         
         System.out.println(Thread.currentThread().getName() + "finish");

	}

	private static String delemiter= "\u0001";
	
	private static String lingSep = "\n";
	
	
    public static  void main(String args[])throws Exception{  
    	
    	Configuration conf = new Configuration(); 
    	
//    	conf.set("hadoop.job.ugi", "hadoop-hdfs1,hadoop-user"); 
    	

//    	StringBuilder sb = new StringBuilder();
//    	
//    	sb.append("ext").append(delemiter).append("reso").append(delemiter).append("pageid").append(lingSep);
//    	sb.append("uid").append(delemiter).append("custno").append(delemiter).append("refid").append(lingSep);
//        
//    	doProcess(conf,sb.toString(),"/demo/pv");
//    	
//    	System.out.println("finish");
    	
//    	FileSystem hdfs = FileSystem.get(URI.create("/demo/pv1"),conf); 
    	
    	FileSystem hdfs = FileSystem.get(conf);
    	
    	Thread t1 = new Thread(new Job(hdfs,"/hive/data/pv4",5),"j1");
    	Thread t2 = new Thread(new Job(hdfs,"/hive/data/pv5",5),"j2");
    	Thread t3 = new Thread(new Job(hdfs,"/hive/data/pv6",5),"j3");
    	
    	t1.start();
    	
    	t2.start();
    	
    	t3.start();
    	
    	t1.join();
    	hdfs.close();
    	
    	t2.join();
    	t3.join();
    	
    	
    	
    	
    	
    	
    	System.out.println("job finish");
    	
    }
    
    static class Job implements Runnable {
    	
    	private FileSystem hdfs;
    	
    	private String path;
    	
    	private int count;
    	
    	public Job (FileSystem hdfs,String path,int count){
    		this.hdfs = hdfs;
    		this.path = path;
    		this.count = count;
    	}
    	
		public void run() {
			
			String tname = Thread.currentThread().getName();
			
			StringBuilder sb = new StringBuilder();
	    	
			for(int i=0;i<count;i++){
				
				String ext = tname + "_" + "ext" + "_" + i;
				
				String reso = tname + "_" + "reso" + "_" + i;
				
				String pageid = tname + "_" + "pageid" + "_" + i;
				
				sb.append(ext).append(delemiter).append(reso).append(delemiter).append(pageid).append(lingSep);
			}
	    	
	        
	    	try {
				doProcess(hdfs,sb.toString(),path);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
    
    
    
    
    
}
