package com.howbuy.hadoop.mr.online;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Mywordcount {

	public static  class WordcountMapper extends  
	    Mapper<LongWritable, Text, Text, IntWritable>{  
		    private final static IntWritable one = new IntWritable(1);  
		    private Text word = new Text();  
		    public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{  
		        String line = value.toString();  
		        StringTokenizer itr = new StringTokenizer(line);  
		        while(itr.hasMoreElements()){  
		            word.set(itr.nextToken());  
		            context.write(word, one);
		        }  
		    }  
	   	}
	
	public static  class WordcountReducer extends  Reducer<Text, IntWritable, Text, IntWritable>{  
	    public void reduce(Text key, Iterable<IntWritable>values, Context context)throws IOException, InterruptedException{  
	        int sum = 0;  
	        for (IntWritable str : values){  
	            sum += str.get();  
	        }  
	        context.write(key, new IntWritable(sum));  
	    }  
	}
	
	/** 
     * 2 args, the file you want to count words from and the directory you want to save the result 
     * @param args /home/hadooper/testmp/testtext /home/hadooper/testmp/testresult 
     * @throws Exception 
     */  
    public static  void main(String args[])throws Exception{  
        //首先定义两个临时文件夹，这里可以使用随机函数+文件名，这样重名的几率就很小。  
        String dstFile = "temp_src";  
        String srcFile = "temp_dst";  
        
        Configuration conf = new Configuration();  
        
        System.out.println("fs:" + conf.get("fs.default.name"));
        // must!!!  config the fs.default.name be the same to the value in core-site.xml  
        
//        conf.set("fs.default.name","hdfs://hadoop1-node1:9000"); 
//        
//        conf.set("mapred.job.tracker","hadoop1-node1:9001"); 
        
        conf.set("mapred.jar", "E:/work_space_trade/howbuy-hadoop_20150209/target/test-classes/wordcount.jar");
        
//        conf.set("hadoop.native.lib", "false");
        
       /* //这里生成文件操作对象。  
        HDFS_File file = new HDFS_File();  
          
        
        //从本地上传文件到HDFS,可以是文件也可以是目录  
        file.PutFile(conf, args[0], dstFile);  
          
        System.out.println("up ok");  */
        
        
        
        Job job = new Job(conf, "mywordcount1");       
        job.setJarByClass(Mywordcount.class);  
          
        job.setInputFormatClass(TextInputFormat.class);  
          
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(IntWritable.class);  
          
        job.setMapperClass(WordcountMapper.class);  
        job.setReducerClass(WordcountReducer.class);  
        job.setCombinerClass(WordcountReducer.class);  
        //注意这里的输入输出都应该是在HDFS下的文件或目录       
        FileInputFormat.setInputPaths(job, new Path("/wordcount"));  
        FileOutputFormat.setOutputPath(job, new Path("/wordoutput"));
        //开始运行  
        job.waitForCompletion(true);  
        
        /*//从HDFS取回文件保存至本地  
        file.GetFile(conf, srcFile, args[1]);  
        System.out.println("down the result ok!");  
        //删除临时文件或目录  
        file.DelFile(conf, dstFile, true);  
        file.DelFile(conf, srcFile, true);  */
        
        System.out.println("delete file on hdfs ok!");  
    }  
}
