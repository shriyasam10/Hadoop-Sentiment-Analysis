/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


/**
 *
 * @author aj
 */




package combi;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import java.util.Properties;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import static org.apache.hadoop.io.SequenceFile.Reader.file;
import static org.apache.hadoop.io.SequenceFile.Writer.file;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import java.io.File;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;





public class pn1 {
    	private static pn1 sentimentParser = new pn1();
	static java.util.Map<String, Integer> dictpos;
	static java.util.Map<String, Integer> dictneg;
	public pn1() {
		dictpos = new HashMap<String, Integer>();
		dictneg = new HashMap<String, Integer>();
	}
	public boolean initpos() {
		try {
			BufferedReader bReader = new BufferedReader(new FileReader("/home/aj/positive.txt"));
			String line = null;
			while ((line = bReader.readLine()) != null) {
				dictpos.put(line, 1);
			}
			bReader.close();
			return true;
		} catch(Exception e) {
			return false;
		}
	}
	public boolean initnega() {
		try {
			BufferedReader bReader = new BufferedReader(new FileReader("/home/aj/negative.txt"));
			String line = null;
			while ((line = bReader.readLine()) != null) {
				dictneg.put(line, 1);
			}
			bReader.close();
			return true;
		} catch (Exception e) {
			
			return false;
		}
                
                
	}

  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> 
 {
      
    private final static IntWritable one = new IntWritable(1);
    //private final static IntWritable two = new IntWritable(2);
    private final static IntWritable minusone = new IntWritable(-1);
    private final static IntWritable zero = new IntWritable(0);
    private Text word = new Text();
   

public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException 
   {
        String line = value.toString();
	String[] mdata= line.split(" ");
	Text outputkey =new Text();
        
	for(int i=1;i<mdata.length;i++)
	{
	
      

	
	
                if (dictpos.containsKey(mdata[i])) {
				outputkey.set(mdata[0]);
                                context.write(outputkey, one);
			} else if (dictneg.containsKey(mdata[i])) {
                                   
                                 outputkey.set(mdata[0]);
                                 context.write(outputkey, minusone);
                   
			}
			 else {
                                   
                                 outputkey.set(mdata[0]);
                                 context.write(outputkey, zero);
                   
			}
	

	
	}
   }
 }

 public static class Reduce extends Reducer<Text, IntWritable,Text, IntWritable> 
{
    
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
	int sum = 0;
	
	for (IntWritable val :values) {
        sum += val.get();
      }

      context.write(key, new IntWritable(sum));
    }
}
 
 	
        


 


public void PNcall() throws Exception  {
	
        
        pn1 p = new pn1();
        
        Configuration conf = new Configuration();
        sentimentParser.initpos();
        sentimentParser.initnega();
        
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
        
        
        
        
        
       

	Job job = new Job(conf, "sentiment");
	job.setJarByClass(pn1.class);

	

        //FileInputFormat.addInputPath(job, new Path(args[0]));
	//FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        //p.readFile(args[1]);
        //FileInputFormat.addInputPath(job, new Path("/home/aj/input"));
        FileInputFormat.addInputPath(job, new Path("/usr/hive/warehouse/db1.db/tweets555"));
       // FileInputFormat.addInputPath(job, new Path("/user/hduser/output25/"));
	FileOutputFormat.setOutputPath(job, new Path("/user/hduser/output53/"));
        
        //FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310/user/hduser/input/"));
	//FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:54310/user/hduser/output"));
        
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);

	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);

	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
        
        
	job.waitForCompletion(true);
	
	}
}  


