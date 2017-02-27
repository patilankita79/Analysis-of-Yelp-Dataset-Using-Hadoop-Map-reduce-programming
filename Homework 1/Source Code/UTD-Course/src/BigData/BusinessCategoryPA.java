package BigData;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BusinessCategoryPA {
	
	/*
	 * Mapper Class
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, NullWritable>{
		private Text businessCategory = new Text(); 	//Type of Output key
		
		/* 
		 * Map function that emits a business category as a key and null value as a value
		 */
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
			String[] business = value.toString().split("::");
			if(business[1].contains("Palo Alto")){
				String businessCategoryList = business[2];
				
				businessCategoryList = businessCategoryList.replace("(", "");
				businessCategoryList = businessCategoryList.replace(")", "");
				businessCategoryList = businessCategoryList.replace("List", "");
				businessCategoryList = businessCategoryList.replace(" ", "");
				String[] businessCategoryList1 = businessCategoryList.toString().split(",");
				
				for(String item:businessCategoryList1){
					businessCategory.set(item);
					context.write(businessCategory, NullWritable.get());
				}
				
			}
		}
	}
	
	/*
	 * Reducer Class
	 */
	public static class Reduce extends Reducer<Text, NullWritable, Text, NullWritable>{
		//private IntWritable outcome = new IntWritable();
		
		/*
		 * Reduce function
		 */
		public void reduce(Text key, Iterable<NullWritable> value, Context context) throws IOException, InterruptedException{
			
			context.write(key, NullWritable.get());
		}
	}
	
	/* 
	 * Driver program
	 */
	public static void main(String[] args) throws Exception {
		
		/*
		 * Configuration of a job
		 */
		Configuration conf = new Configuration();
		
		/*
		 * Getting all the arguments
		 */
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	
		if (otherArgs.length != 2) {
			System.err.println("Usage: BusinessCategoryPA <in> <out>");
			System.exit(2);
		}
		
		/*
		 * Create a job with name "BusinessCategoryPA"
		 */
		Job job = new Job(conf, "BusinessCategoryPA");
		job.setJarByClass(BusinessCategoryPA.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		/*
		 *  set output key type
		 */
		job.setOutputKeyClass(Text.class);
		
		/*
		 * set output value type
		 */
		job.setOutputValueClass(NullWritable.class);
		
		/*
		 * set the HDFS path of the input data
		 */
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		
		/*
		 * set the HDFS path for the output
		 */
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		/*
		 * Wait till job completion
		 */
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
	}
}



