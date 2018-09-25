package org.carl.hadoop.Hadoop0614.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WCDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		//job
		Job job = Job.getInstance(this.getConf(), "user-wc");
		job.setJarByClass(WCDriver.class);
		//input
		Path inputDir = new  Path(args[0]);
		FileInputFormat.setInputPaths(job, inputDir);
		//map
		job.setMapperClass(WCMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		//shuffle
		//reduce
		job.setReducerClass(WCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//output
		Path outputDir = new Path(args[1]);
		FileSystem fs = FileSystem.get(this.getConf());
		if(fs.exists(outputDir)){
			fs.delete(outputDir, true);
		}
		FileOutputFormat.setOutputPath(job, outputDir);
		//submit
		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess ? 0 : -1;
	}

	public static void main(String[] args) {
		//new hadoop configure instance 
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://bigdata.demo.com:8020");
		try {
			int status = ToolRunner.run(conf, new WCDriver(), args);
			System.exit(status);
		} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
		}
	}

}
