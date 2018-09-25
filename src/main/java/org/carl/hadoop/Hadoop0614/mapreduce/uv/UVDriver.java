package org.carl.hadoop.Hadoop0614.mapreduce.uv;

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


public class UVDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//job
		Job job = Job.getInstance(this.getConf(),"uv-ip-count");
		job.setJarByClass(UVDriver.class);
		//input
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		//map
		job.setMapperClass(UVMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//shuffle
		//reduce
		job.setReducerClass(UVReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//output
		Path outputDir = new Path(args[1]);
		FileSystem fs = FileSystem.get(this.getConf());
		if(fs.exists(outputDir)){
			fs.delete(outputDir,true);
		}
		FileOutputFormat.setOutputPath(job, outputDir);
		//submit
		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess ? 0 : -1;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Configuration config = new Configuration();
		try {
			int status = ToolRunner.run(config, new UVDriver(),args);
			System.exit(status);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
