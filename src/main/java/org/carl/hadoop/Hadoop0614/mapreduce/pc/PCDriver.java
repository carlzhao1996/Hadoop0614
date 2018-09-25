package org.carl.hadoop.Hadoop0614.mapreduce.pc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PCDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		//job
		Job job = Job.getInstance(this.getConf(),"PC");
		//job running class
		job.setJarByClass(PCDriver.class);
		//input
		Path inputPath = new Path(args[0]);
		FileInputFormat.setInputPaths(job, inputPath);
		//map
		job.setMapperClass(PCMapper.class);
		job.setMapOutputKeyClass(PersonWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		//shuffle
		//reduce
		job.setReducerClass(PCReducer.class);
		job.setOutputKeyClass(PersonWritable.class);
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
		Configuration conf = new Configuration();
		try {
			int status = ToolRunner.run(conf, new PCDriver(), args);
			System.exit(status);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
