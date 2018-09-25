package org.carl.hadoop.Hadoop0614.mapreduce.pv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class PVDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//job
		Job job = Job.getInstance(this.getConf(),"pv");
		job.setJarByClass(PVDriver.class);
		//input
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		//map
		job.setMapperClass(PVMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		//shuffle
		//Use default shuffle
		//reduce
		job.setReducerClass(PVReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(2);
		//output
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//submit
		return job.waitForCompletion(true)?0:-1;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		//conf.set("fs.defaultFS", "hdfs://bigdata.demo.com:8020");
		try {
			int status = ToolRunner.run(conf, new PVDriver(), args);
			System.exit(status);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
