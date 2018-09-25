package org.carl.hadoop.Hadoop0614.mapreduce.sort01;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Sort01Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private Text outputKey = new Text();
	private IntWritable outputValue = new IntWritable();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		String[] items = line.split("\t");
		this.outputKey.set(items[0]);
		this.outputValue.set(Integer.valueOf(items[1]));
		context.write(outputKey, outputValue);
	}

}
