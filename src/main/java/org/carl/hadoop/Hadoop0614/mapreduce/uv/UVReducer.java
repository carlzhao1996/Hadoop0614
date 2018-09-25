package org.carl.hadoop.Hadoop0614.mapreduce.uv;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UVReducer extends Reducer<Text, Text, Text, IntWritable> {
	
	private IntWritable outputValue = new IntWritable();
	Set<String> sets = new HashSet<String>();
	
	@Override
	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		sets.clear();
		for(Text value : values){
			sets.add(value.toString());
		}
		this.outputValue.set(sets.size());
		context.write(key, outputValue);
	}

}
