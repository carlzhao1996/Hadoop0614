package org.carl.hadoop.Hadoop0614.mapreduce.pc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class PCReducer extends Reducer<PersonWritable, IntWritable, PersonWritable, IntWritable> {
	
	private IntWritable outputValue = new IntWritable();
	
	@Override
	protected void reduce(PersonWritable key, Iterable<IntWritable> values,Context context)throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int sum=0;
		for(IntWritable value : values){
			sum +=value.get();
		}
		this.outputValue.set(sum);
		context.write(key, outputValue);
	}

}
