package org.carl.hadoop.Hadoop0614.mapreduce.sort01;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Sort01Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	private IntWritable outputValue = new IntWritable();
	List<Integer> list = new ArrayList<Integer>();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//Add to list
		list.clear();
		for(IntWritable value: values){
			list.add(value.get());
		}
		//sort
		Collections.sort(list,new Comparator<Integer>(){

			public int compare(Integer o1, Integer o2) {
				// TODO Auto-generated method stub
				if(o1>o2){
					return -1;
				}else if(o1<o2){
					return 1;
				}else{
					return 0;
				}
			}
			
		});
		//output
		for (int i : list) {
			this.outputValue.set(i);
			context.write(key, outputValue);
		}
	}
}
