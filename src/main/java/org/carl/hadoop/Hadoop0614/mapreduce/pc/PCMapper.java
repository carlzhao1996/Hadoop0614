package org.carl.hadoop.Hadoop0614.mapreduce.pc;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PCMapper extends Mapper<LongWritable, Text, PersonWritable, IntWritable> {
	private PersonWritable outputKey = new PersonWritable();
	private IntWritable outputValue = new IntWritable(1);
@Override
protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		String line = value.toString();
		String[] people = line.split(" ");
		if(4==people.length){
			if(StringUtils.isNotBlank(people[0])&&StringUtils.isNotBlank(people[1])){
				this.outputKey.setAll(people[0], people[1]);
				context.write(this.outputKey, this.outputValue);
			}else{
				context.getCounter("Person Group","Person data is not valid").increment(1L);
			}
		}else{
			context.getCounter("Person Group","Person data length is not valid").increment(1L);
		}
	}
}
