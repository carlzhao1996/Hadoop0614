package org.carl.hadoop.Hadoop0614.mapreduce.pv;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PVMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private Text outputKey = new Text();
	private IntWritable outputValue = new IntWritable(1);
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		String[] items = line.split("\t");
		//filter
		if(36<items.length){
			if(StringUtils.isNotBlank(items[1])&&StringUtils.isNotBlank(items[24]))
			{
				this.outputKey.set(items[24]);
				context.write(this.outputKey, this.outputValue);
			}else{
				context.getCounter("usergroup","cityId or url is not valid").increment(1L);
				return;
			}
		}else{
			context.getCounter("usergroup","length is not valid").increment(1L);
			return;
		}
	}
}
