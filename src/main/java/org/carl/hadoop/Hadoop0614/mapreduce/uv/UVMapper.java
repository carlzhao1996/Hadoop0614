package org.carl.hadoop.Hadoop0614.mapreduce.uv;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UVMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text outputKey = new Text();
	private Text outputValue = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		String[] items = line.split("\t");
		
		if(items.length >36){
			if(StringUtils.isNotBlank(items[23])&&StringUtils.isNotBlank(items[5])){
				this.outputKey.set(items[23]);
				this.outputValue.set(items[15]);
				context.write(this.outputKey, this.outputValue);
			}else{
				context.getCounter("ip Group: ","ip or province data is not valid");
				return;
			}
		}else{
			context.getCounter("ip Group:","Data length is not valid");
			return;
		}
	}
}
