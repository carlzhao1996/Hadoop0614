package org.carl.hadoop.Hadoop0614.mapreduce.pc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class PersonWritable implements WritableComparable<PersonWritable> {
	
	private String id;
	private String name;
	
	public PersonWritable(){
		
	}
	
	public void setAll(String id, String name){
		this.setId(id);
		this.setName(name);
	}

	public String getId() {
		return id;
	}



	public void setId(String id) {
		this.id = id;
	}



	public String getName() {
		return name;
	}



	public void setName(String name) {
		this.name = name;
	}


	@Override
	public String toString() {
		return id+"\t"+name;
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.id = in.readUTF();
		this.name=in.readUTF();
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(this.id);
		out.writeUTF(this.name);
	}

	public int compareTo(PersonWritable o) {
		int comp = this.getId().compareTo(o.getId());
		if(0==comp){
			return this.getName().compareTo(o.getName());
		}
		return comp;
	}

}
