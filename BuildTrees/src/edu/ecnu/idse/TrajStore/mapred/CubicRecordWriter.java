package edu.ecnu.idse.TrajStore.mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import edu.ecnu.idse.TrajStore.core.Shape;
import edu.ecnu.idse.TrajStore.core.SpaceInfo;

/*
* A record writer that can be used in MapReduce programs to write an index
* file where the key is the sub-space ID and the value is the shape to write to
* that sub-space. A given shape is not implicitly replicated to any other sub-spaces
* other than the one provided.
*/

public class CubicRecordWriter <S extends Shape> extends 
edu.ecnu.idse.TrajStore.core.CubicRecordWriter<S>
implements RecordWriter<IntWritable, S>{
	
	public CubicRecordWriter(JobConf job, String name, SpaceInfo[] cubics) throws IOException{
		super(null, job, name, cubics);
		/*OutDir Job  Prefix,SPaceINfos  */
	}
	
	 @Override
	 public void write(IntWritable key, S value) throws IOException {
		// System.out.println(key.get()+"---"+value);
		 if(key.get() < 0){
			 System.out.println("Exception");
		 }
	    super.write(key.get(), value);
	 }

	  @Override
	 public void close(Reporter reporter) throws IOException {
	    super.close(reporter);
	 }
	
	

}
