package edu.ecnu.idse.TrajStore.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import edu.ecnu.idse.TrajStore.core.Shape;
import edu.ecnu.idse.TrajStore.core.SpaceInfo;
import edu.ecnu.idse.TrajStore.core.SpatialTemporalSite;

public class CubicOutputFormat <S extends Shape> extends FileOutputFormat<IntWritable, S> {
	 @Override
	  public RecordWriter<IntWritable, S> getRecordWriter(FileSystem ignored,
			  											JobConf job,
			  											String name,
			  											Progressable progress) throws IOException {
	    // Get grid info
	    SpaceInfo[] cellsInfo = SpatialTemporalSite.getSpaces(job);
	    for(int i=0;i<cellsInfo.length;i++){
	    	System.out.println(cellsInfo[i]);
	    }
	    CubicRecordWriter<S> writer = new CubicRecordWriter<S>(job, name, cellsInfo);
	    return writer;
	  }
}
