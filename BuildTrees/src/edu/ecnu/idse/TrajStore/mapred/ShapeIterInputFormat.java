package edu.ecnu.idse.TrajStore.mapred;

import java.io.IOException;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.ecnu.idse.TrajStore.core.Cubic;
import edu.ecnu.idse.TrajStore.core.Shape;
import edu.ecnu.idse.TrajStore.mapred.SpatialTemporalRecordReader.ShapeIterator;

public class ShapeIterInputFormat extends SpatialTemporalInputFormat<Cubic, Iterable<? extends Shape>> {
	
	@Override
	public RecordReader<Cubic, Iterable<? extends Shape>> getRecordReader( InputSplit split,
				JobConf job, Reporter reporter) throws IOException{
		
		if(reporter != null)
			reporter.setStatus(split.toString());
		this.rrClass = ShapeIterRecordReader.class;
		return super.getRecordReader(split, job, reporter);
	}
			

}
