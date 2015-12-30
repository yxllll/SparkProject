package edu.ecnu.idse.TrajStore.mapred;

import java.io.IOException;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.ecnu.idse.TrajStore.core.Cubic;
import edu.ecnu.idse.TrajStore.core.Shape;

/*
 * @author ZZG
 */
public class ShapeInputFormat <S extends Shape> extends SpatialTemporalInputFormat<Cubic, S>{
	
	@Override
	public RecordReader<Cubic, S> getRecordReader(InputSplit split,
					JobConf  job, Reporter reporter) throws IOException{
		if(reporter != null)
			reporter.setStatus(split.toString());
		this.rrClass = ShapeRecordReader.class;
		return super.getRecordReader(split, job, reporter);
	}

}
