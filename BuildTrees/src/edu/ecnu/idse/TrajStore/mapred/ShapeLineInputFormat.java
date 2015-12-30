package edu.ecnu.idse.TrajStore.mapred;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.ecnu.idse.TrajStore.core.Cubic;

public class ShapeLineInputFormat extends SpatialTemporalInputFormat<Cubic, Text> {

	@Override
	public RecordReader<Cubic, Text> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException{
		if(reporter != null){
			reporter.setStatus(split.toString());
		}
		this.rrClass = ShapeLineRecordReader.class;
		return super.getRecordReader(split, job, reporter);
	}
}
