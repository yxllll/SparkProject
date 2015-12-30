package edu.ecnu.idse.TrajStore.mapred;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import edu.ecnu.idse.TrajStore.core.Cubic;

public class ShapeLineRecordReader  extends SpatialTemporalRecordReader<Cubic, Text>{

	public ShapeLineRecordReader(Configuration job, FileSplit split)
				throws IOException{
		super(job, split);
	}
	
	public ShapeLineRecordReader(CombineFileSplit split, Configuration conf, Reporter reporter, 
			Integer index) throws IOException{
		super(split, conf, reporter, index);
	}
	
	public ShapeLineRecordReader(InputStream in, long offset, long endOffset)
		throws IOException{
		super(in, offset,endOffset);
	}
	
	@Override
	public boolean next(Cubic key, Text shapeLine) throws IOException{
		//这一行调用 nextLine
		boolean read_line = nextLine(shapeLine);
		key.set(spaceMBC);   // the mbc of this space.
		return read_line;
	}
	

	public Cubic createKey(){
		return new Cubic();
	}
	

	public Text createValue(){
		return new Text();
	}
	
}
