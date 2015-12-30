package edu.ecnu.idse.TrajStore.mapred;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import edu.ecnu.idse.TrajStore.core.Cubic;
import edu.ecnu.idse.TrajStore.core.Shape;
import edu.ecnu.idse.TrajStore.core.SpatialTemporalSite;

public class ShapeRecordReader <S extends Shape> extends SpatialTemporalRecordReader<Cubic, S>{
	
	@SuppressWarnings("unused")
	private static final Log LOG = LogFactory.getLog(ShapeRecordReader.class);
	
	// Object used for deserialization
	private S stockShape;
	
	public ShapeRecordReader(Configuration job, FileSplit split)throws IOException{
		super(job, split);
		stockShape = (S) SpatialTemporalSite.createStockShape(job);
	}
	
	public ShapeRecordReader(CombineFileSplit split, Configuration conf, Reporter reporter, Integer index) throws IOException{
		super(split, conf,	reporter,index);
		stockShape = (S) SpatialTemporalSite.createStockShape(conf);
	}
	
	public ShapeRecordReader(InputStream in, long offset, long endOffset) throws IOException{
		super(in, offset,endOffset);
	}
	
	@Override 
	public boolean next(Cubic key, S shape) throws IOException{
		boolean read_line = nextShape(shape);
		key.set(spaceMBC);
		return read_line;
	}

	@Override
	public Cubic createKey(){
		return new Cubic();
	}
	
	@Override
	public S createValue(){
		return stockShape;
	}
}
