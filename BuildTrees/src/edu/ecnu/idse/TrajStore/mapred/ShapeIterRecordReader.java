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
import edu.ecnu.idse.TrajStore.util.OperationsParams;
import edu.ecnu.idse.TrajStore.mapred.SpatialTemporalRecordReader.ShapeIterator;

public class ShapeIterRecordReader  extends SpatialTemporalRecordReader<Cubic, ShapeIterator>{

	public static final Log LOG = LogFactory.getLog(ShapeIterRecordReader.class);
	private Shape shape;
	
	public ShapeIterRecordReader(CombineFileSplit split, Configuration conf, Reporter reporter,Integer index) throws IOException{
		super(split, conf, reporter, index);
		this.shape = OperationsParams.getShape(conf, "shape"); //      may need to be changed!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		
	}
	
	public ShapeIterRecordReader(Configuration conf, FileSplit split) throws IOException{
		super(conf, split);
		this.shape = OperationsParams.getShape(conf, "shape"); // shape = params.getShape("shape");    
	}
	
	public ShapeIterRecordReader(InputStream is, long offset, long endOffset) throws IOException{
		super(is, offset,endOffset);
	}
	
	public void setShape(Shape shape){
		this.shape = shape;
	}
	
	@Override
	public boolean next(Cubic key, ShapeIterator shapeIterator) throws IOException{
		// get cubicInfo for the current position in file
		boolean elemets = nextShapeIter(shapeIterator);
		key.set(spaceMBC);
		return elemets;
	}
	
	@Override
	public Cubic createKey(){
		return new Cubic();
	}
	
	@Override 
	public ShapeIterator createValue(){
		ShapeIterator shapeIter = new ShapeIterator();
		shapeIter.setShape(shape);
		return shapeIter;
	}
	
}
