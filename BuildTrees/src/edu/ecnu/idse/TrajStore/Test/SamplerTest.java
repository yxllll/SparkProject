package edu.ecnu.idse.TrajStore.Test;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.ecnu.idse.TrajStore.core.Point;
import edu.ecnu.idse.TrajStore.core.ResultCollector;
import edu.ecnu.idse.TrajStore.core.SpatialTemporalSite;
import edu.ecnu.idse.TrajStore.io.TextSerializable;
import edu.ecnu.idse.TrajStore.operations.Sampler;
import edu.ecnu.idse.TrajStore.util.OperationsParams;

public class SamplerTest {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
		 final Vector<Point> sample = new Vector<Point>();
		 float sample_ratio =
		            params.getFloat(SpatialTemporalSite.SAMPLE_RATIO, 0.01f);
		 long sample_size =
		           params.getLong(SpatialTemporalSite.SAMPLE_SIZE, 100*1024*1024);
		 params.setFloat("ratio", sample_ratio);
		 params.setClass("outshape", Point.class, TextSerializable.class);
		  ResultCollector<Point> resultCollector = new ResultCollector<Point>(){
		      @Override
		      public void collect(Point value) {
		        sample.add(value.clone());
		      }
		    };
		 Path inPath =  new Path(args[0]);
/*		 FileSystem fs = inPath.getFileSystem(params);
		 FileStatus[] fStatus =fs.listStatus(inPath, new PathFilter() {
			
			@Override
			public boolean accept(Path path) {
				if(path.getName().startsWith("_"))
				return false;
				else
					return true;
			}
		});
		 Path[] inPaths = new Path[fStatus.length];
		 for(int i = 0;i< fStatus.length;i++){
			 inPaths[i] = fStatus[i].getPath();
		 }*/
		 Path[] inPaths = new Path[1];
		 inPaths[0] = inPath;
		    
		Sampler.sampleWithRatio(inPaths, resultCollector, params);
		
	}

}
