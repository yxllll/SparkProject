package edu.ecnu.idse.TrajStore.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import edu.ecnu.idse.TrajStore.util.OperationsParams;

public class GridPartitioner  extends Partitioner{
	private static final Log LOG = LogFactory.getLog(GridPartitioner.class);
	
	// the information of the underlyign grid
	private GridInfo gridInfo;
	
	public GridPartitioner(){
		// initialize the grid info so that readfields work correctly

		this.gridInfo = new GridInfo();
	}
	
	  public void createFromPoints(Rectangle mbr, Point[] points, int numPartitions) {
		    this.gridInfo.set(mbr.x1, mbr.y1, mbr.x2, mbr.y2);
		    this.gridInfo.calculateCellDimensions(numPartitions);
	 }
	
	  public GridPartitioner(Path[] inFiles, JobConf job){
		  Rectangle inMBR = (Rectangle) OperationsParams.getShape(job, "mbr");
		  this.gridInfo = new GridInfo(inMBR.x1, inMBR.y1, inMBR.x2, inMBR.y2);
		  int numOfPartitions = job.getInt("m", job.getNumReduceTasks()* job.getNumReduceTasks());
		  this.gridInfo.calculateCellDimensions(numOfPartitions);
	  }

	  public GridPartitioner(Path[] inFiles, JobConf job, int width, int height){
		  Rectangle inMBR = (Rectangle) OperationsParams.getShape(job, "mbr");
		  this.gridInfo = new GridInfo(inMBR.x1, inMBR.y1, inMBR.x2, inMBR.y2);
		  this.gridInfo.columns = width;
		    this.gridInfo.rows = height;
	  }
	
	  public void write(DataOutput out) throws IOException {
		    this.gridInfo.write(out);
		  }

		  @Override
		  public void readFields(DataInput in) throws IOException {
		    this.gridInfo.readFields(in);
		  }

		  @Override
		  public int getPartitionCount() {
		    return gridInfo.rows * gridInfo.columns;
		  }
		  
		  @Override
		  public CellInfo getPartition(int partitionID) {
		    return gridInfo.getCell(partitionID);
		  }

		  @Override
		  public CellInfo getPartitionAt(int index) {
		    return getPartition(index+1);
		  }
		  
		  public void overlapPartitions(Shape shape, ResultCollector<Integer> matcher) {
			    if (shape == null)
			      return;
			    Rectangle shapeMBR = shape.getMBR();
			    if (shapeMBR == null)
			      return;
			    java.awt.Rectangle overlappingCells = this.gridInfo.getOverlappingCells(shapeMBR);
			    for (int x = overlappingCells.x; x < overlappingCells.x + overlappingCells.width; x++) {
			      for (int y = overlappingCells.y; y < overlappingCells.y + overlappingCells.height; y++) {
			        matcher.collect(this.gridInfo.getCellId(x, y));
			      }
			    }
			  }
		  
		  @Override
		  public int overlapPartition(Shape shape) {
		    if (shape == null)
		      return -1;
		    Rectangle shapeMBR = shape.getMBR();
		    if (shapeMBR == null)
		      return -1;
		    Point centerPoint = shapeMBR.getCenterPoint();
		    return this.gridInfo.getOverlappingCell(centerPoint.x, centerPoint.y);
		  }
}
