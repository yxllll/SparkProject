package edu.ecnu.idse.TrajStore.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import edu.ecnu.idse.TrajStore.io.TextSerializerHelper;

/**
 * Stores cubic information that can be used with spatial-temporal files.
 * The grid is uniform which means all cubic have the same width and the same
 * height.
 * @author
 *
 */

public class CubicInfo extends Cubic{
	public int columns, rows, deepth;
	
	public CubicInfo(){		
	}
	
	public CubicInfo(double x1, double y1, double z1, double x2, double y2, double z2) {
	    super(x1, y1, z1, x2, y2, z2);
	    this.columns = 0;
	    this.rows = 0;
	    this.deepth = 0;
	}
	
	  @Override
	 public void write(DataOutput out) throws IOException {
	    super.write(out);
	    out.writeInt(columns);
	    out.writeInt(rows);
	    out.write(deepth);
	  }
	  
	  @Override
	  public void readFields(DataInput in) throws IOException {
	     super.readFields(in);
	     columns = in.readInt();
	     rows = in.readInt();
	     deepth = in.readInt();
	  }
	
	  @Override
	  public String toString() {
		  return "cubic: "+x1+","+y1+","+z1+"---"+x2+","+y2+","+z2+", " +
				  " averageWidth: "+getAverageCellWidth()+","+" averageHeight:"+getAverageCellHeight()+
				  " averageDeepth"+getAverageCellDeepth()+
				  " total columns:"+columns+" total rows:"+rows+"total deepth:l"+deepth;
	  }
	  
	  public double getAverageCellHeight() {
		  return (y2 - y1) / Math.max(rows, 1);
	  }
	  
	  public double getAverageCellWidth() {
		  return (x2 - x1) / Math.max(columns, 1);
	  }

	  public double getAverageCellDeepth() {
		  return (z2 - z1) / Math.max(columns, 1);
	  }
	  
	  @Override
	  public boolean equals(Object obj) {
		  CubicInfo gi = (CubicInfo) obj;
		  return super.equals(obj)
				  && this.columns == gi.columns && this.rows == gi.rows && this.deepth == gi.deepth;
	  }
	  
	  public void calculateCubicDimensions(long totalFileSize, long blockSize) {
		  // An empirical number for the expected overhead in grid file due to
		  // replication
		  int numBlocks = (int) Math.ceil((double)totalFileSize / blockSize);
		  calculateCubicDimensions(numBlocks);
	}
	  
	  /// 可能有问题！！！！！！！！！！！！！！！！
	  public void calculateCubicDimensions(int numCells) {
		  int gridCols = 1;
		  int gridRows = 1;
		  int gridDeeps = 1;
		  while (gridRows * gridCols * gridDeeps < numCells) {
			  // (  cellWidth          >    cellHeight        )
			  if ((x2 - x1) / gridCols > (y2 - y1) / gridRows && (x2 - x1) / gridCols > (z2 - z1) / gridDeeps) {
				  gridCols++;
			  } else if((y2 - y1) / gridRows > (x2 - x1) / gridCols && (y2 - y1) / gridRows > (z2 - z1) / gridDeeps){
				  gridRows++;
			  }else {
				  gridDeeps++;
			  }
		  }
		  columns = gridCols;
		  rows = gridCols;
		  deepth = gridDeeps;
		  System.out.println("columns: "+columns);
		  System.out.println("rows: "+rows);
		  System.out.println("deept:" +deepth);
	  }

	  @Override
	  public Text toText(Text text) {
	    final byte[] Comma = ",".getBytes();
	    super.toText(text);
	    text.append(Comma, 0, Comma.length);
	    TextSerializerHelper.serializeInt(columns, text, ',');
	    TextSerializerHelper.serializeInt(rows, text, ',');
	    TextSerializerHelper.serializeInt(deepth, text, '\0');
	    return text;
	  }
	  
	  @Override
	  public void fromText(Text text) {
	    super.fromText(text);
	    if (text.getLength() > 0) {
	      // Remove the first comma
	      text.set(text.getBytes(), 1, text.getLength() - 1);
	      columns = (int) TextSerializerHelper.consumeInt(text, ',');
	      rows = (int) TextSerializerHelper.consumeInt(text, ',');
	      deepth = (int) TextSerializerHelper.consumeInt(text, '\0');
	    }
	  }
	  
	  public SpaceInfo[] getAllSpaces() {
		  int spaceIndex = 0;
		  SpaceInfo[] spaces = new SpaceInfo[columns * rows * deepth];
		
		  double cDeepth = (z2-z1)/deepth; //每个小立方体 的深度
		  double cWidth = (x2-x1)/columns;
		  double cHeight = (y2 - y1)/rows;
		  double zbegin,zend;
		  double ybeign,yend;
		  double xbegin,xend;
		  for (int dep =0; dep < deepth; dep++){
			  zbegin = z1 +dep*cDeepth;
			  zend = zbegin +  cDeepth;
			  for(int row = 0 ; row	< rows; row++){
				  ybeign = y1 + row * cHeight;
				  yend  = ybeign + cHeight;
				  for (int col = 0; col < columns; col++) {
					  xbegin = x1+ col * cWidth;
					  xend = xbegin + cWidth;
					  spaces[spaceIndex] = new SpaceInfo(spaceIndex++, xbegin,ybeign,zbegin,xend,yend,zend);
				  }
			  }
		  
		  }
		 return spaces;
	  }
	  
	  // 根据cubic id 返回 所在立方体的信息
	  public SpaceInfo getCubic(int cubicId) {
		  int dep = ((cubicId -1)/ columns)/rows;
		  int flat =  (cubicId -1) % (columns * rows);
		  int col = flat % columns;
		  int row = flat /columns;
		  double xstart = x1 + (x2 - x1) * col / columns;
		  double xend = col == columns - 1? x2 : (x1 + (x2 - x1) * (col + 1) / columns);
		  double ystart = y1 + (y2 - y1) * row / rows;
		  double yend = (row == rows - 1)? y2 : (y1 + (y2 - y1) * (row + 1) / rows);
		  double zstart = z1 + (z2 - z1) * dep / deepth;
		  double zend = dep == deepth -1 ? z2 : (z1 + (z2 - z1) * (dep +1) / deepth);
		  
		 return new SpaceInfo(cubicId, xstart, ystart, zstart, xend, yend, zend);
	  }
}
