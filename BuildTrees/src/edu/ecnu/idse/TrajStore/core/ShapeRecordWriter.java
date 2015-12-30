package edu.ecnu.idse.TrajStore.core;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.Progressable;

public interface ShapeRecordWriter<S extends Shape> {
	/*
	 * Writes the given shape to the file to all cubics it overlaps with
	 */
	public void write(NullWritable dummy, S shape) throws IOException;
	
	/**
	   * Writes the given shape to the specified sub-space
	   * @param spaceId
	   * @param shape
	   */
	  public void write(int spaceId, S shape) throws IOException;
	  
	  /**
	   * Writes the given shape only to the given sub-space even if it overlaps
	   * with other sub-spaces. This is used when the output is prepared to write
	   * only one sub-space. The spacer ensures that another spacer will write the object
	   * to the other spaces(s) later.
	   * @param spaceInfo
	   * @param shape
	   * @throws IOException
	   */
	  public void write(SpaceInfo spaceInfo, S shape) throws IOException;
	  
	  /**
	   * Sets a stock object used to serialize/deserialize objects when written to
	   * disk.
	   * @param shape
	   */
	  public void setStockObject(S shape);
	  
	  /**
	   * Closes this writer
	   * @param reporter
	   * @throws IOException
	   */
	  public void close(Progressable progressable) throws IOException;
	  
}
