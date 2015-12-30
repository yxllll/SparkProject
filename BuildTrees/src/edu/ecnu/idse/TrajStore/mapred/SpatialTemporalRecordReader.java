package edu.ecnu.idse.TrajStore.mapred;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.util.LineReader;
import org.apache.htrace.fasterxml.jackson.databind.deser.std.UntypedObjectDeserializer.Vanilla;

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.ResultTreeType;

import edu.ecnu.idse.TrajStore.core.Cubic;
import edu.ecnu.idse.TrajStore.core.GlobalIndex;
import edu.ecnu.idse.TrajStore.core.Partition;
import edu.ecnu.idse.TrajStore.core.RTree;
import edu.ecnu.idse.TrajStore.core.Shape;
import edu.ecnu.idse.TrajStore.core.SpatialTemporalSite;
import javafx.scene.shape.Line;
import sun.text.normalizer.UBiDiProps;

public abstract class SpatialTemporalRecordReader<K,V> implements RecordReader<K, V> {

	private static final Log LOG = LogFactory.getLog(SpatialTemporalRecordReader.class);
	
	/*
	 * Maximum number of shapes to read in one operation to return as array
	 */
	private int maxShapesInOneRead;
	
	/*
	 * maximum size can be read in one read
	 */
	private int maxBytesInOneRead;
	
	enum BlockType {HEAP, RTREE};
	
	/*
	 * first offfset that is read form the input
	 */
	protected long start;
	/*
	 * last offset to stop at
	 */
	protected long end;
	
	// postition of the next byte to read from the input
	protected long pos;
	
	private InputStream in;
	
	private Seekable filePosition;
	private CompressionCodec codec;
	private Decompressor decompressor;
	
	protected LineReader lineReader;
	protected Text tempLine = new Text();
	protected byte[] buffer;
	
	private FileSystem fs;
	private Path path;
	protected long blockSize;
	protected Cubic spaceMBC;
	protected BlockType blockType;
	
	/*The input stream that reads directly from the input file. 
	 * if the file is not compressed , this stream is the samse as the in..
	 * other wise, this is the raw(compressed) input stream, this stream is used to calculate the progress of the input file.
	*/
	private FSDataInputStream directIN;
	
	
	// initial form a split
	public SpatialTemporalRecordReader (CombineFileSplit split, Configuration conf,
						Reporter reporter, Integer index) throws IOException{
		this(conf, split.getStartOffsets()[index], split.getLength(index), split.getPath(index));
	}
	
	public SpatialTemporalRecordReader(Configuration job, FileSplit split) throws IOException{
		this(job, split.getStart(), split.getLength(), split.getPath());
	}
	
	public SpatialTemporalRecordReader(Configuration job, long s, long l, Path p) throws IOException{

		this.start = s;
		this.end = s + l;
		this.path = p;
		this.fs = this.path.getFileSystem(job);
		this.directIN = fs.open(this.path);
		this.blockSize = fs.getFileStatus(this.path).getBlockSize();
		this.spaceMBC = new Cubic();
		LOG.info("open a spatialRecordReader to file" + this.path);
		codec = new CompressionCodecFactory(job).getCodec(this.path);
		if(isCompressedInput()){
			decompressor = CodecPool.getDecompressor(codec);
			if(codec instanceof SplittableCompressionCodec){
				final SplitCompressionInputStream cIn = ((SplittableCompressionCodec) codec).createInputStream(
						directIN, decompressor, start, end, SplittableCompressionCodec.READ_MODE.BYBLOCK);
				in = cIn;
				start = cIn.getAdjustedStart();
				end = cIn.getAdjustedEnd();
				filePosition = cIn;  // take position from compressed stream                    
			}else{
				in = codec.createInputStream(directIN, decompressor);
				filePosition = directIN;
			}
		}else{
			directIN.seek(start);
			in = directIN;
			filePosition = directIN;
		}
		this.pos = start;
		this.maxShapesInOneRead = job.getInt(SpatialTemporalSite.MaxShapesInOneRead, 1000000);
		this.maxBytesInOneRead = job.getInt(SpatialTemporalSite.MaxBytesInOneRead, 32*1024*1024);  // default 32Mb

	    initializeReader();
	}
	
	public SpatialTemporalRecordReader(InputStream in, long offset, long endOffset) throws IOException{
		this.in = in;
		this.start = offset;
		this.end = endOffset;
		this.pos = offset;
		this.spaceMBC = new Cubic();
		initializeReader();
	}
	
	private long getFilePosition() throws IOException{
		long retVal;
		if(isCompressedInput() && filePosition!= null){
			retVal = filePosition.getPos();
		}else {
			retVal = pos;
		}
		return retVal;
	}
	
	 private boolean isCompressedInput() {
		    return codec != null;
	}	
	
	 @Override
	 public long getPos() throws IOException{
		 return pos;
	 }
	 
	 @Override
	 public void close() throws IOException{
		 try{
			 if(lineReader != null){
				 lineReader.close();
			 }else if(in != null){
				 in.close();
			 }
			 lineReader = null;
			 in = null;
		 }finally{
			 if(decompressor != null)
				 CodecPool.returnDecompressor(decompressor);
		 }
	 }
	 
	 @Override
	 public float getProgress() throws IOException{
		 if(start == end){
			 return 0.0f;
		 }else {
			return Math.min(1.0f, (directIN.getPos() - start)/(float)(end - start));
			
		}
	 }
	/* initialize the reader to read from the input stream or file.
	 * First, it initialize the MBC of the partition being read if the file is globally indexed. It also detects whether the file is R-tree indexed. 
	 * or not which allows it to skip the R-tree if not needed to be read.
	 */
	  protected boolean initializeReader() throws IOException {
		  spaceMBC.invalidate(); // initialize to invalid cubic
		  if(path != null){
			  GlobalIndex<Partition> globalIndex = SpatialTemporalSite.getGlobalIndex(fs, path.getParent());
			  if(globalIndex != null){
				  for(Partition partition: globalIndex){
					  if(partition.filename.equals(path.getName())){
						  spaceMBC.set(partition);
					  }
				  }
			  }
		  }
		  // read the first part of the block to determine its type
		  buffer = new byte[8];
		  int bufferLength = in.read(buffer);
		  if(bufferLength <= 0){
			  buffer = null;
		  }else if(bufferLength < buffer.length){
		      byte[] old_buffer = buffer;
		      buffer = new byte[bufferLength];
		      System.arraycopy(old_buffer, 0, buffer, 0, bufferLength);
		  }
		  if (buffer != null && Arrays.equals(buffer, SpatialTemporalSite.RTreeFileMarkerB)) {
		      blockType = BlockType.RTREE;
		      pos += 8;
		      // Ignore the signature
		      buffer = null;
		    } else{
		    	blockType = BlockType.HEAP;
		    	lineReader = new LineReader(in);
		    	// skip the first line unless we are reading the first block in file
		    	// for globally indexed blocks, never skip the first line in the block             ***************************************??????????????????????????????
		    	boolean skipFirstLine = getPos()!=0;
		    	if(buffer != null && skipFirstLine){
		    	      // Search for the first occurrence of a new line
		    		int eol = RTree.skipToEOL(buffer, 0);
		    		 // If we found an end of line in the buffer, we do not need to skip
		            // a line from the open stream. This happens if the EOL returned is
		            // beyond the end of buffer and the buffer is not a complete line
		            // by itself
		            boolean skip_another_line_from_stream = eol >= buffer.length &&
		                buffer[buffer.length - 1] != '\n';
		            if (eol < buffer.length) {
		              // Found an EOL in the buffer and there are some remaining bytes
		              byte[] tmp = new byte[buffer.length - eol];
		              System.arraycopy(buffer, eol, tmp, 0, tmp.length);
		              buffer = tmp;
		              // Advance current position to skip the first partial line
		              this.pos += eol;
		            } else {
		              // Did not find an EOL in the buffer or found it at the very end
		              pos += buffer.length;
		              // Buffer does not contain any useful data
		              buffer = null;
		            } 
		    		
		            if(skip_another_line_from_stream){
		            	// Didn't find an EOL in the buffer, need to skip it from the stream
		            	 pos += lineReader.readLine(tempLine, Integer.MAX_VALUE, (int)(end - pos));
		                 if (pos >= end) {
		                   // Special case when the whole split is in the middle of a line
		                   // Skip the split
		                   // Increase position beyond end to ensure the next call to
		                   // nextLine would return false
		                   pos++;
		            }
		    	}
		    }
	     }
		    	return true;
	  }
	
	  /*
	   * Read the next line from input and return true if a line was read.
	   * If no more lines are available in this split, a false is returned.
	   */
	  protected boolean nextLine(Text value)throws IOException{
		  if(blockType == BlockType.RTREE && pos==8){
			  // file is positioned at the RTree header skip the header and go to first data object in file
			  pos += RTree.skipHeader(in);
			  LOG.info("Skipped R-tree to position:" + pos);
			  lineReader = new LineReader(in);
		  }
		  
		  while( getFilePosition() <= end ){
			  value.clear();
			  int b = 0;
			  if(buffer != null){
				  // read the first line encountered in buffer
				  int eol = RTree.skipToEOL(buffer, 0);
				  b += eol;
				  value.append(buffer, 0, eol);
				  if(eol < buffer.length){
			          // There are still some bytes remaining in buffer
			          byte[] tmp = new byte[buffer.length - eol];
			          System.arraycopy(buffer, eol, tmp, 0, tmp.length);
			          buffer = tmp;
			        
				  }else{
					  buffer = null;
				  }
				  // Check if a complete line has been read from the buffer
			      byte last_byte = value.getBytes()[value.getLength()-1];
			      if (last_byte == '\n' || last_byte == '\r')
			          return true;
			 }
		  
		  // read the first line from stream
		  Text temp = new Text();
		  b += lineReader.readLine(temp);
		  if(b == 0){
			  return false;
		  }
		  pos +=b;
		  value.append(temp.getBytes(), 0, temp.getLength());
		  
		  if(value.getLength() > 1){
			  // read a non-empty line. Note that end-of line character is included
			  return true;
		  }
	  }
	  // reached end of file
		  return false;
	  }
	 
	  // reads next shape from input and returns true. If no more shapes are left. In the split, a false is returned. This function first reads a line, then parses the returned
	  // by calling on that line. If no stock shape is set, a is thrown.
	  
	  protected boolean nextShape(Shape s) throws IOException{
		  if(! nextLine(tempLine))
			  return false;
		  s.fromText(tempLine);
		  		return true;
	  }
	  
	  /*
	   * reads all shapes left in the current block in one shot, this function runs a loop while it keeps reading shapes by calling the methodunitl one of the following 
	   * conditions happen.
	   */
	  
	  protected boolean nextShapes(ArrayWritable shapes) throws IOException{
		    // Prepare a vector that will hold all objects in this 
		    Vector<Shape> vshapes = new Vector<Shape>();
		    try {
		      Shape stockObject = (Shape) shapes.getValueClass().newInstance();
		      // Reached the end of this split
		      if (getFilePosition() >= end)
		        return false;
		      
		      long initialReadPos = getPos();
		      long readBytes = 0;
		      
		      // Read all shapes in this block
		      while ((maxShapesInOneRead <= 0 || vshapes.size() < maxShapesInOneRead) &&
		          (maxBytesInOneRead <= 0 || readBytes < maxBytesInOneRead) &&
		          nextShape(stockObject)) {
		        vshapes.add(stockObject.clone());
		        readBytes = getPos() - initialReadPos;
		      }

		      // Store them in the return value
		      shapes.set(vshapes.toArray(new Shape[vshapes.size()]));
		      
		      return vshapes.size() > 0;
		    } catch (InstantiationException e1) {
		      e1.printStackTrace();
		    } catch (IllegalAccessException e1) {
		      e1.printStackTrace();
		    } catch (OutOfMemoryError e) {
		      LOG.error("Error reading shapes. Stopped with "+vshapes.size()+" shapes");
		      throw e;
		    }
		    return false;
		  }
	  
	  /**
	   * Returns an iterator that iterates over all remaining shapes in the file.
	   */
	  protected boolean nextShapeIter(ShapeIterator iter) throws IOException {
	    iter.setSpatialRecordReader((SpatialTemporalRecordReader<?, ? extends Shape>) this);
	    return iter.hasNext();
	  }
	  
	  public static class ShapeIterator implements Iterator<Shape>, Iterable<Shape>{

		    protected Shape shape;
		    protected Shape nextShape;
		    private SpatialTemporalRecordReader<?, ? extends Shape> srr;
		    
		    public ShapeIterator() {
		    }

		    public void setSpatialRecordReader(SpatialTemporalRecordReader<?, ? extends Shape> srr) {
		      this.srr = srr;
		      try {
		        if (shape != null)
		          nextShape = shape.clone();
		        if (nextShape != null && !srr.nextShape(nextShape))
		            nextShape = null;
		      } catch (IOException e) {
		      }
		    }
		    
		    public void setShape(Shape shape) {
		      this.shape = shape;
		      this.nextShape = shape.clone();
		      try {
		        if (srr != null && !srr.nextShape(nextShape))
		            nextShape = null;
		      } catch (IOException e) {
		      }
		    }

		    public boolean hasNext() {
		      if (nextShape == null)
		        return false;
		      return nextShape != null;
		    }

		    @Override
		    public Shape next() {
		      try {
		        if (nextShape == null)
		          return null;
		        // Swap Shape and nextShape and read next
		        Shape temp = shape;
		        shape = nextShape;
		        nextShape = temp;
		        
		        if (!srr.nextShape(nextShape))
		          nextShape = null;
		        return shape;
		      } catch (IOException e) {
		        return null;
		      }
		    }

		    @Override
		    public Iterator<Shape> iterator() {
		      return this;
		    }

		    @Override
		    public void remove() {
		      throw new RuntimeException("Unsupported method ShapeIterator#remove");
		    }
		
	  }
	  
	  protected boolean nextRTree(RTree<? extends Shape> rtree) throws IOException {
		    if (blockType == BlockType.RTREE) {
		      if (getPos() != 8)
		        return false;
		      // Signature was already read in initialization.
		      buffer = null;
		      DataInput dataIn = in instanceof DataInput?
		          (DataInput) in : new DataInputStream(in);
		      rtree.readFields(dataIn);
		      pos++;
		      return true;
		    } else {
		      throw new RuntimeException("Not implemented");
		    }
		  }
	  
	  
}