package edu.ecnu.idse.TrajStore.core;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

public class CubicRecordWriter <S extends Shape>
	implements ShapeRecordWriter<S>{
	
	  public static final Log LOG = LogFactory.getLog(CubicRecordWriter.class);
	  /**The spatial-temporal boundaries for each cell*/
	  protected SpaceInfo[] spaces;
	  
	  /**Paths of intermediate files*/
	  protected Path[] intermediateSpacePath;
	  
	  /**An output stream for each grid cell*/
	  protected OutputStream[] intermediateSpaceStreams;

	  /**MBC of the records written so far to each space*/
	  protected Cubic[] spacesMbc;
	  
	  /**Job configuration if part of a MapReduce job*/
	  protected JobConf jobConf;
	  /**Path of the output directory if not part of a MapReduce job*/
	  protected Path outDir;
	  
	  /**File system for output path*/
	  protected final FileSystem fileSystem;

	  /**Temporary text to serialize one object*/
	  protected Text text;
	  
	  /**Block size for grid file written*/
	  protected long blockSize;
	  
	  /**A stock object used for serialization/deserialization*/
	  protected S stockObject;
	  
	  /**An output stream to the master file*/
	  protected OutputStream masterFile;
	  
	  /**A list of threads closing spaces in background*/
	  protected ArrayList<Thread> closingThreads;
	  
	  /**
	   * Keeps the number of elements written to each space so far.
	   * Helps calculating the overhead of RTree indexing
	   */
	  protected int[] intermediateSpaceRecordCount;
	  
	  /**Size in bytes of intermediate files written so far*/
	  protected int[] intermediateSpacesize;
	  
	  /**New line marker to separate records*/
	  protected static byte[] NEW_LINE;
	  
	  static {
		    try {
		      NEW_LINE = System.getProperty("line.separator").getBytes("utf-8");
		    } catch (UnsupportedEncodingException e) {
		      e.printStackTrace();
		    }
	  }
	  
	  /**A unique prefix to all files written by this writer*/
	  protected String prefix;
	  
	  /**Pack MBC of each space around its content after it's written to disk*/
	  protected boolean pack;
	  
	  /**Expand MBC of each space to totally cover all of its contents*/
	  private boolean expand;
	  private int counter;
	  
	  /**Type of index being constructed*/
	  private String sindex;

	  /**
	   * A list of indexes the can be optimized by packing each partition to remove
	   * empty space
	   */
	  public static final Set<String> PackedIndexes;
	  
	  /**
	   * Indexes where an object might be replicated to multiple partitions.
	   */
	  public static final Set<String> ReplicatedIndexes;

	  /**
	   * A list of indexes in which each partition has to be expanded to fully
	   * contain all the records inside it
	   */
	  public static final Set<String> ExpandedIndexes;
	  
	  static {
	    PackedIndexes = new HashSet<String>();
	    PackedIndexes.add("heap");
	    PackedIndexes.add("rtree");
	    PackedIndexes.add("r+tree");
	    PackedIndexes.add("str");
	    PackedIndexes.add("str+");
	    ExpandedIndexes = new HashSet<String>();
	    ExpandedIndexes.add("heap");
	    ExpandedIndexes.add("rtree");
	    ExpandedIndexes.add("str");
	    ReplicatedIndexes = new HashSet<String>();
	    ReplicatedIndexes.add("grid");
	    ReplicatedIndexes.add("r+tree");
	    ReplicatedIndexes.add("str+");
	  }

	  /**
	   * Creates a new GridRecordWriter that will write all data files to the
	   * given directory
	   * @param outDir - The directory in which all files will be stored
	   * @param job - The MapReduce job associated with this output
	   * @param prefix - A unique prefix to be associated with files of this writer
	   * @param spaces - spaces to partition the file
	   * @param pack - After writing each space, pack its MBR around contents
	   * @throws IOException
	   */
	  
	  public CubicRecordWriter(Path outDir, JobConf job, String prefix,
		      SpaceInfo[] spaces) throws IOException {
		  	//this.spaces = spaces;	
		  	if (job != null) {
		   //   this.sindex = job.get("sindex", "heap");
		  		this.sindex = job.get("sindex");
		      System.out.println("sindex : "+this.sindex);
		      this.pack = PackedIndexes.contains(sindex);
		      this.expand = ExpandedIndexes.contains(sindex);
		    }
		    this.prefix = prefix;
		    this.fileSystem = outDir == null ? 
		      FileOutputFormat.getOutputPath(job).getFileSystem(job):
		      outDir.getFileSystem(job != null? job : new Configuration());
		    this.outDir = outDir;
		    this.jobConf = job;
		    //spaces！= null means a lot of subspaces
		    //spaces == null means only one subspace
		    if (spaces != null) {
		      // Make sure spaceIndex maps to array index. This is necessary for spaces that
		      // call directly write(int, Text)
		      int highest_index = 0;
		      
		      for (SpaceInfo space : spaces) {
		        if (space.spaceId > highest_index)
		          highest_index = (int) space.spaceId;
		      }
		      
		      // Create a master file that contains meta information about partitions
		      masterFile = fileSystem.create(getMasterFilePath());
		      
		      this.spaces = new SpaceInfo[highest_index + 1];
		      //space id 从1开始，所以第0个为空
		      for (SpaceInfo space : spaces)
		        this.spaces[(int) space.spaceId] = space;
		      System.out.println("new space ids");
		      for (int i = 1; i < this.spaces.length; i++) {
				System.out.println("space id"+this.spaces[i].spaceId);
			}
		      // Prepare arrays that hold spaces information
		      intermediateSpaceStreams = new OutputStream[this.spaces.length];
		      intermediateSpacePath = new Path[this.spaces.length];
		      spacesMbc = new Cubic[this.spaces.length];
		      // Initialize the counters for each spaces
		      intermediateSpaceRecordCount = new int[this.spaces.length];
		      intermediateSpacesize = new int[this.spaces.length];

		    } else {
		    	intermediateSpaceStreams = new OutputStream[1];
		    	intermediateSpacePath = new Path[1];
		    	spacesMbc = new Cubic[1];
		    	intermediateSpacesize = new int[1];
		    	intermediateSpaceRecordCount = new int[1];
		    }
		    for (int i = 0; i < spacesMbc.length; i++) {
		    	spacesMbc[i] = new Cubic(Double.MAX_VALUE, Double.MAX_VALUE,Double.MAX_VALUE,
		          -Double.MAX_VALUE, -Double.MAX_VALUE, -Double.MAX_VALUE);
		    }

		    this.blockSize = fileSystem.getDefaultBlockSize();
		    
		    closingThreads = new ArrayList<Thread>();
		    text = new Text();
		  }

	  protected Path getMasterFilePath() throws IOException {
		    String extension = sindex;
		    return getFilePath("_master."+extension);
	  }
		  
		  /**
		   * Returns a path to a file with the given name in the output directory
		   * of the record writer.
		   * @param filename
		   * @return
		   * @throws IOException
		   */
	  protected Path getFilePath(String filename) throws IOException {
		    if (prefix != null)
		      filename = prefix + "_" + filename;
		    return outDir != null ? new Path(outDir, filename) : 
		      FileOutputFormat.getTaskOutputPath(jobConf, filename);
	  }
		  
	  public void setStockObject(S stockObject) {
		  this.stockObject = stockObject;
	  }

	 @Override
	  public synchronized void write(NullWritable dummy, S shape) throws IOException {
		   if (spaces == null) {
	      // No cells. Write to the only stream open to this file
	         writeInternal(0, shape);
		   } else {
	      // Check which cells should contain the given shape
	      Cubic mbc = shape.getMBC();
	      	for (int spaceIndex = 0; spaceIndex < spaces.length; spaceIndex++) {
	      		if (spaces[spaceIndex] != null && mbc.isIntersected(spaces[spaceIndex])) {
	      			writeInternal(spaceIndex, shape);
	      		}
	      	}
		   }
	 }
		  
		  /**
		   * Write the given shape to a specific sub-space. The shape is not replicated to any other sub-spaces.
		   * It's just written to the given sub-space. This is useful when shapes are already assigned
		   * and replicated to subspaces another way, e.g. from a map phase that partitions.
		   * @param spaceInfo
		   * @param shape
		   * @throws IOException
		   */
		  @Override
	  public synchronized void write(SpaceInfo spaceInfo, S shape) throws IOException {
		    for (int i_space = 0; i_space < spaces.length; i_space++) {
		      if (spaceInfo.equals(spaces[i_space]))
		        write(i_space, shape);
		    }
	  }
		  
		  /**
		   * Write a shape given the MBC of its partition. If no partition exists with
		   * such an MBC, the corresponding partition is added first.
		   * @param rect
		   * @param shape
		   * @throws IOException
		   */
	  public synchronized void write(Cubic cub, S shape) throws IOException {
		    int i_space = 1;
		    if (spaces == null) {
		      // Initialize cells array if null
		      spaces = new SpaceInfo[1];
		    }
		    while (i_space < spaces.length && !cub.equals(spaces[i_space])) {
		    	i_space++;
		    }
		    if (i_space >= spaces.length) {
		      // space doesn't exist, create it first
		      SpaceInfo[] newSpaces = new SpaceInfo[i_space + 1];
		      System.arraycopy(spaces, 0, newSpaces, 0, spaces.length);
		      newSpaces[i_space] = new SpaceInfo(i_space, cub);
		      spaces = newSpaces;
		      
		      // Expand auxiliary data structures too
		      Path[] newIntermediateSpacePath = new Path[spaces.length];
		      if (newIntermediateSpacePath != null)
		        System.arraycopy(intermediateSpacePath, 0, newIntermediateSpacePath, 0,
		        		intermediateSpacePath.length);
		      intermediateSpacePath = newIntermediateSpacePath;
		      
		      OutputStream[] newIntermediateSpaceStreams = new OutputStream[spaces.length];
		      if (intermediateSpaceStreams != null)
		        System.arraycopy(intermediateSpaceStreams, 0, newIntermediateSpaceStreams,
		      		  0, intermediateSpaceStreams.length);
		      intermediateSpaceStreams = newIntermediateSpaceStreams;
		      
		      Cubic[] newCellsMbc = new Cubic[spaces.length];
		      if (newCellsMbc != null)
		        System.arraycopy(spacesMbc, 0, newCellsMbc, 0, spacesMbc.length);
		      newCellsMbc[i_space] = new Cubic(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE,
		          -Double.MAX_VALUE, -Double.MAX_VALUE, -Double.MAX_VALUE);
		      spacesMbc = newCellsMbc;
		    }
		    write(i_space, shape);
	  }
		  
		  @Override
	  public void write(int spaceId, S shape) throws IOException {
		    writeInternal(spaceId, shape);
	  }
		  
	  /**
	   * Write the given shape to the spaceIndex indicated.
	   * @param cellIndex
	   * @param shape
	   * @throws IOException
	   */
	  protected synchronized void writeInternal(int spaceIndex, S shape) throws IOException {
		    if (spaceIndex < 0) {
		      // A special marker to close a space
		    	spaceIndex = 0- spaceIndex;
		      closeSpace(spaceIndex);
		      return;
		    }
		    try {
		      spacesMbc[spaceIndex].expand(shape.getMBC());
		    } catch (NullPointerException e) {
		      e.printStackTrace();
		    }
		    // Convert shape to text
		    text.clear();
		    shape.toText(text);
		    // Write text representation to the file
		    OutputStream cellStream = getIntermediateSpaceStream(spaceIndex);
		    cellStream.write(text.getBytes(), 0, text.getLength());
		    cellStream.write(NEW_LINE);
		    intermediateSpacesize[spaceIndex] += text.getLength() + NEW_LINE.length;
		    intermediateSpaceRecordCount[spaceIndex]++;
	  }
		  
		  /**
		   * Returns an output stream in which records are written as they come before
		   * they are finally flushed to the cell file.
		   * @param cellIndex
		   * @return
		   * @throws IOException
		   */
      protected OutputStream getIntermediateSpaceStream(int spaceIndex) throws IOException {
		    if (intermediateSpaceStreams[spaceIndex] == null) {
		      // For grid file, we write directly to the final file
		      intermediateSpacePath[spaceIndex] = getFinalSpacePath(spaceIndex);
		      intermediateSpaceStreams[spaceIndex] = createFinalSpaceStream(intermediateSpacePath[spaceIndex]);
		    }
		    return intermediateSpaceStreams[spaceIndex];
      }
		  
		  
		  /**
		   * Creates an output stream that will be used to write the final cell file
		   * @param cellFilePath
		   * @return
		   * @throws IOException 
		   */
	  protected OutputStream createFinalSpaceStream(Path spaceFilePath)
		      throws IOException {
		    OutputStream spaceStream;
		    boolean isCompressed = jobConf != null && FileOutputFormat.getCompressOutput(jobConf);
		    
		    if (!isCompressed) {
		      // Create new file
		    	spaceStream = fileSystem.create(spaceFilePath, true,
		          fileSystem.getConf().getInt("io.file.buffer.size", 4096),
		          fileSystem.getDefaultReplication(), this.blockSize);
		    } else {
		      Class<? extends CompressionCodec> codecClass =
		          FileOutputFormat.getOutputCompressorClass(jobConf, GzipCodec.class);
		      // create the named codec
		      CompressionCodec codec = ReflectionUtils.newInstance(codecClass, jobConf);

		      // Open a stream to the output file
		      spaceStream = fileSystem.create(spaceFilePath, true,
		          fileSystem.getConf().getInt("io.file.buffer.size", 4096),
		          fileSystem.getDefaultReplication(), this.blockSize);

		      // Encode the output stream using the codec
		      spaceStream = new DataOutputStream(codec.createOutputStream(spaceStream));
		    }

		    return spaceStream;
		}
		  
		  
		  /**
		   * Closes (or initiates a close command) for the cell with the given index.
		   * Once this method returns, it should be safe to reuse the same cell index
		   * to write more data in a new file.
		   * @param cellIndex
		   * @throws IOException
		   */
	  protected void closeSpace(int spaceIndex) throws IOException {
		  //try{
		    SpaceInfo space = spaces != null? spaces[spaceIndex] : new SpaceInfo(spaceIndex+1, spacesMbc[spaceIndex]);
		    if(space != null) {
		    	System.out.println("before expand "+ space.toString());
		    //	return;
		    }
		    System.out.println("expand: " + expand);
		    System.out.println("pack :" +pack);
		    if (expand)
		      space.expand(spacesMbc[spaceIndex]);
			System.out.println("after expand "+ space.toString());
		    if (pack)
		    	try{
		    		space = new SpaceInfo(space.spaceId, space.getIntersection(spacesMbc[spaceIndex]));
		    	}catch(NullPointerException exception) {
		    		exception.printStackTrace();
					System.out.println("after pack "+ space.toString());
		    	}
		    		
		    closeSpaceBackground(intermediateSpacePath[spaceIndex],
		    		getFinalSpacePath(spaceIndex), intermediateSpaceStreams[spaceIndex],
		        masterFile, space, intermediateSpaceRecordCount[spaceIndex], intermediateSpacesize[spaceIndex]);
		    spacesMbc[spaceIndex] = new Cubic(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE,
		        -Double.MAX_VALUE, -Double.MAX_VALUE, -Double.MAX_VALUE);
		    intermediateSpacePath[spaceIndex] = null;
		    intermediateSpaceStreams[spaceIndex] = null;
		    intermediateSpaceRecordCount[spaceIndex] = 0;
		    intermediateSpacesize[spaceIndex] = 0; 
		  //}catch(NullPointerException e) {
			//  System.out.println("------------------>>>>>>>>>>>>>>>>>>>>>>");
		 // }
	  }
		  
		  /**
		   * Close the given space freeing all memory reserved by it.
		   * Once a sub space is closed, we should not write more data to it.
		   * @param cellInfo
		   * @throws IOException
		   */
		  
	  protected void closeSpaceBackground(final Path intermediateSpacePath,
			      final Path finalSpacePath, final OutputStream intermediateSpaceStream,
			      final OutputStream masterFile, final SpaceInfo spaceMbc,
			      final long recordCount, final long cellSize) throws IOException {
			    
		    Thread closingThread = new Thread() {
		      @Override
		      public void run() {
		        try {
		          Path finalfinalSpacePath = flushAllEntries(intermediateSpacePath,
		        		  intermediateSpaceStream, finalSpacePath);
		          // Write an entry to the master file

		          // Write a line to the master file including file name and cellInfo
		          if (masterFile != null) {
		            Partition partition = new Partition(finalfinalSpacePath.getName(), spaceMbc);
		            partition.recordCount = recordCount;
		            partition.size = cellSize;
		            Text line = partition.toText(new Text());
		            masterFile.write(line.getBytes(), 0, line.getLength());
		            masterFile.write(NEW_LINE);
		          }
		        } catch (IOException e) {
		          throw new RuntimeException("Error closing thread", e);
		        }
		      }
		    };
			    
		    closingThreads.add(closingThread);
		    // Remove previously terminated threads
		    while (!closingThreads.isEmpty() &&
		        closingThreads.get(0).getState() == Thread.State.TERMINATED) {
		      closingThreads.remove(0);
		    }
		    // Start first thread (if exists)
		    if (!closingThreads.isEmpty() && closingThreads.get(0).getState() == Thread.State.NEW)
		      closingThreads.get(0).start();
	  }
		  
		  /**
		   * Returns path to a file in which the final space will be written.
		   * @param column
		   * @param row
		   * @return
		   * @throws IOException 
		   */
	  protected Path getFinalSpacePath(int spaceIndex) throws IOException {
		    Path path = null;
		    do {
		      String filename = counter == 0 ? String.format("data_%05d", spaceIndex)
		          : String.format("data_%05d_%d", spaceIndex, counter);
		      boolean isCompressed = jobConf != null && FileOutputFormat.getCompressOutput(jobConf);
		      if (isCompressed) {
		        Class<? extends CompressionCodec> codecClass =
		            FileOutputFormat.getOutputCompressorClass(jobConf, GzipCodec.class);
		        // create the named codec
		        CompressionCodec codec = ReflectionUtils.newInstance(codecClass, jobConf);
		        filename += codec.getDefaultExtension();
		      }
		      
		      path = getFilePath(filename);
		      counter++;
		    } while (fileSystem.exists(path));
		    return path;
	  }
		  
		  
      protected Path flushAllEntries(Path intermediateSpacePath,
			      OutputStream intermediateSpaceStream, Path finalSpacePath) throws IOException {
			 // For global-only indexed file, the intermediate file is the final file
			 intermediateSpaceStream.close();
			 return intermediateSpacePath;
      }
		  
		 /**
		   * Close the whole writer. Finalize all cell files and concatenate them
		   * into the output file.
		   */
      public synchronized void close(Progressable progressable) throws IOException {
		    // Close all output files
		    for (int spaceIndex = 0; spaceIndex < intermediateSpaceStreams.length; spaceIndex++) {
		      if (intermediateSpaceStreams[spaceIndex] != null) {
		        closeSpace(spaceIndex);
		      }
		      // Indicate progress. Useful if closing a single cell takes a long time
		      if (progressable != null)
		        progressable.progress();
		    }

		    while (!closingThreads.isEmpty()) {
		      try {
		        Thread t = closingThreads.get(0);
		        switch (t.getState()) {
		        case NEW: t.start(); break;
		        case TERMINATED: closingThreads.remove(0); break;
		        default:
		          // Use limited time join to indicate progress frequently
		          t.join(10000);
		        }
		        // Indicate progress. Useful if closing a single cell takes a long time
		        if (progressable != null)
		          progressable.progress();
		      } catch (InterruptedException e) {
		        e.printStackTrace();
		      }
		    }
		    
		    if (masterFile != null)
		      masterFile.close();
		  }

}
