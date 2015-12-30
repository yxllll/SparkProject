package edu.ecnu.idse.TrajStore.core;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import edu.ecnu.idse.TrajStore.mapred.ShapeIterRecordReader;
import edu.ecnu.idse.TrajStore.mapred.SpatialTemporalRecordReader.ShapeIterator;
import edu.ecnu.idse.TrajStore.util.OperationsParams;

public class SpatialTemporalSite {
	 private static final Log LOG = LogFactory.getLog(SpatialTemporalSite.class);
	 
	 public static final String OUTPUT_SPACES = "edu.ecnu.idse.TrajStore.mapReduce.GridOutputFormat.SpacesInfo";
	 
	  /**The class used to filter blocks before starting map tasks*/
	  public static final String FilterClass = "spatialHadoop.mapreduce.filter";
	 
	  
	 /**Whether to build the RTree in fast mode or slow (memory saving) mode.*/
	  public static final String RTREE_BUILD_MODE =
	      "spatialHadoop.storage.RTreeBuildMode";
	  
	  /**Configuration line name for replication overhead*/
	  public static final String INDEXING_OVERHEAD =
	      "spatialHadoop.storage.IndexingOverhead";
	  
	  /**Ratio of the sample to read from files to build a global R-tree*/
	  public static final String SAMPLE_RATIO = "spatialHadoop.storage.SampleRatio";
	  
	  /**Ratio of the sample to read from files to build a global R-tree*/
	  public static final String SAMPLE_SIZE = "spatialHadoop.storage.SampleSize";
	  
	  public static final long RTreeFileMarker = -0x00012345678910L;
	  /**
	   * Maximum number of shapes to read in one read operation and return when
	   * reading a file as array
	   */
	  public static final String MaxShapesInOneRead =
	      "spatialHadoop.mapred.MaxShapesPerRead";

	  /**
	   * Maximum size in bytes that can be read in one read operation
	   */
	  public static final String MaxBytesInOneRead =
	      "spatialHadoop.mapred.MaxBytesPerRead";

	  public static byte[] RTreeFileMarkerB;
	  
	  public static final PathFilter NonHiddenFileFilter = new PathFilter(){
		    public boolean accept(Path p){
		      String name = p.getName(); 
		      return !name.startsWith("_") && !name.startsWith("."); 
		    }
		  };
	  /**
	   * Set an array of cells in the job configuration. As the array might be
	   * very large to store as one value, an alternative approach is used.
	   * The cubics are all written to a temporary file, and that file is added
	   * to the DistributedCache of the job. Later on, a call to
	   * {@link #getCells(JobConf)} will open the corresponding file from
	   * DistributedCache and parse cells from that file.
	   * @param conf
	   * @param cellsInfo
	   * @throws IOException
	   */
	  public static void setSpaces(Configuration conf, SpaceInfo[] spacesInfo) throws IOException {
	    Path tempFile;
	    FileSystem fs = FileSystem.get(conf);
	    do {
	      tempFile = new Path("cubic_info.cubics");
	    } while (fs.exists(tempFile));
	    FSDataOutputStream out = fs.create(tempFile);
	    /// total spaces number
	    out.writeInt(spacesInfo.length);
	    for (SpaceInfo space : spacesInfo) {
	      space.write(out);
	    }
	    out.close();
	    
	    fs.deleteOnExit(tempFile);

	    DistributedCache.addCacheFile(tempFile.toUri(), conf);
	    conf.set(OUTPUT_SPACES, tempFile.getName());
	    LOG.info("Partitioning file into "+spacesInfo.length+" sub-spaces");
	  }
	  
	  public static SpaceInfo[] getSpaces(Configuration conf) throws IOException {
		  SpaceInfo[] spaces = null;
		    String spaces_file = conf.get(OUTPUT_SPACES);
		    if (spaces_file != null) {
		      Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
		      for (Path cacheFile : cacheFiles) {
		        if (cacheFile.getName().contains(spaces_file)) {
		          FSDataInputStream in = FileSystem.getLocal(conf).open(cacheFile);
		          //  read numbers firstly
		          int spaceCount = in.readInt();
		          spaces = new SpaceInfo[spaceCount];
		          for (int i = 0; i < spaceCount; i++) {
		        	  spaces[i] = new SpaceInfo();
		        	  spaces[i].readFields(in);
		          }
		          
		          in.close();
		        }
		      }
		    }
		   return spaces;
	  }
	  
	  public static GlobalIndex<Partition> getGlobalIndex(FileSystem fs,
		      Path dir) {
		  System.out.println("spatial temporal path :"+dir.getName());
			  try {
					      if (!fs.getFileStatus(dir).isDir())
					    	  return null;
					      FileStatus[] allFiles;
					      if (OperationsParams.isWildcard(dir)) {
					    	  allFiles = fs.globStatus(dir);
					      } else {
					    	  allFiles = fs.listStatus(dir);
					      }
					      
					      FileStatus masterFile = null;
					      int taxiFiles = 0;
					      for (FileStatus fileStatus : allFiles) {
					    	  if (fileStatus.getPath().getName().startsWith("_master")) {
					    		  	if (masterFile != null)
					    		  		throw new RuntimeException("Found more than one master file in "+dir);
					    		  	masterFile = fileStatus;
					        } else if (fileStatus.getPath().getName().toLowerCase().matches(".*h\\d\\dv\\d\\d.*\\.(hdf|jpg|xml)")) {
					          // TODOHandle on-the-fly global indexes imposed from file naming of NASA data
					        			}
					      }
					      if(masterFile != null){
					    	  ShapeIterRecordReader reader = new ShapeIterRecordReader(fs.open(masterFile.getPath()), 0, masterFile.getLen());
					    	  Cubic dummy = reader.createKey();
					    	  reader.setShape(new Partition());
					    	  ShapeIterator values = reader.createValue();
					    	  ArrayList<Partition> partitions = new ArrayList<Partition>();
					    	  while (reader.next(dummy, values)) {
								for(Shape value: values){
									partitions.add((Partition)value.clone());
								}								
							}
					    	  GlobalIndex< Partition> globalIndex = new GlobalIndex<Partition>();
					    	  globalIndex.bulkLoad(partitions.toArray(new Partition[partitions.size()]));
					          String extension = masterFile.getPath().getName();
					          extension = extension.substring(extension.lastIndexOf('.') + 1);
					          globalIndex.setCompact(CubicRecordWriter.PackedIndexes.contains(extension));
					          globalIndex.setReplicated(CubicRecordWriter.ReplicatedIndexes.contains(extension));
					          return globalIndex;    	  
					      }else{
					    	  System.out.println("master file is null !!!!");
					    	  return null;
					      }
			  }catch (IOException e){
				  LOG.info("error retrieving global index of " + dir + "");
				  LOG.info(e);
				  return null;
			  }
	  }
	  
	  public static Shape createStockShape(Configuration job){
		  return OperationsParams.getShape(job, "shape");
	  }
}
