package edu.ecnu.idse.TrajStore.operations;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.ecnu.idse.TrajStore.core.Cubic;
import edu.ecnu.idse.TrajStore.core.CubicInfo;
import edu.ecnu.idse.TrajStore.core.Shape;
import edu.ecnu.idse.TrajStore.core.SpaceInfo;
import edu.ecnu.idse.TrajStore.core.SpatialTemporalSite;
import edu.ecnu.idse.TrajStore.mapred.CubicOutputFormat;
import edu.ecnu.idse.TrajStore.util.OperationsParams;

public class Repartition {
	  static final Log LOG = LogFactory.getLog(Repartition.class);
	  
	  /**
	   * The map class maps each object to every space it overlaps with.
	   * @author zzg
	   *
	   */
	  public static class RepartitionMap<T extends Shape> extends MapReduceBase
	      implements Mapper<Cubic, T, IntWritable, T> {
	    /**List of sub-spaces used by the mapper*/
	    private SpaceInfo[] spaceInfos;
	    
	    /**Used to output intermediate records*/
	    private IntWritable spaceId = new IntWritable();
	    
	    @Override
	    public void configure(JobConf job) {
	      try {
	        spaceInfos = SpatialTemporalSite.getSpaces(job);
	        super.configure(job);
	      } catch (IOException e) {
	        e.printStackTrace();
	      }
	    }
	    
	    /**
	     * Map function
	     * 将shape发送到对应空间中期
	     * @param dummy
	     * @param shape
	     * @param output
	     * @param reporter
	     * @throws IOException
	     */
	    public void map(Cubic spaceMbc, T shape,
	        OutputCollector<IntWritable, T> output, Reporter reporter)
	        throws IOException {
	      Cubic shape_mbc = shape.getMBC();
	      if (shape_mbc == null)
	        return;
	      // Only send shape to output if its lowest corner lies in the spaceMbc
	      // This ensures that a replicated shape in an already partitioned file
	      // doesn't get send to output from all partitions
	      if (!spaceMbc.isValid() || spaceMbc.contains(shape_mbc.x1, shape_mbc.y1, shape_mbc.z1)) {
	        for (int spaceIndex = 0; spaceIndex < spaceInfos.length; spaceIndex++) {
	          if (spaceInfos[spaceIndex].isIntersected(shape_mbc)) {
	        	  if(spaceInfos[spaceIndex].spaceId < 0){
	        		  System.out.println("space id exception!");
	        	  }
	        	  spaceId.set((int) spaceInfos[spaceIndex].spaceId);
	        	  output.collect(spaceId, shape);
	          }
	        }
	      }
	    }
	    
	  }
	  
	  /**
	   * The map class maps each object to the cell with maximum overlap.
	   * @author 章志刚
	   *
	   */
	  public static class RepartitionMapNoReplication<T extends Shape> extends MapReduceBase
	      implements Mapper<Cubic, T, IntWritable, T> {
	    /**List of cells used by the mapper*/
		
		private SpaceInfo[] spaceInfos;
		    
	   /**Used to output intermediate records*/
		private IntWritable spaceId = new IntWritable();
	    
	    @Override
	    public void configure(JobConf job) {
	      try {
	    	  spaceInfos = SpatialTemporalSite.getSpaces(job);
	        super.configure(job);
	      } catch (IOException e) {
	        throw new RuntimeException("Error loading spaces", e);
	      }
	    }
	    
	    /**
	     * Map function
	     * 将shape发送到对应空间中期
	     * @param dummy
	     * @param shape
	     * @param output
	     * @param reporter
	     * @throws IOException
	     */
	    public void map(Cubic spaceMbc, T shape,
	        OutputCollector<IntWritable, T> output, Reporter reporter)
	        throws IOException {
	      Cubic shape_mbc = shape.getMBC();
		  if (shape_mbc == null)
		      return;
	      double maxOverlap = -1.0;
	      int bestCubic = -1;
	      // Only send shape to output if its lowest corner lies in the cellMBR
	      // This ensures that a replicated shape in an already partitioned file
	      // doesn't get send to output from all partitions
	      if (!spaceMbc.isValid() || spaceMbc.contains(shape_mbc.x1, shape_mbc.y1, shape_mbc.z1)) {
	        for (int spaceIndex = 0; spaceIndex < spaceInfos.length; spaceIndex++) {
	          Cubic overlap = spaceInfos[spaceIndex].getIntersection(shape_mbc);
	          if (overlap != null) {
	            double overlapVolume = overlap.getWidth() * overlap.getHeight() *overlap.getDeepth();
	            if (bestCubic == -1 || overlapVolume > maxOverlap) {
	              maxOverlap = overlapVolume;
	              bestCubic = spaceIndex;
	            }
	          }
	        }
	      }
	      if (bestCubic != -1) {
	    	  spaceId.set((int) spaceInfos[bestCubic].spaceId);
	        output.collect(spaceId, shape);
	      } else {
	        LOG.warn("Shape: "+shape+" doesn't overlap any partitions");
	      }
	    }
	  }
	  
	  public static class RepartitionReduce<T extends Shape> extends MapReduceBase
	  implements Reducer<IntWritable, T, IntWritable, T> {

	    @Override
	    public void reduce(IntWritable spaceIndex, Iterator<T> shapes,
	        OutputCollector<IntWritable, T> output, Reporter reporter)
	        throws IOException {
	      T shape = null;
	      while (shapes.hasNext()) {
	        shape = shapes.next();
	        output.collect(spaceIndex, shape);
	      }
	      // Close space
	      output.collect(new IntWritable(-spaceIndex.get()), shape);
	    }
	    
	  }
	  
	  public static class RepartitionOutputCommitter extends FileOutputCommitter {
	      @Override
	      public void commitJob(JobContext context) throws IOException {
	        super.commitJob(context);
	        
	        JobConf job = context.getJobConf();
	        Path outPath = CubicOutputFormat.getOutputPath(job);
	        FileSystem outFs = outPath.getFileSystem(job);
	  
	        // Concatenate all master files into one file
	        FileStatus[] resultFiles = outFs.listStatus(outPath, new PathFilter() {
	          @Override
	          public boolean accept(Path path) {
	            return path.getName().contains("_master");
	          }
	        });
	        
	        if (resultFiles.length == 0) {
	          LOG.warn("No _master files were written by reducers");
	        } else {
	          String ext = resultFiles[0].getPath().getName()
	                .substring(resultFiles[0].getPath().getName().lastIndexOf('.'));
	          Path masterPath = new Path(outPath, "_master" + ext);
	          OutputStream destOut = outFs.create(masterPath);
	          byte[] buffer = new byte[4096];
	          for (FileStatus f : resultFiles) {
	            InputStream in = outFs.open(f.getPath());
	            int bytes_read;
	            do {
	              bytes_read = in.read(buffer);
	              if (bytes_read > 0)
	                destOut.write(buffer, 0, bytes_read);
	            } while (bytes_read > 0);
	            in.close();
	            outFs.delete(f.getPath(), false);
	          }
	          destOut.close();
	        }
	        
	        // Plot an image for the partitions used in file
	  /*      
	        CellInfo[] cellInfos = SpatialSite.getCells(job);
	        Path imagePath = new Path(outPath, "_partitions.png");
	        int imageSize = (int) (Math.sqrt(cellInfos.length) * 300);
	        Plot.plotLocal(masterPath, imagePath, new Partition(), imageSize, imageSize, Color.BLACK, false, false, false);
	  */      
	      }
	  }

	  /**
	   * Calculates number of partitions required to index the given file
	   * @param inFs
	   * @param inFile
	   * @param rtree
	   * @return
	   * @throws IOException 
	   */
	 @SuppressWarnings("deprecation")
	 public static int calculateNumberOfPartitions(Configuration conf, long inFileSize,
	      FileSystem outFs, Path outFile, long blockSize) throws IOException {
	    final float IndexingOverhead =
	        conf.getFloat(SpatialTemporalSite.INDEXING_OVERHEAD, 0.1f);
	    long indexedFileSize = (long) (inFileSize * (1 + IndexingOverhead));
	    if (blockSize == 0)
	    	blockSize = outFs.getDefaultBlockSize();
	    return (int)Math.ceil((float)indexedFileSize / blockSize);
	 }

}
