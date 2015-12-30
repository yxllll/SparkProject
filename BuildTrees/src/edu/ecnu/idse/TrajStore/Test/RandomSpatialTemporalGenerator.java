package edu.ecnu.idse.TrajStore.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.ecnu.idse.TrajStore.core.Cubic;
import edu.ecnu.idse.TrajStore.core.CubicInfo;
import edu.ecnu.idse.TrajStore.core.Shape;
import edu.ecnu.idse.TrajStore.core.SpaceInfo;
import edu.ecnu.idse.TrajStore.core.SpatialTemporalSite;
import edu.ecnu.idse.TrajStore.mapred.CubicOutputFormat;
import edu.ecnu.idse.TrajStore.mapred.CubicRecordWriter;
import edu.ecnu.idse.TrajStore.mapred.RandomInputFormat;
import edu.ecnu.idse.TrajStore.util.OperationsParams;
import edu.ecnu.idse.TrajStore.operations.Repartition;

public class RandomSpatialTemporalGenerator extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		ToolRunner.run(new RandomSpatialTemporalGenerator(), args);

	}

	public int run(String [] args) throws Exception{
		OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
		if(params.get("mbc") == null){
			System.err.println("Set MBC of the generated file using cubic:<x1,y1,z1,x2,y2,z2>");
			printUsage();
			System.exit(1);
		}
		System.out.println(params.get("mbc"));
		if(params.get("shape") == null){
			System.err.println("Shape should be specified");
			printUsage();
			System.exit(1);
		}
		System.out.println(params.get("shape"));
	
		
		Path outputFile = params.getPath();
		System.err.println(outputFile.getName());
		
		if (!params.checkOutput()) {
		      printUsage();
		      System.exit(1);
		}  
		    
	    long totalSize = params.getSize("size");
	    System.out.println("total size: " +totalSize);
	    generateMapReduce(outputFile, params);
		return 1;
	}
	
	private static void generateMapReduce(Path outFile, OperationsParams params)
		      throws IOException{
		JobConf job = new JobConf(params, RandomSpatialTemporalGenerator.class);
	    job.setJobName("Generator");
	    Shape shape = params.getShape("shape");    
	    FileSystem outFs = outFile.getFileSystem(job);
	    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
	    job.setInputFormat(RandomInputFormat.class);
	    job.setMapperClass(Repartition.RepartitionMap.class);
	    
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(shape.getClass());
	    job.setNumMapTasks(10 * Math.max(1, clusterStatus.getMaxMapTasks()));
	
	    String sindex = params.get("sindex");
	    System.out.println("sindex :" +sindex);
	    Cubic mbc = new Cubic();
	    mbc.set(params.get("mbc"));
	    
	    SpaceInfo[] cubics;
	    if(sindex == null){
	    	cubics = new SpaceInfo[]{new SpaceInfo(1,mbc)};
	    }else if(sindex.equals("grid")){
	    	//整个网格空间的大小
	    	  CubicInfo cubicInfo = new CubicInfo(mbc.x1, mbc.y1, mbc.z1, mbc.x2, mbc.y2, mbc.z2);
			  FileSystem fs = outFile.getFileSystem(job);
		      long blocksize = fs.getDefaultBlockSize();
		      System.out.println("block size :" +blocksize/(1024*1024)+"Mb");
		      // 1g
		      long size = params.getSize("size");
		      // 计算所需分区数 n=[S(1+arfa)/B]
		      int numOfCubics = Repartition.calculateNumberOfPartitions(job, size, fs, outFile, blocksize);
		      System.out.println("number of cubics :"+numOfCubics);
		      // 根据分区数，计算将空间划分成实际的 a*b*c份
		      cubicInfo.calculateCubicDimensions(numOfCubics);
		      cubics = cubicInfo.getAllSpaces();      
		}else {
			throw new RuntimeException("UNEXPECTED SPATIAL-TEMPORTAL INDEX"+sindex);
		}
	    
	    //将输出空间的划分情况放入到全局变量中
	    try{
	    	SpatialTemporalSite.setSpaces(job, cubics);
	    }catch(Exception e){
	    	System.out.println("lalala");
	    }
	    
	    if(cubics.length == 1){
	    	job.setNumReduceTasks(1);
	    }else {
			job.setReducerClass(Repartition.RepartitionReduce.class);
			job.setNumReduceTasks(Math.max(1, Math.min(cubics.length,
			          (clusterStatus.getMaxReduceTasks() * 9 + 5) / 10)));
		}
	
	    FileOutputFormat.setOutputPath(job, outFile);
	    if (sindex == null || sindex.equals("grid")) {
	      job.setOutputFormat(CubicOutputFormat.class); //输出方式
	    } else {
	      throw new RuntimeException("Unsupported spatial index: "+sindex);
	    }
	    
	    JobClient.runJob(job);
	    
	    FileStatus[] resultFiles = outFs.listStatus(outFile, new PathFilter() {
	        @Override
	        public boolean accept(Path path) {
	          return path.getName().contains("_master");
	        }
	    });
	    
	    String ext = resultFiles[0].getPath().getName()
	            .substring(resultFiles[0].getPath().getName().lastIndexOf('.'));
	    Path masterPath = new Path(outFile, "_master" + ext);
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
	  
	  
	  private static void printUsage() {
		    System.out.println("*******************************");
		    System.out.println("Generates a file with random shapes");
		    System.out.println("Parameters (* marks required parameters):");
		    System.out.println("<output file> - Path to the file to generate. If omitted, file is generated to stdout.");
		    System.out.println("mbc:<x1,y1,z1,x2,y2,z2> - (*) The MBC of the generated data. Originated at (x,y,z) with dimensions (w,h,d)");
		    System.out.println("shape:<point|(rectangle)|cubic> - Type of shapes in generated file");
		    System.out.println("blocksize:<size> - Block size in the generated file");
		    System.out.println("sindex:<grid> - Type of global index in generated file");
		    System.out.println("seed:<s> - Use a specific seed to generate the file");
		    System.out.println("rectsize:<rs> - Maximum edge size for generated rectangles");
		    System.out.println("-overwrite - Overwrite output file without notice");
		    GenericOptionsParser.printGenericCommandUsage(System.out);
		    System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
		  }
}
