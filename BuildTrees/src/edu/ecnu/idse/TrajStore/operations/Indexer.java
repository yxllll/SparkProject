package edu.ecnu.idse.TrajStore.operations;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;

import edu.ecnu.idse.TrajStore.core.Cubic;
import edu.ecnu.idse.TrajStore.core.GridPartitioner;
import edu.ecnu.idse.TrajStore.core.Partition;
import edu.ecnu.idse.TrajStore.core.Partitioner;
import edu.ecnu.idse.TrajStore.core.Point;
import edu.ecnu.idse.TrajStore.core.Rectangle;
import edu.ecnu.idse.TrajStore.core.ResultCollector;
import edu.ecnu.idse.TrajStore.core.Shape;
import edu.ecnu.idse.TrajStore.core.SpatialTemporalSite;
import edu.ecnu.idse.TrajStore.mapred.CubicOutputFormat;
import edu.ecnu.idse.TrajStore.mapred.IndexOutputFormat;
import edu.ecnu.idse.TrajStore.mapred.ShapeInputFormat;
import edu.ecnu.idse.TrajStore.util.FileUtil;
import edu.ecnu.idse.TrajStore.util.OperationsParams;

public class Indexer {

	private static final Log LOG = LogFactory.getLog(Indexer.class);
	private static final Map<String, Class<? extends Partitioner>> PartitionerClasses;
	private static final Map<String, Boolean> PartitionerReplicate;
	
	static{
		PartitionerClasses = new HashMap<String, Class<? extends Partitioner>>();
		PartitionerClasses.put("gird", GridPartitioner.class);
		
		
		PartitionerReplicate = new HashMap<String,Boolean>();
	}
	
	/*
	 * The map and reduce functions for the repartition
	 * @ZZG
	 */
	
	
	public static class IndexMethods extends MapReduceBase
		implements Mapper<Rectangle, Iterable<? extends Shape>, IntWritable, Shape>,    /************ Rectangle may need to fix*/
		Reducer<IntWritable, Shape, IntWritable, Shape>{
		/*
		 * The partitioner used to partitioner the data across reducers
		 */
		
		private Partitioner partitioner;
		
		/**
		 * Wheter to replicate a record to all overlapping partitions or to assign it to only one partition
		 */
		private boolean replicate;
		
		@Override
		public void configure(JobConf job){
		//	super(job);
			this.partitioner = Partitioner.getPartitioner(job);
			this.replicate = job.getBoolean("replicate",false);
		}
		
		@Override
		public void map(Rectangle dummy,Iterable<?extends Shape> shapes,
				final OutputCollector<IntWritable, Shape> output, Reporter reporter)
				throws IOException{
			final IntWritable partitionID = new IntWritable();
			int i = 0;
			for(final Shape shape :shapes){
				if(replicate){
					partitioner.overlapPartitions(shape, new ResultCollector<Integer>() {
						@Override
						public void collect(Integer r) {
							partitionID.set(r);
							try {
								output.collect(partitionID, shape);
							} catch (Exception e) {
								LOG.warn("Error checking overlapping partitions",e);
							}
						}
					});
				}else {
					partitionID.set(partitioner.overlapPartition(shape)); //find the overlap partition with shape
					if(partitionID.get() >=0){
						output.collect(partitionID, shape);
					}
				}///report after a few inputs
				if(((++i) & 0xff) == 0){
					reporter.progress();
				}
			}
		}
	
		@Override
		public void reduce(IntWritable partitionID, Iterator<Shape> shapes,
					OutputCollector<IntWritable, Shape> output, Reporter reporter)
				throws IOException{
			while(shapes.hasNext()){
				output.collect(partitionID, shapes.next());
			}
			// indicate end of partition to close the file
			partitionID.set(-(partitionID.get() + 1));
			output.collect(partitionID, null);
		}
	}
	
	
	 public static class IndexerOutputCommitter extends FileOutputCommitter {
		 
		 //For committing job's output after successful job completion.
		 @Override
		 public void commitJob(JobContext context) throws IOException{
			 super.commitJob(context);
			 JobConf job = context.getJobConf();
			 Path outPath = CubicOutputFormat.getOutputPath(job);
			 FileSystem outFs = outPath.getFileSystem(job);
			 
			 FileStatus [] resultFiles = outFs.listStatus(outPath,new PathFilter() {
				
				@Override
				public boolean accept(Path path) {
					// TODO Auto-generated method stub
					return path.getName().contains("_master");
				}
			});
			 
			 if(resultFiles.length == 0){
				 LOG.warn("No _master files were written by reducers");
			 }else {
				String sIndex = job.get("sindex");
				// create the master information
				Path masterPath = new Path(outPath,"_master."+sIndex);
				OutputStream destOut = outFs.create(masterPath);
				Path wktPath = new Path(outPath,"_"+sIndex+".wkt");
				/* we haven't use the wkt
				PrintStream wktOut = new PrintStream(outFs.create(wktPath));
				wktOut.print("ID\tBoundaries\tRecord Count\tSize\tFile name");
				*/
				Text tempLine = new Text();
				Partition tempPartition = new Partition();
				final byte[] NewLine = new byte[]{'\n'};
				for(FileStatus f: resultFiles){
					LineReader in = new LineReader(outFs.open(f.getPath())); // outFs directory
					while(in.readLine(tempLine) > 0){
						destOut.write(tempLine.getBytes(),0,tempLine.getLength());
						destOut.write(NewLine);
						tempPartition.fromText(tempLine);
//						wktOut.println(tempPartition.toWKT());
					}
					in.close();
					outFs.delete(f.getPath(), false);
				}
				destOut.close();
			}
		 }
		 
		 
	 }
	/**
	 * Output committer that concatenates all master files into one master file
	 * @ZZG
	 */
	
	
	private  static RunningJob indexMapReduce(Path inPath, Path outPath,
			OperationsParams params) throws IOException{
		JobConf job = new JobConf(params,Indexer.class);
		job.setJobName("Indexer");
		// set input file MBC if not already set
		
		// job about static MBC of Rectangle
		//:0,0,0,1000000,1000000,1000000 
		Cubic inputMBC = (Cubic)params.getShape("mbc");
		
		//set input and output
		job.setInputFormat(ShapeInputFormat.class);
		ShapeInputFormat.setInputPaths(job, inPath);
		job.setOutputFormat(IndexOutputFormat.class);
		
		
		// set the correct partitioner according to index type
		String index = job.get("sindex");
		if(index == null){
			System.out.println("Index type is not set");
			throw new RuntimeException("Sindex is not set") ;
		}
		
		long t1 = System.currentTimeMillis();
		Partitioner partitioner = createPartitioner(inPath,outPath,job,index);
		Partitioner.setPartitioner(job, partitioner);
		long t2 = System.currentTimeMillis();
		System.out.println("Total time for space subdivision in millis: " +(t2- t1));
		
		Shape shape = params.getShape("shape");
		job.setMapperClass(IndexMethods.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(shape.getClass());
		job.setReducerClass(IndexMethods.class);
		job.setOutputCommitter(IndexerOutputCommitter.class);
		ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
		job.setNumMapTasks(5 * Math.max(1, clusterStatus.getMaxMapTasks()));
		job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks()));
		// use multi -threading in case the job is running locally
		job.setInt(LocalJobRunner.LOCAL_MAX_MAPS, Runtime.getRuntime().availableProcessors());
		if(params.getBoolean("background", false)){
			JobClient jc = new JobClient(job);
			return jc.submitJob(job);
 		}else{
 			return JobClient.runJob(job);
 		}
	}
	
	public static RunningJob index(Path inPath, Path outPath,
			OperationsParams params) throws IOException,InterruptedException{
		if(params.get("local") != null){
			//
			System.out.println("index local");
		}else{
			return indexMapReduce(inPath, outPath, params);
		}
		
		
		
		
		
		
		
		
		
		
		return null;//只是为了除错
		
		
		
		
		
		
		
		
		
		
		
	}
	
	private static Partitioner createPartitioner(Path in, Path out,
				JobConf job, String index) throws IOException{
		try {
			Partitioner partitioner = null;
			Class<? extends Partitioner> partitionerClass = PartitionerClasses.get(index.toLowerCase());
			if(partitionerClass == null){
				throw new RuntimeException("Unknown index type "+index +"");
			}
			boolean replicate = PartitionerReplicate.get(index.toLowerCase());
			job.setBoolean("replicate", replicate);
			partitioner = partitionerClass.newInstance();
			
			long t1 = System.currentTimeMillis();
			final Cubic InCubic = (Cubic) OperationsParams.getShape(job, "mbc");
			// determine number of partitions
			long InSize = FileUtil.getPathSize(in.getFileSystem(job), in);
			FileSystem outFS = out.getFileSystem(job);
			long outBlockSize = outFS.getDefaultBlockSize(out);
			int numPartitions = Math.max(1, (int)(InSize / outBlockSize));
			LOG.info("Partitioning the space into "+ numPartitions + " partitions");

			final Vector<Point> sample = new Vector<Point>();
			float sample_ratio = job.getFloat(SpatialTemporalSite.SAMPLE_RATIO, 0.01f);
			long sample_size = job.getLong(SpatialTemporalSite.SAMPLE_SIZE, 100 * 1024 *1024);
			
			LOG.info("Reading a sample of "+ (int)Math.round(sample_ratio * 100)+"%");
			ResultCollector<Point> resultCollector = new ResultCollector<Point>() {
				@Override
				public void collect(Point p){
					sample.add(p);
				}
			};
			OperationsParams params2 = new OperationsParams(job);
			params2.setFloat("ratio", sample_ratio);
			params2.setLong("size", sample_size);
			params2.setClass("outshape", Point.class, Shape.class);
			
			Sampler.sample(new Path[]{in}, resultCollector, params2); //  begin sample and get results
			long t2 = System.currentTimeMillis();
			System.out.println("Total time for sampling in millis: "+(t2 - t1));
			LOG.info("Finished reading a sample of "+ sample.size()+" records");
			// construct a new partitioner from sample
			
			
			
			
			
			
			
			//只是为了除错
			//partitioner.createFromPoints(InCubic, sample.toArray(new Point[sample.size()]), numPartitions); // which kind of index it is used here?
			
			
			
			
			
			
			
			return partitioner;
		} catch (InstantiationException e) {
			e.printStackTrace();
			return null;
		}catch (IllegalAccessException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public static void main(String [] args) throws Exception{
		OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
		if(!params.checkInputOutput(true)){
			System.out.println("params error!");
			return;
		}
		if(params.get("sindex") == null){
			System.out.println("index formatte error");
			return;
		}
		Path inputPath = params.getInputPath();
		Path outPath = params.getOutputPath();
		
		long t1 = System.currentTimeMillis();
		index(inputPath,outPath,params);
		long t2 = System.currentTimeMillis();
		
		
	}
	
	public static boolean isLocal(JobConf job,OperationsParams params){
		if(job.get("local") != null){
			return true;
		}else {
			return false;
		}
	}
}
