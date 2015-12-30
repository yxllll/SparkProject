package edu.ecnu.idse.TrajStore.operations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TextOutputFormat;


import edu.ecnu.idse.TrajStore.core.Cubic;
import edu.ecnu.idse.TrajStore.core.Point;
import edu.ecnu.idse.TrajStore.core.ResultCollector;
import edu.ecnu.idse.TrajStore.core.Shape;
import edu.ecnu.idse.TrajStore.io.Text2;
import edu.ecnu.idse.TrajStore.io.TextSerializable;
import edu.ecnu.idse.TrajStore.mapred.ShapeLineInputFormat;
import edu.ecnu.idse.TrajStore.mapred.ShapeLineRecordReader;
import edu.ecnu.idse.TrajStore.util.OperationsParams;

public class Sampler {

	private static final Log LOG = LogFactory.getLog(Sampler.class);
	
	/**
	 * keeps track of the uncompressed size of the last processed file or directory
	 * 
	 */
	public static long sizeOfLastProcessedFile;
	
	public static class Map extends MapReduceBase implements Mapper<Cubic, Text, IntWritable, Text>{
		/*
		 * Sample Ratio of  records
		 */
		private float sampleRatio;
		private Random random;
		
		private IntWritable key = new IntWritable();
		
		private Shape inShape;
		enum Conversion{ None, ShapeToPoint, ShapeToCubic	};
		Conversion conversion;
		
		@Override
		public void configure(JobConf job){
			sampleRatio = job.getFloat("ratio", 0.01f);
			random = new Random(job.getLong("seed", System.currentTimeMillis()));
			TextSerializable InObj = OperationsParams.getTextSerializable(job, "shape", new Text2());
			TextSerializable OutObj = OperationsParams.getTextSerializable(job, "outshape", new Text2());
			
			if(InObj.getClass() == OutObj.getClass()){
				conversion = conversion.None;
			}else {
				if (InObj instanceof Shape && OutObj instanceof Point) {
					inShape = (Shape) InObj;
					conversion = conversion.ShapeToPoint;
				}else if (InObj instanceof Shape && OutObj instanceof Cubic) {
					inShape = (Shape) InObj;
					conversion = conversion.ShapeToCubic;
				}else if (OutObj instanceof TextSerializable) {
					conversion = Conversion.None;
				}else {
					throw new RuntimeException("Don't know how to convert from:"+ InObj.getClass()+ "to "+ OutObj.getClass());
				}
			}
		}
		
		public void map(Cubic cubic, Text line, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException{
			System.out.println(line.toString());
			if(random.nextFloat() < sampleRatio){
				switch (conversion) {
				case None:
					output.collect(key, line);
					break;
				case ShapeToPoint:
						inShape.fromText(line);
						Cubic mbc = inShape.getMBC();
						if(mbc != null){
							Point center = mbc.getCenterPoint();
							line.clear();
							center.toText(line);
							output.collect(key, line);
						}
						break;
				case ShapeToCubic:
					inShape.fromText(line);
					mbc = inShape.getMBC();
					if(mbc != null){
						line.clear();
						mbc.toText(line);
						output.collect(key, line);
					}
					break;
				}
			}
		}
	}
	
	
	public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, NullWritable,	Text>{
		@Override
		public void reduce(IntWritable dummy, Iterator<Text> values,
					OutputCollector<NullWritable, Text> output, Reporter reporter)
						throws IOException{
			while(values.hasNext()){
				Text x = values.next();
				output.collect(NullWritable.get(), x);
			}
		}
	}
	
	
	public static int sampleWithRatio( Path[] files, final ResultCollector<? extends TextSerializable> output,
			OperationsParams params) throws IOException{
		FileSystem fs= files[0].getFileSystem(params);
		FileStatus inFStatus = fs.getFileStatus(files[0]);
	
		System.out.println("is dir :" + inFStatus.isDir());
		System.out.println("getlen :" + inFStatus.getLen());
		System.out.println("getblocksize" + inFStatus.getBlockSize());
		if(inFStatus.isDir() || inFStatus.getLen() / inFStatus.getBlockSize() > 1){
			System.out.println("sample with mapreduce");
			return sampleMapReduceWithRatio(files, output, params);
		}else{
			System.out.println("sample with local");
			// 这需要进行测试！！！！！ 
			return sampleLocalWithRatio(files, output, params);
		}
	}
	
	
	private static <T extends TextSerializable> int sampleMapReduceWithRatio(
			Path[] files, final ResultCollector<T> output, OperationsParams params)
					throws IOException{
		JobConf job = new JobConf(params,Sampler.class);
		Path outputPath ;
		FileSystem outFS = FileSystem.get(job);
		System.out.println(files[0].toUri().getPath());
		outputPath = new Path(files[0].toUri().getPath() + ".sample" );
		outFS.delete(outputPath, true);
		System.out.println("output  path construct finished");
		
		job.setJobName("Sample");
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
		int mapTaskNum = clusterStatus.getMaxMapTasks() * 5;
		job.setNumMapTasks(mapTaskNum);
		int reduceTaskNum = Math.max(1, (int) Math.round(clusterStatus.getMaxReduceTasks() * 0.9));
		job.setNumReduceTasks(reduceTaskNum);
		
		job.setInputFormat(ShapeLineInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		
	  /*  for (Path file : files) {
		      FileSystem fs = file.getFileSystem(params);
		      if (fs.getFileStatus(file).isDir()) {
		        // Directory, process all data files in this directory (visible files)
		        FileStatus[] fileStatus = fs.listStatus(file,DataFileFilter);
		        for (FileStatus f : fileStatus) {
		//          data_files.add(f.getPath());
		        	System.out.println(f.getPath());
		        	ShapeLineInputFormat.addInputPath(job, f.getPath());
		        }
		      }
		    }*/
	    
		ShapeLineInputFormat.setInputPaths(job, files);
		TextOutputFormat.setOutputPath(job, outputPath);
		
		// Submit the job
		RunningJob run_job = JobClient.runJob(job);
		
		Counters counters = run_job.getCounters();
		Counter outputRecordCounter = counters.findCounter(Task.Counter.MAP_OUTPUT_RECORDS);
		final long resultCount = outputRecordCounter.getValue();
		System.out.println("sampled records num: " + resultCount);
		
		Counter ouputSizeCounter = counters.findCounter(Task.Counter.MAP_OUTPUT_BYTES);
		final long sampleSize = ouputSizeCounter.getValue();
		System.out.println("sampled records size: " + sampleSize);
		
		LOG.info("resultCount : " + resultCount);
		LOG.info("resultSize: "+ sampleSize);
		
		Counter InputBytesCounter = counters.findCounter(Task.Counter.MAP_INPUT_BYTES);
		Sampler.sizeOfLastProcessedFile  = InputBytesCounter.getValue();
		
		// Ratio of records to be return from output based on the threshold
		// Note
		long desiredSampleSize = job.getLong("size", 0);
		float selectRatio = desiredSampleSize <= 0? 2.0f :(float) desiredSampleSize /sampleSize;
		
		int result_size = 0;
		if(selectRatio > 1.0f){ //想要的样本数 超过 实际采样书， 足够多！
			// get all records from the output.
			ShapeLineInputFormat inputFormat = new ShapeLineInputFormat();
			ShapeLineInputFormat.setInputPaths(job, outputPath);
			InputSplit [] splits = inputFormat.getSplits(job, 1);
			for(InputSplit split: splits){
				RecordReader<Cubic, Text> reader = inputFormat.getRecordReader(split, job, null);
				Cubic key = reader.createKey();
				Text value = reader.createValue();
				T oubObj = (T) OperationsParams.getTextSerializable(params, "outshape", new Text2());  ///  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! may be  text2
				while(reader.next(key, value)){
					oubObj.fromText(value);
					output.collect(oubObj);
				}
				reader.close();
			}
		}else {//采样数不够，则需要重新考虑
			if(output != null){
				OperationsParams params2 =new OperationsParams(params);
				params2.setFloat("ratio",selectRatio);
				params2.set("shape", params.get("outshape"));
				if(selectRatio > 0.1){ //想要的量 超过已采样量的百分之十。 
					LOG.info("Local return "+ selectRatio +" of "+resultCount + " records");
					long tempSize = sizeOfLastProcessedFile;
					result_size = sampleLocalWithRatio(files, output, params2);
				}
			}
		
		}
		return 1;
	}
	
	private static <T extends TextSerializable> int sampleLocalWithRatio(
		      Path[] files, final ResultCollector<T> output, OperationsParams params) throws IOException {
		 long total_size = 0;
		    // TODO handle compressed files
		    // TODO Use a global index to determine the exact size quickly
		    for (Path file : files) {
		      FileSystem fs = file.getFileSystem(params);
		      FileStatus fStatus = fs.getFileStatus(file);
		      if (fStatus.isDir()) {
		        // Go one level deeper
		        for (FileStatus subFStatus : fs.listStatus(file)) {
		          if (!subFStatus.isDir())
		            total_size += subFStatus.getLen();
		        }
		      } else {
		        total_size += fStatus.getLen();
		      }
		    }
		    sizeOfLastProcessedFile = total_size;
		    float ratio = params.getFloat("ratio", 0.1f);
		    params.setLong("size", (long) (total_size * ratio));
		    return sampleLocalWithSize(files, output, params);
	}
	
	private static <T extends TextSerializable> int sampleLocalWithSize(Path[] files,
		      final ResultCollector<T> output, OperationsParams params)
		      throws IOException {
		int average_record_size = 1024;
		final LongWritable current_sample_size = new LongWritable();
		int sample_count = 0;
		
		TextSerializable inObj1, outObj1;
		inObj1 = OperationsParams.getTextSerializable(params, "shape", new Text2());
		outObj1 = OperationsParams.getTextSerializable(params, "outshape", new Text2());
		// make the objects to be able  to use
		final TextSerializable inObj = inObj1;
		final T outObj = (T) outObj1;
		final ResultCollector<TextSerializable> converter = createConverter(output, inObj, outObj);
		
		final ResultCollector<Text2> counter = new ResultCollector<Text2>() {
			
			@Override
			public void collect(Text2 r) {
			      current_sample_size.set(current_sample_size.get() + r.getLength());
			        inObj.fromText(r);
			        converter.collect(inObj);	
			}
		};
		
		System.out.println(inObj.getClass().toString());
		System.out.println(outObj.getClass().toString());
		
		long total_size = params.getLong("size", 0);
		System.out.println("total sample size "+total_size);
		long seed = params.getLong("seed", System.currentTimeMillis());
		System.out.println("initial current_sample_size :" + current_sample_size.get());
		while(current_sample_size.get() < total_size){
			  int count = (int) ((total_size - current_sample_size.get()) / average_record_size);  // divide by  one  KB
		      if (count < 10)
		        count = 10;
		      System.out.println("count:" + count);
		      OperationsParams params2 = new OperationsParams(params);
		      params2.setClass("shape", Text2.class, TextSerializable.class);
		      params2.setClass("outshape", Text2.class, TextSerializable.class);
		      params2.setInt("count", count);
		      params.setLong("seed", seed);
		      sample_count += sampleLocalByCount(files, counter, params2);
		      seed += sample_count;
		      average_record_size= (int) (current_sample_size.get() / sample_count);
		}
		
		return sample_count;
	}
	
	 private static <T extends TextSerializable> int sampleLocalByCount(Path[] files,
		      ResultCollector<T> output, OperationsParams params) throws IOException {

		    ArrayList<Path> data_files = new ArrayList<Path>();
		    for (Path file : files) {
		      FileSystem fs = file.getFileSystem(params);
		      if (fs.getFileStatus(file).isDir()) {
		        // Directory, process all data files in this directory (visible files)
		        FileStatus[] fileStatus = fs.listStatus(file, hiddenFileFilter);
		        for (FileStatus f : fileStatus) {
		          data_files.add(f.getPath());
		        }
		      } else {
		        // File, process this file
		        data_files.add(file);
		      }
		    }
		    
		    files = data_files.toArray(new Path[data_files.size()]);
		    
		    TextSerializable inObj1, outObj1;
		    inObj1 = OperationsParams.getTextSerializable(params, "shape", new Text2());
		    outObj1 = OperationsParams.getTextSerializable(params, "outshape", new Text2());
		  	    
		    final TextSerializable inobj = inObj1;
		    final T outobj = (T) outObj1;
		    
		    ResultCollector<TextSerializable> converter = createConverter(output, inobj, outobj);
		    long[] files_start_offset = new long[files.length+1];
		    long total_length = 0;
		    for(int i_file = 0; i_file < files.length;i_file++){
		    	FileSystem fs = files[i_file].getFileSystem(params);
		    	files_start_offset[i_file] = total_length;
		    	total_length += fs.getFileStatus(files[i_file]).getLen();
		    }
		    files_start_offset[files.length] = total_length;
		    System.out.println("files total size "+files_start_offset[files.length]);
		    // generate offsets to read from and make sure they are ordered to minimize 
		    //seeks between different hdfs blocks
		    Random random = new Random(params.getLong("seed", System.currentTimeMillis()));
		    long[] offsets =new long[params.getInt("count", 0)];
		    for(int i=0;i<offsets.length;i++){
		    	if(total_length == 0)
		    		offsets[i]=0;
		    	else
		    		offsets[i] = Math.abs(random.nextLong() )% total_length;
		    }
		    Arrays.sort(offsets);
		    for(int i=0;i< offsets.length;i++){
		    	System.out.println("offsets ["+ i+"]=" +offsets[i]);
		    }
		    int record_in = 0;
		    int records_returned = 0;
		    
		    int file_i = 0;
		    while (record_in < offsets.length) {
		    	// skip to the file that contains the next sample
				while(offsets[record_in] > files_start_offset[file_i + 1])
					file_i++;
					
					long current_file_size = files_start_offset[file_i + 1] - files_start_offset[file_i];
					FileSystem fs = files[file_i].getFileSystem(params);
					ShapeLineRecordReader reader = new ShapeLineRecordReader(fs.getConf(),
							new FileSplit(files[file_i], 0, current_file_size, new String[]{}));
					Cubic key = reader.createKey();
					Text line = reader.createValue();
					long pos = files_start_offset[file_i];
					while(record_in < offsets.length && offsets[record_in] <= files_start_offset[file_i+1]
							&& reader.next(key, line)){
						pos+=line.getLength();
						if(pos > offsets[record_in]){
							// passed the offset of record_i report this element to output
							if(converter != null){
								inobj.fromText(line);
								converter.collect(inobj);
							}
							record_in++;
							records_returned++;
						}
					}
					reader.close();
					while(record_in < offsets.length && offsets[record_in] <= files_start_offset[file_i + 1])
						record_in++;
				}
				return records_returned;
	  }
	
	  private static <O extends TextSerializable, T extends TextSerializable> ResultCollector<T> createConverter(
		      final ResultCollector<O> output, T inObj, final O outObj) {
		  if (output == null)
		      return null;
		  if(inObj.getClass() == outObj.getClass()){
			  return new ResultCollector<T>() {
				  @Override
				  public void collect(T r){
					  output.collect(((O)r));
				  }
			};
		  }else if (inObj instanceof Shape && outObj instanceof Point) {
			final Point out_pt = (Point) outObj;
			return  new ResultCollector<T>() {
				@Override
				public void collect(T r){
					Shape s = (Shape)r;
					if(s==null)
						return;
					Cubic mbc = s.getMBC();
					if(mbc == null)
						return;
					Point pt = mbc.getCenterPoint();
					out_pt.x = pt.x;	out_pt.y = pt.y;	out_pt.z = pt.z;
					output.collect(outObj);
				}
			};
		}else if (inObj instanceof Shape && outObj instanceof Cubic) {
			final Cubic out_cub = (Cubic) outObj;
			return new ResultCollector<T>() {
				@Override
				public void collect(T r){
					out_cub.set((Cubic)r);
					output.collect(outObj);
				}
			};
		}else if(inObj instanceof Text) {
			final Text text = (Text) inObj;
			return new ResultCollector<T>() {
				@Override
				public void collect(T r){
					outObj.fromText(text);
					output.collect(outObj);
				}
			};
		}else {
			throw new RuntimeException("can not convert from " + inObj.getClass() +" to "+outObj.getClass());
		}
	  }
	  
	  public static void sample(Path[] inputFiles,
			  ResultCollector<? extends TextSerializable> output,
			  OperationsParams params) throws IOException{
		  if(params.get("ratio") != null){
			  if(params.getBoolean("local", false)){
				  sampleLocalWithRatio(inputFiles, output, params);
			  }else{
				  sampleMapReduceWithRatio(inputFiles, output, params);
			  }
		  }
	  }
	  
	  private static final PathFilter hiddenFileFilter = new PathFilter(){
		    public boolean accept(Path p){
		      String name = p.getName(); 
		      return !name.startsWith("_") && !name.startsWith("."); 
		    }
		  }; 
	
		  private static final PathFilter DataFileFilter = new PathFilter(){
			    public boolean accept(Path p){
			      String name = p.getName(); 
			      return !name.startsWith("_"); 
			    }
			    }; 

	
	
	
	
}
