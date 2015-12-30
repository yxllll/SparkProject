package edu.ecnu.idse.TrajStore.mapred;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.ecnu.idse.TrajStore.core.GlobalIndex;
import edu.ecnu.idse.TrajStore.core.Partition;
import edu.ecnu.idse.TrajStore.core.ResultCollector;
import edu.ecnu.idse.TrajStore.core.SpatialTemporalSite;
import edu.ecnu.idse.TrajStore.util.OperationsParams;

public class SpatialTemporalInputFormat<K,V> extends FileInputFormat<K, V> {
	
	/**
	 * used to check whether files are compressed or not. Some compressed files(e.g., gz)  are not splittable
	 */
	
	private CompressionCodecFactory compressionCodecs = null;
// we need to use this way of constructing readers to be able to pass it to CombineFileRecordReader.
	static final Class[] constructorSignature = new Class[] {
			Configuration.class, FileSplit.class};
	
	protected Class<? extends RecordReader> rrClass;
	
	@Override
	public RecordReader<K, V> getRecordReader(InputSplit split, JobConf job,
			Reporter reporter) throws IOException{
		if(compressionCodecs == null)
			compressionCodecs = new CompressionCodecFactory(job);
		if(split instanceof FileSplit){
			FileSplit fSplit = (FileSplit) split;
			@SuppressWarnings("rawtypes")
			Constructor<? extends RecordReader> rrConstructor;	
			try{
			
				rrConstructor = rrClass.getConstructor(constructorSignature);
				rrConstructor.setAccessible(true);
				return rrConstructor.newInstance(new Object[]{job,fSplit});
			}catch(SecurityException e){
				e.printStackTrace();
			}catch (NoSuchMethodException e) {
				e.printStackTrace();
			}catch (InvocationTargetException e) {
				e.printStackTrace();
			}catch (IllegalAccessException e) {
				e.printStackTrace();
			}catch (InstantiationException e) {
				e.printStackTrace();
			}
			throw new RuntimeException("can not generate a record reader"); /*************************** some questions here*/
		}else{
			throw new RuntimeException(" can not handle splits of type " +split.getClass());
		}
	}
	
	protected void listStatus(final FileSystem fs, Path dir, final List<FileStatus> result, BlockFilter filter) throws IOException{
		GlobalIndex<Partition> gIndex =  SpatialTemporalSite.getGlobalIndex(fs,dir);
		
		if(gIndex == null){ //index hasn't constructed
			FileStatus [] listStatus;
			if(OperationsParams.isWildcard(dir)){     // need to be checked in operationsparams
				listStatus = fs.globStatus(dir);
			}else{
				listStatus = fs.listStatus(dir, SpatialTemporalSite.NonHiddenFileFilter);
			}
			// add all files under this directory
			for(FileStatus status : listStatus){
				if(status.isDir()){
					listStatus(fs, status.getPath(), result, filter);
				}else if(status.getPath().getName().toLowerCase().endsWith(".list")){
					LineRecordReader in = new LineRecordReader(fs.open(status.getPath()), 0, status.getLen(), Integer.MAX_VALUE);
					LongWritable key = in.createKey();
					Text value = in.createValue();
					while(in.next(key, value)){
							result.add(fs.getFileStatus(new Path(status.getPath().getParent(), value.toString())  ));
					}
					in.close();
				}else {
					result.add(status);
				}
			}
		}else{ // global index has been constructed
			final Path indexDir = OperationsParams.isWildcard(dir)? dir.getParent() :dir;
			// use the global index to limit files
			filter.selectCells(gIndex, new ResultCollector<Partition>() {
				
				@Override
				public void collect(Partition r) {
					// TODO Auto-generated method stub
					try{
						Path cell_path = new Path(indexDir, r.filename);
						if(!fs.exists(cell_path))
							LOG.warn("Matched File not found:" + cell_path);
						result.add(fs.getFileStatus(cell_path));
					}catch (Exception e){
						e.printStackTrace();
					}
				}
			});
			
		}
	}
	
	protected  FileStatus[] listStatus (JobConf job) throws IOException{
		try{
			// create the compressioncodecs to be used later by isSplitable
			if(compressionCodecs ==null)
				compressionCodecs = new CompressionCodecFactory(job);
			
			// retrieve the BlockFilter set by the developer in the jobconf
			Class<? extends BlockFilter> blockFilterClass = job.getClass(SpatialTemporalSite.FilterClass, null,BlockFilter.class);
			if(blockFilterClass == null){
				LOG.info("No block filter specified");
				return super.listStatus(job);
			}
			
			// get  all blocks the user want to process
			BlockFilter blockFilter;
			blockFilter = blockFilterClass.newInstance();
			blockFilter.configure(job);
			
			// filter files based on user specified filter function and get directions
			List<FileStatus> result = new ArrayList<FileStatus>();
			Path [] inputDirs = getInputPaths(job);
			
			for(Path dir : inputDirs){
				FileSystem fs = dir.getFileSystem(job);
				listStatus(fs, dir, result, blockFilter);
			}
			LOG.info("Spatial-teporal filter functions mathed with "+ result.size() + " cells");
			return result.toArray(new FileStatus[result.size()]);
		}catch (InstantiationException e){
			LOG.warn(e);
			return super.listStatus(job);
		}catch (IllegalAccessException e) {
			LOG.warn(e);
			return super.listStatus(job);
		}
	}
	
	@Override
	protected boolean isSplitable(FileSystem fs, Path file){
		// HDF files are not splitable
		if(file.getName().toLowerCase().endsWith(".hdf"))
			return false;
		final CompressionCodec codec = compressionCodecs.getCodec(file);
		if(codec != null && !( codec instanceof SplittableCompressionCodec))
			return false;
		
		try{
			// and never split a file less than 120MB to perform better with many small files
			if(fs.getFileStatus(file).getLen() < 120*1024*1024)
				return false;
				// judge whether file is indexed with RTree--------------------------------------------------------------------------------------check any other index methods?
	///		return !SpatialTemporalSite.IsRTree(fs,file); //////////////////////////////////!!!!!!!!!!!!!!!!!!!先注释，以后得改回来
		}catch(IOException e){
			return super.isSplitable(fs, file);
		}
		return false;
	}
	
}
