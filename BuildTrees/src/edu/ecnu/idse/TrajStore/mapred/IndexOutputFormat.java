package edu.ecnu.idse.TrajStore.mapred;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import com.sun.jersey.core.util.StringIgnoreCaseKeyComparator;
import com.sun.org.apache.bcel.internal.generic.RET;

import edu.ecnu.idse.TrajStore.core.Partition;
import edu.ecnu.idse.TrajStore.core.Partitioner;
import edu.ecnu.idse.TrajStore.core.Shape;

public class IndexOutputFormat<S extends Shape>
 	extends FileOutputFormat<IntWritable, S>{

	private static final Log LOG = LogFactory.getLog(IndexOutputFormat.class);
	
	protected static byte[] NEW_LINE;
	
	static{
		try {
			NEW_LINE = System.getProperty("line.separator","\n").getBytes("utf-8");
		} catch (UnsupportedEncodingException e) {
			// TODO: handle exception
			e.printStackTrace();
			throw new RuntimeException("Cannot retrieve system line separator",e);
		}
	}
	
	public static class IndexRecordWriter<S extends Shape> implements RecordWriter<IntWritable, S>{
		// the partitioner used by the current job
		private Partitioner partitioner;
		private FileSystem outFS;
		private Path outPath;
		// info of partitions being written
		private Map<Integer, Partition> partitionsInfo = new HashMap<Integer, Partition>();
		/*
		 * DataOutputStream for all partitions being written. It needs to be an instance of stream so that it can be closed later.
		 */
		private Map<Integer, DataOutputStream> partitionsOutput = new HashMap<Integer, DataOutputStream>();
		// a tmp text to serialize objects before writing to output file
		private Text tempText = new Text();                                                                                                  															//////////////////////modified here
		private Vector<Thread> closingThreads = new Vector<Thread>();
		private Progressable progress;
		// the master file contains information about all written partitions
		private DataOutputStream masterFile;
		// list of errors that happened by a background thread.
		private Vector<Throwable> listOfErrors = new Vector<Throwable>();
		// whether records are replicated in the index or distributed.
		private boolean replicated;
		
		public IndexRecordWriter(JobConf job, Path outPath) throws IOException{
			this(job, null,outPath,null);
		}
		
		public IndexRecordWriter(JobConf job, String name, Path outPath,
				Progressable progress) throws IOException{
			String sindex = job.get("sindex");
			this.replicated = job.getBoolean("replicated", false);
			this.progress = progress;
			this.outFS = outPath.getFileSystem(job);
			this.outPath = outPath;
			this.partitioner = Partitioner.getPartitioner(job);
			Path masterFilePath = ( name ==null? new Path(outPath,String.format("_mastere_.%s", sindex)): new Path(outPath, String.format("_master_%s_.%s", name,sindex)));
			this.masterFile = outFS.create(masterFilePath);
		}
		
		public IndexRecordWriter(Partitioner partitioner, boolean replicate,
					String sindex, Path outPath, Configuration conf) throws IOException{
			this.replicated = replicate;
			this.outFS = outPath.getFileSystem(conf);
			this.outPath = outPath;
			this.partitioner = partitioner;
			Path masterFilePath = new Path(outPath,"_master_."+sindex);
			this.masterFile = outFS.create(masterFilePath);
		}
		
		public  void write(IntWritable partitionID, S value) throws IOException{
			int Id = partitionID.get();
			if(Id < 0){
				// an indicator to close a partition
				int partitionToClose = -(Id+1);
				this.closePartition(partitionToClose);
			}else{
				// An actual object that we need to write
				DataOutput output = getOrCreateDataOutput(Id);
				tempText.clear();
				value.toText(tempText);
				byte[] bytes = tempText.getBytes();
				output.write(bytes,0,tempText.getLength());
				output.write(NEW_LINE);
				Partition partition = partitionsInfo.get(Id);
				partition.recordCount++;
				partition.size += tempText.getLength() + NEW_LINE.length;
				partition.expand(value);
			}
			if(progress != null)
				progress.progress();
		}
		
		//close a file that is currently open for a specific partition. Returns a background thread that will continue all closed-related logic.
		private void closePartition(final int id){
			final Partition partitionInfo = partitionsInfo.get(id);
			final DataOutputStream outStream = partitionsOutput.get(id);
			Thread closeThread = new Thread(){
				@Override
				public void run(){
					try{
						outStream.close();
						if(replicated){
							// if data is replicated, we can shrink down the size of the partition to keep partitions disjoint.
							partitionInfo.set(partitionInfo.getIntersection(partitioner.getPartition(id)));
						}
						Text partitionText = partitionInfo.toText(new Text());
						synchronized (masterFile) {
							// write the partition information to the master file
							masterFile.write(partitionText.getBytes(),0,partitionText.getLength());
							masterFile.write(NEW_LINE);
						}
						if(!closingThreads.remove(Thread.currentThread())){
							throw new RuntimeException("couldn't remove closing thread");
						}
					}catch(IOException e){
						e.printStackTrace();
						throw new RuntimeException("Error closing partion: "+partitionInfo,e);
					}
				}
			};
			
		
			closeThread.setUncaughtExceptionHandler(new UncaughtExceptionHandler(){
				public void uncaughtException(Thread t, Throwable e){
					listOfErrors.add(e);
				}
			});
			// clear partition information to indicate we can no longer write to it
			partitionsInfo.remove(id);
			partitionsOutput.remove(id);
			// start the thread in the background and keep track of it.
			closingThreads.add(closeThread);
			closeThread.start();
		}
	
		private DataOutput getOrCreateDataOutput(int id) throws IOException{
			DataOutputStream out = partitionsOutput.get(id);
			if(out == null){
				Path path = getPartitionFile(id);
				out = outFS.create(path);
				Partition partition = new Partition();
				partition.spaceId = id;
				partition.set(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE, -Double.MAX_VALUE, -Double.MAX_VALUE, -Double.MAX_VALUE);
				partition.filename = path.getName();
				// store in the hash tables for further user
				partitionsOutput.put(id, out);
				partitionsInfo.put(id, partition);
				}
			return out;
		}
		
		 private Path getPartitionFile(int id) throws IOException {
			 // doesn't exist at first
			 Path partitionPath = new Path(outPath, String.format("part-%05d", id));
			 if(outFS.exists(partitionPath)){
				 int i = 0;
				 do {
					 partitionPath = new Path(outPath, String.format("part-%05d-%03d", id,++i));
					 
				} while (outFS.exists(partitionPath) );
			 }
			 return partitionPath;
		 }
	
		 @Override
		 public void close(Reporter reporter) throws IOException{
			 try{
				 for(Integer id: partitionsInfo.keySet()){
					 closePartition(id);
					 if(reporter != null)
						 reporter.progress();
				 }
				for(int i = 0; i< closingThreads.size();i++){
					Thread thread = closingThreads.elementAt(i);
					while(thread.isAlive()){
						try{
							thread.join(10000);
							if(reporter!=null){
								reporter.progress();
							}
						}catch(InterruptedException e){
							e.printStackTrace();
						}
					}
				}
				if(!listOfErrors.isEmpty()){
					for(Throwable t: listOfErrors)
						LOG.error(t);
				throw new RuntimeException("Encountered "+listOfErrors.size()+"in background thread");	
				}	
				}finally {
					masterFile.close();
			 }
		 }
	}
	
	@Override 
	public RecordWriter<IntWritable, S> getRecordWriter(FileSystem ignored,
				JobConf job, String name, Progressable progress) throws IOException{
		Path taskOutputPath = getTaskOutputPath(job, name).getParent();
		return new IndexRecordWriter<>(job, name, taskOutputPath, progress);
	}
}


