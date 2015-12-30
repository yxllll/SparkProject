package edu.ecnu.idse.TrajStore.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.ecnu.idse.TrajStore.core.Cubic;
import edu.ecnu.idse.TrajStore.core.Shape;
import edu.ecnu.idse.TrajStore.util.OperationsParams;

public class RandomInputFormat<S extends Shape> implements InputFormat<Cubic, S> {
	
	 static class GeneratedSplit implements InputSplit {
		    
		 /**Index of this split*/
	    int index;
	    
	    /**Length of this split*/
	    long length;
	    
	    public GeneratedSplit() {}
	    
	    public GeneratedSplit(int index, long length) {
	      super();
	      this.index = index;
	      this.length = length;
	    }

	    @Override
	    public void write(DataOutput out) throws IOException {
	      out.writeInt(index);
	      out.writeLong(length);
	    }

	    //Deserialize the fields of this object from in. 
	    @Override
	    public void readFields(DataInput in) throws IOException {
	      this.index = in.readInt();
	      this.length = in.readLong();
	    }

	    //Get the total number of bytes in the data of the InputSplit.
	    @Override
	    public long getLength() throws IOException {
	      return 0;
	    }

	    //Get the list of hostnames where the input split is located
	    @Override
	    public String[] getLocations() throws IOException {
	      final String[] emptyLocations = new String[0];
	      return emptyLocations;
	    }
	  }

	 // 根据所需切分的片数，决定每一个切片的大小
	  @Override
	  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
	    long totalFileSize = OperationsParams.getSize(job, "size");
	    @SuppressWarnings("deprecation")
		long splitSize = FileSystem.get(job).getDefaultBlockSize();
	    
	    InputSplit[] splits = new InputSplit[(int) Math.ceil((double)totalFileSize / splitSize)];
	    int i;
	    for (i = 0; i < splits.length - 1; i++) {
	      splits[i] = new GeneratedSplit(i, splitSize);
	    }
	    if (totalFileSize % splitSize != 0)
	      splits[i] = new GeneratedSplit(i, totalFileSize % splitSize);
	    else
	      splits[i] = new GeneratedSplit(i, splitSize);
	    return splits;
	  }

	  @Override
	  public RecordReader<Cubic, S> getRecordReader(InputSplit split,
	      JobConf job, Reporter reporter) throws IOException {
	    GeneratedSplit gsplit = (GeneratedSplit) split;
	 
	    return new RandomShapeGenerator<S>(job, gsplit);
	  }
}
