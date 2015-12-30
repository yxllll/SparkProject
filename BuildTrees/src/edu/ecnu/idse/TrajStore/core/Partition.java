package edu.ecnu.idse.TrajStore.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import edu.ecnu.idse.TrajStore.io.TextSerializerHelper;

public class Partition  extends SpaceInfo{
	/**Name of the file that contains the data*/
	  public String filename;
	  
	  /**Total number of records in this partition*/
	  public long recordCount;
	  
	  /**Total size of data in this partition in bytes (uncompressed)*/
	  public long size;
	  
	  public Partition() {}
	  
	  public Partition(String filename, SpaceInfo space) {
	    this.filename = filename;
	    super.set(space);
	  }
	  
	  public Partition(Partition other) {
	    this.filename = other.filename;
	    this.recordCount = other.recordCount;
	    this.size = other.size;
	    super.set((SpaceInfo)other);
	  }

	 @Override
	  public void write(DataOutput out) throws IOException {
	    super.write(out);
	    out.writeUTF(filename);
	    out.writeLong(recordCount);
	    out.writeLong(size);
	  }
	 
	 @Override
	  public void readFields(DataInput in) throws IOException {
	    super.readFields(in);
	    filename = in.readUTF();
	    this.recordCount = in.readLong();
	    this.size = in.readLong();
	  }
	 
	 @Override
	  public Text toText(Text text) {
	    super.toText(text);
	    text.append(new byte[] {','}, 0, 1);
	    TextSerializerHelper.serializeLong(recordCount, text, ',');
	    TextSerializerHelper.serializeLong(size, text, ',');
	    byte[] temp = (filename == null? "" : filename).getBytes();
	    text.append(temp, 0, temp.length);
	    return text;
	  }
	 
	 @Override
	  public void fromText(Text text) {
	    super.fromText(text);
	    text.set(text.getBytes(), 1, text.getLength() - 1); // Skip comma
	    this.recordCount = TextSerializerHelper.consumeLong(text, ',');
	    this.size = TextSerializerHelper.consumeLong(text, ',');
	    filename = text.toString();
	  }
	 
	  @Override
	  public Partition clone() {
	    return new Partition(this);
	  }
	  
	  @Override
	  public int hashCode() {
	    return filename.hashCode();
	  }
	  
	  @Override
	  public boolean equals(Object obj) {
	    return this.filename.equals(((Partition)obj).filename);
	  }
	  
	  public void expand(Partition p) {
	    super.expand(p);
	    // accumulate size
	    this.size += p.size;
	    this.recordCount += p.recordCount;
	  }
}
