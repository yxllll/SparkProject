package edu.ecnu.idse.TrajStore.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import edu.ecnu.idse.TrajStore.io.TextSerializerHelper;

/*
 * the sub-space of each cubic
 */
public class SpaceInfo extends Cubic {
	/**
	   * A unique ID for this space in a file. This must be set initially when
	   * spaces for a file are created. It cannot be guessed from cubic dimensions.
	   */
	  public int spaceId;
	  
	  public SpaceInfo(DataInput in) throws IOException{
		  this.readFields(in);
	  }

	  public SpaceInfo(String in){
		  this.fromText(new Text(in));
	  }
	  
	  public SpaceInfo(){
		  super();
	  }
	  
	  public SpaceInfo(int id, double x1, double y1, double z1, double x2, double y2,double z2) {
		  super(x1, y1,z1, x2, y2,z2);
		  this.spaceId = id;
	  }
	  
	  public SpaceInfo(int id, Cubic spaceInfo) {
		  this(id, spaceInfo.x1, spaceInfo.y1, spaceInfo.z1,spaceInfo.x2, spaceInfo.y2, spaceInfo.z2);
		  if (id == 0)
		      throw new RuntimeException("Invalid space id: "+id);
	  }
	  
	  public SpaceInfo(SpaceInfo c) {
		    this.set(c);
	  }
	  
	  public void set(SpaceInfo c) {
		  if (c == null) {
		      this.spaceId = 0; // Invalid number
		  } else {
		      super.set(c); // Set rectangle
		      this.spaceId = c.spaceId; // Set cellId
		  }
	  }
	  
	  @Override
	  public String toString() {
	    return "Space #"+spaceId+" "+super.toString();
	  }
	  
	  @Override
	  public SpaceInfo clone() {
	    return new SpaceInfo(spaceId, x1, y1,z1, x2, y2,z2);
	  }
	  
	  @Override
	  public boolean equals(Object obj) {
	    return ((SpaceInfo)obj).spaceId == this.spaceId;
	  }
	  
	  @Override
	  public int hashCode() {
	    return (int) this.spaceId;
	  }
	  
	  @Override
	  public int compareTo(Shape s) {
	    return (int) (this.spaceId - ((SpaceInfo)s).spaceId);
	  }
	  
	  @Override
	  public void write(DataOutput out) throws IOException {
	    out.writeInt(spaceId);
	    super.write(out);
	  }
	  
	  @Override
	  public void readFields(DataInput in) throws IOException {
	    this.spaceId = in.readInt();
	    super.readFields(in);
	  }
	  
	  @Override
	  public Text toText(Text text) {
	    TextSerializerHelper.serializeInt(spaceId, text, ',');
	    return super.toText(text);
	  }
	  
	  @Override
	  public void fromText(Text text) {
	    this.spaceId = TextSerializerHelper.consumeInt(text, ',');
	    super.fromText(text);
	  }
}
