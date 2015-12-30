package edu.ecnu.idse.TrajStore.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;

public class GlobalIndex <S extends Shape> implements Writable, Iterable<S> {

	/**A stock instance of S used to deserialize objects from disks */
	
	protected S stockShape;
	
	//* All underlying shapes in no specific order */
	protected S[] shapes;
	
	//* Whether partitions in this global index are compact or not     */
	private boolean compact;
	
	private boolean replicated;
	
	public GlobalIndex(){	
	}

	public void bulkLoad(S[] shapes){
		this.shapes = shapes.clone(); // a shallow copy
		for(int i=0;i<this.shapes.length;i++){
			this.shapes[i] = (S) shapes[i].clone();              ///*** different from original file
		}
	}
	
	public void write(DataOutput out) throws IOException{
		out.writeInt(shapes.length);
		for(int i=0; i<shapes.length;i++){
			shapes[i].write(out);
		}
	}
	
	public void readFields(DataInput in) throws IOException{
		int length = in.readInt();
		this.shapes = (S[]) new Shape[length];
		for(int i=0;i<length;i++){
			this.shapes[i] = (S) stockShape.clone();
			this.shapes[i].readFields(in);
		}
	} 

	/*
	 * return the intersected ranges
	 */
	public int rangeQuery(Shape queryRange, ResultCollector<S> output){
		int result_count = 0;
		for(S shape: shapes){
			if(shape.isIntersected(queryRange)){
				result_count++;
				if(output!=null){
					output.collect(shape);
				}
			}
		}
		return result_count;
	}
	
	class SimpleIterator implements Iterator<S>{
		
		int i = 0;
		/*
		 * 
		 */
		@Override
		public boolean hasNext(){
			return i< shapes.length;
		}
		
		@Override
		public S next(){
			return shapes[i++];
		}
		
		@Override
		public void remove(){
			throw new RuntimeException("Not implemented");
		}
	}
	
	@Override
	public Iterator<S> iterator(){
		return new SimpleIterator();
	}
	
	/*
	 * Number of objects stored in the index
	 */
	public int size(){
		return shapes.length;
	}
	
	/*
	 * Return  the minimal bounding cubic of all objects in the index
	 * if the index is empty, null is returned.
	 */
	// expand according to each shape
	public Cubic getMBC(){
		Iterator<S> i = this.iterator();
		if(!i.hasNext())
			return null;
		Cubic globalMBC = new Cubic(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE, 
				-Double.MAX_VALUE, -Double.MAX_VALUE, -Double.MAX_VALUE);
		while(i.hasNext()){
			globalMBC.expand(i.next().getMBC());
		}
		return globalMBC;	
	}
	
	
	public int knn(final double qx, final double qy, int k, ResultCollector2<S, Double> output){
		
		throw new RuntimeException("hasn't emplemented");
		
	}
	
	// return true if the partitions are compact(minimal) around its contents
	public boolean isCompact(){
		return this.compact;
	}
	
	public void setCompact(boolean compact){
		this.compact = compact;
	}
	
	public void setReplicated(boolean r){
		this.replicated = r;
	}
	
	public boolean isReplicated(){
		return this.replicated;
	}
}