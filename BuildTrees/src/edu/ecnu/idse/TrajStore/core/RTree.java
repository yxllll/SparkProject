package edu.ecnu.idse.TrajStore.core;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.IndexedSorter;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.QuickSort;

import edu.ecnu.idse.TrajStore.io.MemoryInputStream;
import self.test;

/**
 * A disk-based R-tree that can be loaded using a bulk loading method and never
 * changed afterwards. It works with any shape given in the generic parameter.
 * To load the tree, use the
 * {@link #bulkLoadWrite(byte[], int, int, int, DataOutput, boolean)} method. To
 * restore the tree from disk, use the {@link #readFields(DataInput)} methods.
 * To do queries against the tree, use the
 * {@link #search(Shape, ResultCollector)},
 * {@link #knn(double, double, int, ResultCollector2)} or
 * {@link #spatialJoin(RTree, RTree, ResultCollector2)} methods.
 * 
 * @author zzg
 *
 */
public class RTree<T extends Shape> implements Writable, Iterable<T>, Closeable {

	private static final Log LOG = LogFactory.getLog(RTree.class);
	/** Size of tree header on disk. Height + Degree + Number of records */
	public static final int TreeHeaderSize = 4 + 4 + 4;

	/**
	 * Size of a node. Offset of first child + dimensions (x, y,z, width,
	 * height,deep)
	 */
	public static final int NodeSize = 4 + 8 * 6;
	/** An instance of T that can be used to deserialize objects from disk */
	T stockObject;

	/** Height of the tree (number of levels) */
	private int height;

	/** Degree of internal nodes in the tree */
	private int degree;

	/** Total number of nodes in the tree */
	private int nodeCount;

	/** Number of leaf nodes */
	private int leafNodeCount;

	/** Number of non-leaf nodes */
	private int nonLeafNodeCount;
	/** Number of elements in the 0tree */
	private int elementCount;

	/** An input stream that is used to read node structure (i.e., nodes) */
	private FSDataInputStream structure;

	/** Input stream to tree data */
	private FSDataInputStream data;

	/** The start offset of the tree in the data stream */
	private long treeStartOffset;

	/**
	 * Total tree size (header + structure + data) used to read the data in the
	 * last leaf node correctly
	 */
	private int treeSize;

	public RTree() {
	}

	@Override
	  public void write(DataOutput out) throws IOException {
	    throw new RuntimeException("write is no longer supported. " +
	    		"Please use bulkLoadWrite to write the RTree.");
	  }
	/*
	 * builds the Rtree from the given serialized list of elements. it uses the
	 * given stockObject to deserialize these elements using
	 * TextSerializable.fromText(Text) and build the Rtree . and writes the
	 * created tree to disk directly
	 * 
	 * @param element_bytes - serialization of all elements separated by new
	 * lines
	 * 
	 * @param offset - offset of the first byte to use in elements_bytes
	 * 
	 * @param len - number of bytes to use in elements_bytes
	 * 
	 * @param degree(每个节点所包含的 子节点个数) - Degree of the R-tree to build in terms of
	 * number of children per node
	 * 
	 * @param dataOut - output stream to write the result to.
	 * 
	 * @param fast_sort setting this to <code>true</code> allows the method to
	 * run faster by materializing the offset of each element in the list which
	 * speeds up the comparison. However, this requires an additional 16 bytes
	 * per element. So, for each 1M elements, the method will require an
	 * additional 16 M bytes (approximately).
	 */
	public void bulkLoadWriter(final byte[] element_bytes, final int offset, final int len, final int degree,
			DataOutput dataOut, final boolean fast_sort) {
		try {
			int i_start = offset;
			final Text line = new Text();
			while (i_start < offset + len) {
				int i_end = skipToEOL(element_bytes, i_start);
				// extract the line without end of line character
				line.set(element_bytes, i_start, i_end - i_start - 1); // 增加的个数是从i_start+1开始,所以len要-1
				stockObject.fromText(line);
				elementCount++;
				i_start = i_end;
			}
			// It turns out the findBestDegree returns the best degree when the
			// whole
			// tree is loaded to memory when processed. However, as current
			// algorithms
			// process the tree while it's on disk, a higher degree should be
			// selected
			// such that a node fits one file block (assumed to be 4K).
			// final int degree = findBestDegree(bytesAvailable, elementCount);
			LOG.info("Bulk loading an RTree with " + elementCount + " elements");
			LOG.info("Writing an RTree with degree " + degree);
			// 计算深度，叶子节点数，非叶子节点数
			LOG.info("Writing an RTree with degree " + degree);

			int height = Math.max(1, (int) Math.ceil(Math.log(elementCount) / Math.log(degree)));
			int leafNodeCount = (int) Math.pow(degree, height - 1);
			if (elementCount < 2 * leafNodeCount && height > 1) {
				height--;
				leafNodeCount = (int) Math.pow(degree, height - 1);
			}
			int nodeCount = (int) ((Math.pow(degree, height) - 1) / (degree - 1));
			int nonLeafNodeCount = nodeCount - leafNodeCount;

			// Keep track of the offset of each element in the text
			final int[] offsets = new int[elementCount];
			final double[] xs = fast_sort ? new double[elementCount] : null;
			final double[] ys = fast_sort ? new double[elementCount] : null;
			final double[] zs = fast_sort ? new double[elementCount] : null;
			i_start = offset;
			line.clear();
			for (int i = 0; i < elementCount; i++) {
				offsets[i] = i_start;
				int i_end = skipToEOL(element_bytes, i_start);
				if (xs != null) {
					// extract the line with end of line character
					line.set(element_bytes, i_start, i_end - i_start - 1);
					stockObject.fromText(line);
					// sample center of the shape
					xs[i] = (stockObject.getMBC().x1 + stockObject.getMBC().x2) / 2;
					ys[i] = (stockObject.getMBC().y1 + stockObject.getMBC().y2) / 2;
					zs[i] = (stockObject.getMBC().z1 + stockObject.getMBC().z2) / 2;
				}
				i_start = i_end;
			}
			 class SplitStruct extends Cubic {
				/** Start and end index for this split */
				int index1, index2;
				/** Direction of this split */
				byte direction;
				/** Index of first element on disk */
				int offsetOfFirstElement;

				static final byte DIRECTION_X = 0;
				static final byte DIRECTION_Y = 1;
				static final byte DIRECTION_Z = 2;
				
				SplitStruct(int index1, int index2, byte direction) {
					this.index1 = index1;
					this.index2 = index2;
					this.direction = direction;
				}

				@Override
				public void write(DataOutput out) throws IOException {
					out.writeInt(offsetOfFirstElement);
					super.write(out);
				}

			       void partition(Queue<SplitStruct> toBePartitioned) {
			           IndexedSortable sortableX;
			           IndexedSortable sortableY;
			           IndexedSortable sortableZ;
			           
			           if(fast_sort){
			        	   sortableX = new IndexedSortable() {
							
							@Override
							public void swap(int i, int j) {
								// TODO Auto-generated method stub
								double tempx = xs[i];
								xs[i]= xs[j];
								xs[j] = tempx;
								
								double tempy = ys[i];
								ys[i] = ys[j];
								ys[j]= tempy;
								
								double tempz = zs[i];
								zs[i] = zs[j];
								zs[j] = tempz;
								//swap id
								int tempid = offsets[i];
								offsets[i] = offsets[j];
								offsets[j] = tempid;
							}
							
							@Override
							public int compare(int i, int j) {
								if (xs[i] < xs[j])
					                  return -1;
					                if (xs[i] > xs[j])
					                  return 1;
								return 0;
							}
						};
							sortableY = new IndexedSortable() {
								
								@Override
								public void swap(int i, int j) {

									double tempx = xs[i];
									xs[i]= xs[j];
									xs[j] = tempx;
									
									double tempy = ys[i];
									ys[i] = ys[j];
									ys[j]= tempy;
									
									double tempz = zs[i];
									zs[i] = zs[j];
									zs[j] = tempz;
									//swap id
									int tempid = offsets[i];
									offsets[i] = offsets[j];
									offsets[j] = tempid;
								
									
								}
								
								@Override
								public int compare(int i, int j) {
									if (ys[i] < ys[j])
						                  return -1;
						                if (ys[i] > ys[j])
						                  return 1;
									return 0;
								}
							};
							sortableZ = new IndexedSortable() {
								
								@Override
								public void swap(int i, int j) {
									double tempx = xs[i];
									xs[i]= xs[j];
									xs[j] = tempx;
									
									double tempy = ys[i];
									ys[i] = ys[j];
									ys[j]= tempy;
									
									double tempz = zs[i];
									zs[i] = zs[j];
									zs[j] = tempz;
									//swap id
									int tempid = offsets[i];
									offsets[i] = offsets[j];
									offsets[j] = tempid;		
								}
								
								@Override
								public int compare(int i, int j) {
									if (zs[i] < zs[j])
						                  return -1;
						                if (zs[i] > zs[j])
						                  return 1;
									return 0;
								}
							};
			           }else{
			        	   // no materialized xs , ys and zs. always deserialize objects to compare
			        	   	sortableX = new IndexedSortable() {
								
								@Override
								public void swap(int i, int j) {
									//swap id
									int tempid = offsets[i];
									offsets[i] = offsets[j];
									offsets[j] = tempid;
									
								}
								
								@Override
								public int compare(int i, int j) {
									// get end of line
									int eol = skipToEOL(element_bytes, offsets[i]);
									line.set(element_bytes, offsets[i], eol - offsets[i] - 1);
									stockObject.fromText(line);
									double xi =( stockObject.getMBC().x1 + stockObject.getMBC().x2)/ 2;
									
									eol = skipToEOL(element_bytes	, offsets[j]);
									line.set(element_bytes, offsets[j],  eol - offsets[j] -1);
									stockObject.fromText(line);
									double xj =( stockObject.getMBC().x1 + stockObject.getMBC().x2)/ 2;
								      if (xi < xj)
						                  return -1;
						                if (xi > xj)
						                  return 1;
									return 0;
								}
							};
							sortableY  = new IndexedSortable() {
								
								@Override
								public void swap(int i, int j) {
									int tempid = offsets[i];
									offsets[i] = offsets[j];
									offsets[j] = tempid;
									
								}
								
								@Override
								public int compare(int i, int j) {
									// get end of line
									int eol = skipToEOL(element_bytes, offsets[i]);
									line.set(element_bytes, offsets[i], eol - offsets[i] - 1);
									stockObject.fromText(line);
									double yi =( stockObject.getMBC().y1 + stockObject.getMBC().y2)/ 2;
									
									eol = skipToEOL(element_bytes	, offsets[j]);
									line.set(element_bytes, offsets[j],  eol - offsets[j] -1);
									stockObject.fromText(line);
									double yj =( stockObject.getMBC().y1 + stockObject.getMBC().y2)/ 2;
								      if (yi < yj)
						                  return -1;
						                if (yi > yj)
						                  return 1;
									return 0;
								}
							};
							sortableZ = new IndexedSortable() {
								
								@Override
								public void swap(int i, int j) {
									int tempid = offsets[i];
									offsets[i] = offsets[j];
									offsets[j] = tempid;
								}
								
								@Override
								public int compare(int i, int j) {
									// get end of line
									int eol = skipToEOL(element_bytes, offsets[i]);
									line.set(element_bytes, offsets[i], eol - offsets[i] - 1);
									stockObject.fromText(line);
									double zi =( stockObject.getMBC().z1 + stockObject.getMBC().z2)/ 2;
									
									eol = skipToEOL(element_bytes	, offsets[j]);
									line.set(element_bytes, offsets[j],  eol - offsets[j] -1);
									stockObject.fromText(line);
									double zj =( stockObject.getMBC().z1 + stockObject.getMBC().z2)/ 2;
								      if (zi < zj)
						                  return -1;
						                if (zi > zj)
						                  return 1;
									return 0;
								}
							};
			           }
			           final IndexedSorter sorter = new QuickSort();
			           final IndexedSortable[] sortables = new IndexedSortable[3];
			           sortables[SplitStruct.DIRECTION_X] = sortableX;
			           sortables[SplitStruct.DIRECTION_Y] = sortableY;
			           sortables[SplitStruct.DIRECTION_Z]  = sortableZ;
			           sorter.sort(sortables[direction], index1, index2);
			           
			           // Partition into maxEnties partitions and create a splitstruct for each partition
			           int i1 = index1;
			           for(int iSplit = 0; iSplit < degree; iSplit ++){
			        	   int i2 = index1 + (index2 - index1) * (iSplit +1 )/degree;
			        	   SplitStruct newSplit = new SplitStruct(i1, i2, direction);                         // direction may need to be  changed
			        	   toBePartitioned.add(newSplit); 
			        	   i1 = i2;
			           }
			       }
			}
			// all nodes stored in level-order traversal
			Vector<SplitStruct> nodes = new Vector<SplitStruct>();
			final Queue<SplitStruct> toBePartitioned = new LinkedList<SplitStruct>();
			toBePartitioned.add(new SplitStruct(0, elementCount, SplitStruct.DIRECTION_X));
			while (!toBePartitioned.isEmpty()) {
				SplitStruct split = toBePartitioned.poll();
				if (nodes.size() < nonLeafNodeCount) {
					split.partition(toBePartitioned);
				}
				nodes.add(split);
			}
			if (nodes.size() != nodeCount) {
				throw new RuntimeException("Expected node count :" + nodeCount + ".Real node count: " + nodes.size());
			}

			// now, the data are sorted in the required order, start building
			// the tree.
			// store the offset of each leave node in the tree.
			FSDataOutputStream fakeOut = null;
			try {
				fakeOut = new FSDataOutputStream(new OutputStream() {

					@Override
					public void write(int b) throws IOException {
						// TODO Auto-generated method stub

					}

					@Override
					public void write(byte[] b, int off, int len) throws IOException {

					}

					public void write(byte[] b) throws IOException {

					}
				}, null, TreeHeaderSize + nodes.size() * NodeSize);
				for (int i_leaf = nonLeafNodeCount, i = 0; i_leaf < nodes.size(); i_leaf++) {
					nodes.elementAt(i_leaf).offsetOfFirstElement = (int) fakeOut.getPos();
					if (i != nodes.elementAt(i_leaf).index1)
						throw new RuntimeException();
					double x1, y1, z1, x2, y2, z2;

					// initialize MBC to first object
					int eol = skipToEOL(element_bytes, offsets[i]);
					fakeOut.write(element_bytes, offsets[i], eol - offsets[i]);
					line.set(element_bytes, offsets[i], eol - offsets[i] - 1);
					stockObject.fromText(line);
					Cubic mbc = stockObject.getMBC();
					x1 = mbc.x1;
					y1 = mbc.y1;
					z1 = mbc.z1;
					x2 = mbc.x2;
					y2 = mbc.y2;
					z2 = mbc.z2;
					i++;
					while (i < nodes.elementAt(i_leaf).index2) {
						eol = skipToEOL(element_bytes, offsets[i]);
						fakeOut.write(element_bytes, offsets[i], eol - offsets[i]);

						line.set(element_bytes, offsets[i], eol - offsets[i] - 1);
						stockObject.fromText(line);
						mbc = stockObject.getMBC();
						if (mbc.x1 < x1)
							x1 = mbc.x1;
						if (mbc.y1 < y1)
							y1 = mbc.y1;
						if (mbc.z1 < z1)
							z1 = mbc.z1;
						if (mbc.x2 > x2)
							x2 = mbc.x2;
						if (mbc.y2 > y2)
							y2 = mbc.y2;
						if (mbc.z2 > z2)
							z2 = mbc.z2;
						i++;
					}
					nodes.elementAt(i_leaf).set(x1, y1, z1, x2, y2, z2);
				}
			} finally {
				if (fakeOut != null) {
					fakeOut.close();
				}
			}
			// Calculate MBC and offsetOfFirstElement for non-leaves
			for (int i_node = nonLeafNodeCount - 1; i_node >= 0; i_node--) {
				int i_first_child = i_node * degree + 1;
				nodes.elementAt(i_node).offsetOfFirstElement= nodes.elementAt(i_first_child).offsetOfFirstElement;
				int i_child = 0;
				Cubic mbc;
				mbc = nodes.elementAt(i_first_child + i_child);
				  double x1 = mbc.x1;
			        double y1 = mbc.y1;
			        double z1 = mbc.z1;
			        double x2 = mbc.x2;
			        double y2 = mbc.y2;
			        double z2 = mbc.z2;
			        i_child++;
			        
			        while (i_child < degree) {
			        	mbc = nodes.elementAt(i_first_child + i_child);
			          if (mbc.x1 < x1) x1 = mbc.x1;
			          if (mbc.y1 < y1) y1 = mbc.y1;
			          if (mbc.z1< z1) z1 = mbc.z1;
			          if (mbc.x2 > x2) x2 = mbc.x2;
			          if (mbc.y2 > y2) y2 = mbc.y2;
			          if (mbc.z2< z2) z2 = mbc.z2;
			          i_child++;
			        }
			        nodes.elementAt(i_node).set(x1, y1,z1, x2, y2,z2);
			      }
			  // Start writing the tree
		      // write tree header (including size)
		      // Total tree size. (== Total bytes written - 8 bytes for the size itself)
		      dataOut.writeInt(TreeHeaderSize + NodeSize * nodeCount + len);
		      // Tree height
		      dataOut.writeInt(height);
		      // Degree
		      dataOut.writeInt(degree);
		      dataOut.writeInt(elementCount);
		      
		      // write nodes
		      for (SplitStruct node : nodes) {
		        node.write(dataOut);
		      }
		      // write elements
		      for (int element_i = 0; element_i < elementCount; element_i++) {
		        int eol = skipToEOL(element_bytes, offsets[element_i]);
		        dataOut.write(element_bytes, offsets[element_i],
		            eol - offsets[element_i]);
		      }

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	  @Override
	  public void readFields(DataInput in) throws IOException {
	    // Tree size (Header + structure + data)
		  treeSize = in.readInt();
		  if(treeSize == 0){
			  height = elementCount =0;
			  return;
		  }
		  height = in.readInt();
		  if(height == 0)
			  return;
		  degree = in.readInt();
		  elementCount = in.readInt();
		  // keep only tree structure in memory
		    nodeCount = (int) ((powInt(degree, height) - 1) / (degree - 1));
		    int structureSize = nodeCount * NodeSize;
		    byte[] treeStructure = new byte[structureSize];
		    in.readFully(treeStructure, 0, structureSize);
		    structure = new FSDataInputStream(new MemoryInputStream(treeStructure));
		    if (in instanceof FSDataInputStream) {
		        this.treeStartOffset = ((FSDataInputStream) in).getPos() - structureSize - TreeHeaderSize;
		        this.data = (FSDataInputStream) in;
		      } else {
		        // Load all tree data in memory
		        this.treeStartOffset = 0 - structureSize - TreeHeaderSize;
		        int treeDataSize = treeSize - TreeHeaderSize - structureSize;
		        byte[] treeData = new byte[treeDataSize];
		        in.readFully(treeData, 0, treeDataSize);
		        this.data = new FSDataInputStream(new MemoryInputStream(treeData));
		      }
		      nodeCount = (int) ((Math.pow(degree, height) - 1) / (degree - 1));
		      leafNodeCount = (int) Math.pow(degree, height - 1);
		      nonLeafNodeCount = nodeCount - leafNodeCount;
	  }
	  
	  public int getElementCount() {
		    return elementCount;
		  }
		  
	  public Cubic getMBC(){
		  Cubic mbc = null;
		  try{
			  structure.seek(0);
			  mbc = new Cubic();
			  mbc.readFields(structure);
		  }catch(IOException e){
			  e.printStackTrace();
		  }
		  return mbc;
	  }
	  
	  /**
	   * Reads and returns the element with the given index
	   * @param i
	   * @return
	   * @throws IOException 
	   */
	  public T readElement(int i) {
	    Iterator<T> iter = iterator();
	    while (i-- > 0 && iter.hasNext()) {
	      iter.next();
	    }
	    return iter.next();
	  }
	  
	  class RTreeIterator implements Iterator<T> {
		  /**Current offset in the data stream*/
		    int offset;
		    
		    /**Temporary text that holds one line to deserialize objects*/
		    Text line;
		    
		    /**A stock object to read from stream*/
		    T _stockObject;
		    
		    /**A reader to read lines from the tree*/
		    LineReader reader;
		    
		    RTreeIterator() throws IOException {
		        offset = TreeHeaderSize + NodeSize * RTree.this.nodeCount;
		        _stockObject = (T) RTree.this.stockObject.clone();
		        line = new Text();
		        RTree.this.data.seek(offset + RTree.this.treeStartOffset);
		        reader = new LineReader(RTree.this.data);
		      }
		    
		    @Override
		    public boolean hasNext() {
		      return offset < RTree.this.treeSize;
		    }

		    @Override
		    public T next() {
		      try {
		        offset += reader.readLine(line);
		        _stockObject.fromText(line);
		      } catch (IOException e) {
		        e.printStackTrace();
		        return null;
		      }
		      return _stockObject;
		    }
		    
		    @Override
		    public void remove() {
		      throw new RuntimeException("Not supported");
		    }
	  }
	  
	  @Override
	  public Iterator<T> iterator() {
	    try {
	      return new RTreeIterator();
	    } catch (IOException e) {
	      e.printStackTrace();
	    }
	    return null;
	  }
	  public void setStockObject(T stockObject) {
		    this.stockObject = stockObject;
		  }
	  
	  /**
	   * Find log to the base 2 quickly
	   * @param x
	   * @return
	   */
	  public static int log2Floor(int x) {
	    if (x == 0)
	      return -1;
	    int pos = 0;
	    if ((x & 0xFFFF0000) != 0) {
	      pos += 16;
	      x >>>= 16;
	    }
	    if ((x & 0xFF00) != 0) {
	      pos += 8;
	      x >>>= 8;
	    }
	    if ((x & 0xF0) != 0) {
	      pos += 4;
	      x >>>= 4;
	    }
	    if ((x & 0xC) != 0) {
	      pos += 2;
	      x >>>= 2;
	    }
	    if ((x & 0x2) != 0) {
	      pos++;
	      x >>>= 1;
	    }
	    
	    return pos;
	  }
	  
	  public static int powInt(int base, int exponent) {
	    int pow = 1;
	    while (exponent != 0) {
	      if ((exponent & 1) != 0)
	        pow *= base;
	      exponent >>>= 1;
	      base *= base;
	    }
	    return pow;
	  }
	// a struct to  store information about a split

	  public static int skipHeader(InputStream in) throws IOException {
		    DataInput dataIn = in instanceof DataInput ? (DataInput) in
		        : new DataInputStream(in);
		    int skippedBytes = 0;
		    /*int treeSize = */dataIn.readInt(); skippedBytes += 4;
		    int height = dataIn.readInt(); skippedBytes += 4;
		    if (height == 0) {
		      // Empty tree. No results
		      return skippedBytes;
		    }
		    int degree = dataIn.readInt(); skippedBytes += 4;
		    int nodeCount = (int) ((powInt(degree, height) - 1) / (degree - 1));
		    /*int elementCount = */dataIn.readInt(); skippedBytes += 4;
		    // Skip all nodes
		    dataIn.skipBytes(nodeCount * NodeSize); skippedBytes += nodeCount * NodeSize;
		    return skippedBytes;
		  }
		  

	/**
	 * Skip bytes until the end of line
	 * 
	 * @param bytes
	 * @param startOffset
	 * @return
	 */
	public static int skipToEOL(byte[] bytes, int startOffset) {
		int eol = startOffset;
		while (eol < bytes.length && (bytes[eol] != '\n' && bytes[eol] != '\r'))
			eol++;
		while (eol < bytes.length && (bytes[eol] == '\n' || bytes[eol] == '\r'))
			eol++;
		return eol;
	}

	  @Override
	  public void close() throws IOException {
	    if (data != null)
	      data.close();
	  }
}
