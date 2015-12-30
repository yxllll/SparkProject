package edu.ecnu.idse.TrajStore.mapred;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;

import com.sun.accessibility.internal.resources.accessibility;

import edu.ecnu.idse.TrajStore.core.Cubic;
import edu.ecnu.idse.TrajStore.core.Point;
import edu.ecnu.idse.TrajStore.core.Shape;
import edu.ecnu.idse.TrajStore.core.SpatialTemporalSite;
import edu.ecnu.idse.TrajStore.util.OperationsParams;

public class RandomShapeGenerator<S extends Shape> implements RecordReader<Cubic, S> {
	
	/**Correlation factor for correlated, anticorrelated data*/
	private final static double rho = 0.9;
	
	private static final byte[] NEW_LINE =
		System.getProperty("line.separator").getBytes();
		  
	public enum DistributionType {
	    UNIFORM, GAUSSIAN, CORRELATED, ANTI_CORRELATED, CIRCLE
	}
	
	 /**The area where shapes are generated*/
	private Cubic mbc;
	 
	 /**Type of distribution to use for generating points*/
	private DistributionType type; 
	 
	  /**Random generator to use for generating shapes*/
	private Random random;

	  /**Total size to be generated in bytes*/
	private long totalSize;

	  /**The shape to use for generation*/
	private S shape;
	  
	  /**Size generated so far*/
	private long generatedSize;
	 
	  /**A temporary text used to serialize shapes to determine its size*/
	private Text text = new Text();
	
	  /**Maximum edge length for generated cubics*/
	private int cubicsize;
	  
	  
	  /// random function uset the uniform distribution
	  public RandomShapeGenerator(Configuration job,
	      RandomInputFormat.GeneratedSplit split) throws IOException {
		 
	    this(split.length, 
	    		getCubic(job),
	    	DistributionType.UNIFORM,
	        job.getInt("cubicsize", 100), //“cubicsize为单机使用”
	        split.index + job.getLong("seed", System.currentTimeMillis()));
	    setShape((S)SpatialTemporalSite.createStockShape(job));
	    System.out.println(OperationsParams.getShape(job, "shape").getMBC());
	  }
	  
	  public static Cubic getCubic(Configuration job){
			Cubic cubic =new Cubic();
			cubic.set(job.get("mbc"));
			return cubic; 
	  }
	  public RandomShapeGenerator(long size, Cubic mbc,
		      DistributionType type, int cubsize, long seed) {
		    this.totalSize = size;
		    this.mbc = mbc;
		    this.type = type;
		    this.cubicsize = cubsize;
		    this.random = new Random(seed);
		    this.generatedSize = 0;

	  }
	  
	  public void setShape(S shape) {
		    this.shape = shape;
	  }
	  
	  // write the value to the text
	  @Override
	  public boolean next(Cubic key, S value) throws IOException {
	    // Generate a random shape
	    generateShape(value, mbc, type, cubicsize, random);
	    
	    // Serialize it to text first to make it easy count its size
	    text.clear();
	    value.toText(text);
	    
	    // Check if desired generated size has been reached
	    if (text.getLength() + NEW_LINE.length + generatedSize > totalSize)
	    	return false;

	    generatedSize += text.getLength() + NEW_LINE.length;

	    return true;
	  }
	  

	  @Override
	  public Cubic createKey() {
	    Cubic key = new Cubic();
	    key.invalidate();
	    return key;
	  }
	  
	  @Override
	  public S createValue() {
	    return shape;
	  }
	  
	  @Override
	  public long getPos() throws IOException {
	    return generatedSize;
	  }

	  @Override
	  public void close() throws IOException {
	    // Nothing
	  }

	  @Override
	  public float getProgress() throws IOException {
	    if (totalSize == 0) {
	      return 0.0f;
	    } else {
	      return Math.min(1.0f, generatedSize / (float) totalSize);
	    }
	  }

	  private static void generateShape(Shape shape, Cubic mbc,
		      DistributionType type, int cubSize, Random random) {
	    if (shape instanceof Point) {
	      generatePoint((Point)shape, mbc, type, random);
	    } else if (shape instanceof Cubic) {
		      ((Cubic)shape).x1 = random.nextDouble() * (mbc.x2 - mbc.x1) + mbc.x1;
		      ((Cubic)shape).y1 = random.nextDouble() * (mbc.y2 - mbc.y1) + mbc.y1;
		      ((Cubic)shape).z1 = random.nextDouble() * (mbc.z2 - mbc.z1) + mbc.z1; 
		      ((Cubic)shape).x2 = Math.min(mbc.x2, ((Cubic)shape).x1 + random.nextInt(cubSize) + 2);
		      ((Cubic)shape).y2 = Math.min(mbc.y2, ((Cubic)shape).y1 + random.nextInt(cubSize) + 2);
		      ((Cubic)shape).z2 = Math.min(mbc.z2, ((Cubic)shape).z1 + random.nextInt(cubSize) + 2);
	    	} else {
	    		throw new RuntimeException("Cannot generate random shapes of type: "+shape.getClass());
	    	}
	 }
	  
	  public static void generatePoint(Point p, Cubic mbc, DistributionType type, Random rand) {
		    switch (type) {
		    case UNIFORM:
		      p.x = rand.nextDouble() * (mbc.x2 - mbc.x1) + mbc.x1;
		      p.y = rand.nextDouble() * (mbc.y2 - mbc.y1) + mbc.y1; 
		      p.z = rand.nextDouble() * (mbc.z2 - mbc.z1) + mbc.z1; 
		      break;
		    case GAUSSIAN:
		      p.x = nextGaussian(rand) * (mbc.x2 - mbc.x1) / 2.0 + (mbc.x1 + mbc.x2) / 2.0;
		      p.y = nextGaussian(rand) * (mbc.y2 - mbc.y1) / 2.0 + (mbc.y1 + mbc.y2) / 2.0;
		      p.z = nextGaussian(rand) * (mbc.z2 - mbc.z1) / 2.0 + (mbc.z1 + mbc.z2) / 2.0;
		      break;
		    default:
		      throw new RuntimeException("Unrecognized distribution type: "+type);
		    }
	}
	  
	  // The standard deviation is 0.2
	  public static double nextGaussian(Random rand) {
	    double res = 0;
	    do {
	      res = rand.nextGaussian() / 5.0;
	    } while(res < -1 || res > 1);
	    return res;
	  }
}
