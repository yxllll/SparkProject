package edu.ecnu.idse.TrajStore.core;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ZCurvePartitioner extends Partitioner {
	private static final Log LOG = LogFactory.getLog(ZCurvePartitioner.class);

	/** MBR of the input file */
	protected Rectangle mbr;
	/** Upper bound of all partitions */
	protected long[] zSplits;

	protected static final int Resolution = Integer.MAX_VALUE;
	private static BufferedReader bufferedReader;

	/**
	 * A default constructor to be able to dynamically instantiate it and
	 * deserialize it
	 */
	public ZCurvePartitioner() {
		this.mbr = new Rectangle();
	}

	@Override
	public void createFromPoints(Rectangle mbr, Point[] points, int numPartitions) {
		this.mbr = mbr.clone();
		long[] zValues = new long[points.length];
		for (int i = 0; i < points.length; i++)
			zValues[i] = computeZ(mbr, points[i].x, points[i].y);

		createFromZValues(zValues, numPartitions);

		/*
		 * debug code this.mbr = mbr.clone(); Map<Point, Long> p2z = new
		 * HashMap<Point,Long>(); long[] zValues = new long[points.length]; for
		 * (int i = 0; i < points.length; i++){ zValues[i] = computeZ(mbr,
		 * points[i].x, points[i].y); p2z.put(points[i], zValues[i]); }
		 * List<Map.Entry<Point, Long>> infos = new ArrayList<Map.Entry<Point,
		 * Long>>(p2z.entrySet()); Collections.sort(infos,new
		 * Comparator<Map.Entry<Point, Long>>() { public int
		 * compare(Map.Entry<Point, Long> o1, Map.Entry<Point, Long> o2) {
		 * //return (o2.getValue() - o1.getValue()); return
		 * o1.getValue().compareTo(o2.getValue()); } }); for(Map.Entry<Point,
		 * Long> entry : infos){
		 * System.out.println(entry.getKey()+":"+entry.getValue()); }
		 * createFromZValues(zValues, numPartitions);
		 */
	}

	// Computes the Z-order of a point relative to a containing rectangle
	public static long computeZ(Rectangle mbr, double x, double y) {
		int ix = (int) ((x - mbr.x1) * Resolution / mbr.getWidth());
		int iy = (int) ((y - mbr.y1) * Resolution / mbr.getHeight());
		return computeZOrder(ix, iy);
	}

	// Computes the Z-order (Morton order) of a two-dimensional point.
	public static long computeZOrder(long x, long y) {
		long morton = 0;

		for (long bitPosition = 0; bitPosition < 32; bitPosition++) {
			long mask = 1L << bitPosition;
			morton |= (x & mask) << (bitPosition + 1);
			morton |= (y & mask) << bitPosition;
		}
		return morton;
	}

	/**
	 * Create a ZCurvePartitioner from a list of points
	 * 
	 * @param vsample
	 * @param inMBR
	 * @param partitions
	 * @return
	 */
	protected void createFromZValues(final long[] zValues, int partitions) {
		Arrays.sort(zValues);

		this.zSplits = new long[partitions];
		long maxZ = computeZ(mbr, mbr.x2, mbr.y2);

		for (int i = 0; i < partitions; i++) {
			int quantile = (int) ((long) (i + 1) * zValues.length / partitions);
			this.zSplits[i] = quantile == zValues.length ? maxZ : zValues[quantile];
		}
	}

	public static void uncomputeZ(Rectangle mbr, long z, Point outPoint) {
		long ixy = unComputeZOrder(z);
		int ix = (int) (ixy >> 32);
		int iy = (int) (ixy & 0xffffffffL);
		outPoint.x = (double) (ix) * mbr.getWidth() / Resolution + mbr.x1;
		outPoint.y = (double) (iy) * mbr.getHeight() / Resolution + mbr.y1;
	}

	public static long unComputeZOrder(long morton) {
		long x = 0, y = 0;
		for (long bitPosition = 0; bitPosition < 32; bitPosition++) {
			long mask = 1L << (bitPosition << 1);
			y |= (morton & mask) >> bitPosition;
			x |= (morton & (mask << 1)) >> (bitPosition + 1);
		}
		return (x << 32) | y;
	}

	public void write(DataOutput out) throws IOException {
		mbr.write(out);
		out.writeInt(zSplits.length);
		ByteBuffer bbuffer = ByteBuffer.allocate(zSplits.length * 8);
		for (long zSplit : zSplits)
			bbuffer.putLong(zSplit);
		if (bbuffer.hasRemaining())
			throw new RuntimeException("Did not calculate buffer size correctly");
		out.write(bbuffer.array(), bbuffer.arrayOffset(), bbuffer.position());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		mbr.readFields(in);
		int partitionCount = in.readInt();
		zSplits = new long[partitionCount];
		int bufferLength = 8 * partitionCount;
		byte[] buffer = new byte[bufferLength];
		in.readFully(buffer);
		ByteBuffer bbuffer = ByteBuffer.wrap(buffer);
		for (int i = 0; i < partitionCount; i++) {
			zSplits[i] = bbuffer.getLong();
		}
		if (bbuffer.hasRemaining())
			throw new RuntimeException("Error reading STR partitioner");
	}

	@Override
	public int getPartitionCount() {
		return zSplits.length;
	}

	public void overlapPartitions(Shape shape, ResultCollector<Integer> matcher) {
		// TODO match with all overlapping partitions instead of only one
		int partition = overlapPartition(shape);
		if (partition >= 0)
			matcher.collect(partition);
	}

	public int overlapPartition(Shape shape) {
		if (shape == null)
			return -1;
		Rectangle shapeMBR = shape.getMBR();
		if (shapeMBR == null)
			return -1;
		// Assign to only one partition that contains the center point
		Point center = shapeMBR.getCenterPoint();
		long zValue = computeZ(mbr, center.x, center.y);
		int partition = Arrays.binarySearch(zSplits, zValue);
		if (partition < 0)
			partition = -partition - 1;
		return partition;
	}

	public CellInfo getPartitionAt(int index) {
		return getPartition(index);
	}

	public CellInfo getPartition(int id) {
		CellInfo cell = new CellInfo();
		cell.cellId = id;
		long zMax = zSplits[id];
		long zMin = id == 0 ? 0 : zSplits[id - 1];

		Rectangle cellMBR = getMBR(mbr, zMin, zMax);
		cell.set(cellMBR);
		return cell;
	}

	public static Rectangle getMBR(Rectangle mbr, long zMin, long zMax) {
		java.awt.Rectangle mbrInteger = getMBRInteger(zMin, zMax);
		Rectangle trueMBR = new Rectangle();
		trueMBR.x1 = (double) (mbrInteger.x) * mbr.getWidth() / Resolution + mbr.x1;
		trueMBR.y1 = (double) (mbrInteger.y) * mbr.getHeight() / Resolution + mbr.y1;
		trueMBR.x2 = (double) (mbrInteger.getMaxX()) * mbr.getWidth() / Resolution + mbr.x1;
		trueMBR.y2 = (double) (mbrInteger.getMaxY()) * mbr.getHeight() / Resolution + mbr.y1;
		return trueMBR;
	}

	public static java.awt.Rectangle getMBRInteger(long zMin, long zMax) {
		zMax -= 1; // Because the range is exclusive
		long changedBits = zMin ^ zMax;
		// The mask contains 1's for all bits that are less or equal significant
		// to any changed bit
		long mask = changedBits;
		long oldMask;
		do {
			oldMask = mask;
			mask |= (mask >> 1);
		} while (mask != oldMask);
		// Both zMin and zMax can be used in the following equations because we
		// explicitly set all different bits
		java.awt.Point minXY = unComputeZOrder(zMin & (~mask), new java.awt.Point());
		java.awt.Point maxXY = unComputeZOrder(zMin | mask, new java.awt.Point());
		java.awt.Rectangle mbr = new java.awt.Rectangle(minXY.x, minXY.y, maxXY.x - minXY.x, maxXY.y - minXY.y);
		return mbr;
	}

	public static java.awt.Point unComputeZOrder(long morton, java.awt.Point point) {
		long ixy = unComputeZOrder(morton);
		point.x = (int) (ixy >>> 32);
		point.y = (int) (ixy & 0xffffffff);
		return point;
	}

	public static void WriteToFile(ZCurvePartitioner zcu, String filepath) throws IOException {
		java.text.DecimalFormat df = new java.text.DecimalFormat("#.000000");
		FileWriter writer = new FileWriter(filepath);
		BufferedWriter bw = new BufferedWriter(writer);
		String x1, y1, x2, y2;
		CellInfo[] Cinfo = new CellInfo[10000];
		int level = 0;
		String[] strings = new String[10000];
		for (int i = 0; i < zcu.getPartitionCount(); i++) {
			Cinfo[i] = zcu.getPartitionAt(i);
			x1 = df.format(Cinfo[i].x1);
			x2 = df.format(Cinfo[i].x2);
			y1 = df.format(Cinfo[i].y1);
			y2 = df.format(Cinfo[i].y2);
			strings[i] = x1 + " " + y1 + " " + x2 + " " + y2 + " " + level + "\n";
			bw.write(strings[i]);
		}
		bw.close();
		writer.close();
		System.out.println("Zcurve Writing Completed !");
	}

	public static Point[] Filetopoints(File file) throws NumberFormatException, IOException {
		java.util.List<Point> pointlist = new ArrayList<Point>();
		if (file.isFile() && file.exists()) {
			String encoding = "GBK";
			InputStreamReader reader = new InputStreamReader(new FileInputStream(file), encoding);
			bufferedReader = new BufferedReader(reader);
			String linetxt = null;
			while ((linetxt = bufferedReader.readLine()) != null) {
				// 坐标拆分
				String[] str = linetxt.split(" ");
				double x1, y1; // 代表坐标lng，lat
				x1 = Double.valueOf(str[0]);
				y1 = Double.valueOf(str[1]);
				pointlist.add(new Point(x1, y1, 0));
			}
		}
		// 将list转化为数组
		Point points[] = (Point[]) pointlist.toArray(new Point[pointlist.size()]);
		return points;
	}

	public static void main(String[] args) throws NumberFormatException, IOException {

		double x1 = 115.750000;
		double y1 = 39.500000;
		double x2 = 117.2;
		double y2 = 40.5;
		String pointin = "/home/yang/data/allpoints/finalpointbytotal.txt";
		String writepath = "/home/yang/data/Zcurve/final1000.txt"; // file
																	// output
		File pointfile = new File(pointin);
		Point valpoints[] = Filetopoints(pointfile);

		ZCurvePartitioner zcurve = new ZCurvePartitioner();
		int numofparts = 1000;
		zcurve.createFromPoints(new Rectangle(x1, y1, x2, y2), valpoints, numofparts);
		WriteToFile(zcurve, writepath);
		System.out.println(valpoints.length);
		for (int i = 0; i < numofparts; i++) {
			System.out.println(zcurve.getPartitionAt(i));
		}
		System.out.println("!!!!!!!!!!!!111");
		for (int i = 0; i < zcurve.getPartitionCount(); i++) {
			System.out.println(zcurve.zSplits[i]);
		}

	}
}
