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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author zzg how to use is to see the main test. the final partition result is
 *         to use the getparitionAt() Tested at 2015.11.2
 *
 */
public class KdTreePartitioner extends Partitioner {

	private static final Log LOG = LogFactory.getLog(KdTreePartitioner.class);
	private Rectangle mbr;

	public double[] splits;

	private static BufferedReader bufferedReader;
	
	public KdTreePartitioner() {

	}

	public void createFromPoints(Rectangle mbr, Point[] points, int numPartitions) {
		String[] ids = new String[numPartitions];
		for (int id = numPartitions; id < 2 * numPartitions; id++) {
			ids[id - numPartitions] = Integer.toBinaryString(id);
		}
		Comparator<Point>[] comparators = new Comparator[] { new Comparator<Point>() {
			public int compare(Point a, Point b) {
				return a.x < b.x ? -1 : (a.x > b.x ? 1 : 0);
			}
		}, new Comparator<Point>() {
			public int compare(Point a, Point b) {
				return a.y < b.y ? -1 : (a.y > b.y ? 1 : 0);
			}
		} };

		class SplitTask {
			int fromIndex;
			int toIndex;
			int direction;
			int partitionID;

			/** Constructor using all fields */
			public SplitTask(int fromIndex, int toIndex, int direction, int partitionID) {
				this.fromIndex = fromIndex;
				this.toIndex = toIndex;
				this.direction = direction;
				this.partitionID = partitionID;
			}
		}

		Queue<SplitTask> splitTasks = new ArrayDeque<SplitTask>();
		splitTasks.add(new SplitTask(0, points.length, 0, 1));
		this.mbr = mbr.clone();
		this.splits = new double[numPartitions];
		while (!splitTasks.isEmpty()) {
			SplitTask splitTask = splitTasks.remove();
			if (splitTask.partitionID < numPartitions) {
				String child1 = Integer.toBinaryString(splitTask.partitionID * 2);
				String child2 = Integer.toBinaryString(splitTask.partitionID * 2 + 1);
				int size_child1 = 0, size_child2 = 0;
				for (int i = 0; i < ids.length; i++) {
					if (ids[i].startsWith(child1))
						size_child1++;
					else if (ids[i].startsWith(child2))
						size_child2++;
				}

				// Calculate the index which partitions the subrange into sizes
				// proportional to size_child1 and size_child2
				int splitIndex = (int) (((long) size_child1 * splitTask.toIndex
						+ (long) size_child2 * splitTask.fromIndex) / (size_child1 + size_child2));
				partialQuickSort(points, splitTask.fromIndex, splitTask.toIndex, splitIndex,
						comparators[splitTask.direction]);
				Point splitValue = points[splitIndex];
				this.splits[splitTask.partitionID] = splitTask.direction == 0 ? splitValue.x : splitValue.y;
				splitTasks.add(new SplitTask(splitTask.fromIndex, splitIndex, 1 - splitTask.direction,
						splitTask.partitionID * 2));
				splitTasks.add(new SplitTask(splitIndex, splitTask.toIndex, 1 - splitTask.direction,
						splitTask.partitionID * 2 + 1));
			}
		}

	}

	/**
	 * Reorders the given subrange of the array so that the element at the
	 * desiredIndex is at its correct position. Upon return of this function,
	 * the element at the desired index is greater than or equal all previous
	 * values in the subrange and less than or equal to all following items in
	 * the subrange.
	 * 
	 * @param a
	 *            - the array to sort
	 * @param fromIndex
	 *            - the index of the first element in the subrange
	 * @param toIndex
	 *            - the index after the last element in the subrange
	 * @param desiredIndex
	 *            - the index which needs to be adjusted
	 * @param c
	 *            - the comparator used to compare array elements
	 */
	public static <T> void partialQuickSort(T[] a, int fromIndex, int toIndex, int desiredIndex, Comparator<T> c) {
		Arrays.sort(a, fromIndex, toIndex, c);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		mbr.write(out);
		out.writeInt(splits.length);
		ByteBuffer bbuffer = ByteBuffer.allocate(splits.length * 8);
		for (double split : splits)
			bbuffer.putDouble(split);
		if (bbuffer.hasRemaining())
			throw new RuntimeException("Did not calculate buffer size correctly");
		out.write(bbuffer.array(), bbuffer.arrayOffset(), bbuffer.position());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		if (mbr == null)
			mbr = new Rectangle();
		mbr.readFields(in);
		int partitions = in.readInt();
		splits = new double[partitions];

		int bufferLength = splits.length * 8;
		byte[] buffer = new byte[bufferLength];
		in.readFully(buffer);
		ByteBuffer bbuffer = ByteBuffer.wrap(buffer);
		for (int i = 0; i < splits.length; i++)
			splits[i] = bbuffer.getDouble();
		if (bbuffer.hasRemaining())
			throw new RuntimeException("Error reading STR partitioner");
	}

	@Override
	public int getPartitionCount() {
		return splits.length;
	}

	@Override
	public void overlapPartitions(Shape shape, ResultCollector<Integer> matcher) {
		if (shape == null || shape.getMBR() == null)
			return;
		Rectangle shapeMBR = shape.getMBR();
		/** Information about a split to test */
		class SplitToTest {
			/**
			 * The ID of the split in the array of splits in the
			 * KDTreePartitioner
			 */
			int splitID;
			/**
			 * Direction of the split. 0 is vertical (|) and 1 is horizontal (-)
			 */
			int direction;

			public SplitToTest(int splitID, int direction) {
				this.splitID = splitID;
				this.direction = direction;
			}
		}
		// A queue of all splits to test
		Queue<SplitToTest> splitsToTest = new ArrayDeque<SplitToTest>();
		// Start from the first (root) split
		splitsToTest.add(new SplitToTest(1, 0));

		while (!splitsToTest.isEmpty()) {
			SplitToTest splitToTest = splitsToTest.remove();
			if (splitToTest.splitID >= splits.length) {
				// Matched a partition. return it
				matcher.collect(splitToTest.splitID);
			} else {
				// Need to test that split
				if (splitToTest.direction == 0) {
					// The corresponding split is vertical (along the x-axis).
					// Like |
					if (shapeMBR.x1 < splits[splitToTest.splitID])
						splitsToTest.add(new SplitToTest(splitToTest.splitID * 2, 1 ^ splitToTest.direction)); // Go
																												// left
					if (shapeMBR.x2 > splits[splitToTest.splitID])
						splitsToTest.add(new SplitToTest(splitToTest.splitID * 2 + 1, 1 ^ splitToTest.direction)); // Go
																													// right
				} else {
					// The corresponding split is horizontal (along the y-axis).
					// Like -
					if (shapeMBR.y1 < splits[splitToTest.splitID])
						splitsToTest.add(new SplitToTest(splitToTest.splitID * 2, 1 ^ splitToTest.direction));
					if (shapeMBR.y2 > splits[splitToTest.splitID])
						splitsToTest.add(new SplitToTest(splitToTest.splitID * 2 + 1, 1 ^ splitToTest.direction));
				}
			}
		}
	}

	public int overlapPartition(Shape shape) {
		if (shape == null || shape.getMBR() == null)
			return -1;
		Point pt = shape.getMBR().getCenterPoint();
		int splitID = 1; // Start from the root
		int direction = 0;
		while (splitID < splits.length) {
			if (direction == 0) {
				// The corresponding split is vertical (along the x-axis). Like
				// |
				if (pt.x < splits[splitID])
					splitID = splitID * 2; // Go left
				else
					splitID = splitID * 2 + 1; // Go right
			} else {
				// The corresponding split is horizontal (along the y-axis).
				// Like -
				if (pt.y < splits[splitID])
					splitID = splitID * 2;
				else
					splitID = splitID * 2 + 1;
			}
			direction ^= 1;
		}
		return splitID;
	}

	@Override
	public CellInfo getPartitionAt(int index) {
		return getPartition(index + splits.length);
	}

	@Override
	public CellInfo getPartition(int id) {
		CellInfo cellInfo = new CellInfo(id, mbr);
		boolean minXFound = false, minYFound = false, maxXFound = false, maxYFound = false;
		// Direction 0 means x-axis and 1 means y-axis
		int direction = getNumberOfSignificantBits(id) & 1;
		while (id > 1) {
			// 0 means maximum and 1 means minimum
			int minOrMax = id & 1;
			id >>>= 1;
			if (minOrMax == 0 && direction == 0 && !maxXFound) {
				cellInfo.x2 = splits[id];
				maxXFound = true;
			} else if (minOrMax == 0 && direction == 1 && !maxYFound) {
				cellInfo.y2 = splits[id];
				maxYFound = true;
			} else if (minOrMax == 1 && direction == 0 && !minXFound) {
				cellInfo.x1 = splits[id];
				minXFound = true;
			} else if (minOrMax == 1 && direction == 1 && !minYFound) {
				cellInfo.y1 = splits[id];
				minYFound = true;
			}
			direction ^= 1;
		}
		return cellInfo;
	}

	/**
	 * Get number of significant bits. In other words, get the highest position
	 * of a bit that is set to 1 and add 1 to that position. For the value zero,
	 * number of significant bits is zero.
	 * 
	 * @param x
	 * @return
	 */
	public static int getNumberOfSignificantBits(int x) {
		int numOfSignificantBits = 0;
		if ((x & 0xffff0000) != 0) {
			// There's some bit on the upper word that is not zero
			numOfSignificantBits += 16;
			x >>>= 16;
		}
		if ((x & 0xff00) != 0) {
			// There's some non-zero bit in the upper byte
			numOfSignificantBits += 8;
			x >>>= 8;
		}
		if ((x & 0xf0) != 0) {
			numOfSignificantBits += 4;
			x >>>= 4;
		}
		if ((x & 0xC) != 0) {
			numOfSignificantBits += 2;
			x >>>= 2;
		}
		if ((x & 0x2) != 0) {
			numOfSignificantBits += 1;
			x >>>= 1;
		}
		if ((x & 0x1) != 0) {
			numOfSignificantBits += 1;
			// id >>>= 1; // id will always be zero
		}
		return numOfSignificantBits;
	}

	public Points[] Getnetgrids(int m, int n, double x1, double y1, double x2, double y2, File file, double rate)
			throws NumberFormatException, IOException {
		GetGrids grids = new GetGrids(x1, y1, x2, y2);

		grids.setGrid(x1, y1, x2, y2, m, n);

		// 测试输出网格坐标
		// 将每个网格打上坐标和包围和
		Points[][] point = new Points[m][n];
		for (int i = 0; i < m; i++) {
			for (int j = 0; j < n; j++) {
				point[i][j] = new Points();
				point[i][j].getPoint(grids.sublng[i], grids.sublat[j], grids.sublng[i + 1], grids.sublat[j + 1], 0);
				point[i][j].lowboundlng = grids.sublng[i];
				point[i][j].lowboundlat = grids.sublat[j];
				point[i][j].upboundlng = grids.sublng[i + 1];
				point[i][j].upboundlat = grids.sublat[j + 1];
			}
		}

		// 大矩形四个角的网格权值
		// point[0][0].value = 10000;
		// point[0][n - 1].value = 10000;
		// point[m - 1][0].value = 10000;
		// point[m - 1][n - 1].value = 10000;

		// long linenum = 0; //记录数据条数

		if (file.isFile() && file.exists()) {
			String encoding = "GBK";
			InputStreamReader reader = new InputStreamReader(new FileInputStream(file), encoding);
			BufferedReader bufferedReader = new BufferedReader(reader);
			String linetxt = null;
			String[] tmp = null;

			while ((linetxt = bufferedReader.readLine()) != null) {
				// 坐标拆分
				// linenum ++;
				String[] str = linetxt.split("\t");
				int row = 0, col = 0;
				double st1, st2; // 代表坐标lng，lat
				st1 = Double.valueOf(str[0]);
				st2 = Double.valueOf(str[1]);

				// 如果点在大矩形范围内，则判断其在哪个网格内，相应网格的value值加1
				// tmp用来防止记录不动点
				if (!str.equals(tmp) && st1 >= x1 && st1 <= x2 && st2 >= y1 && st2 <= y2) {
					for (int k1 = 0; k1 < m - 1; k1++) {
						if (!(Double.valueOf(str[0]) < grids.sublng[row])) {
							row++;
						} else
							break;
					}
					for (int k2 = 0; k2 < n - 1; k2++) {
						if (!(Double.valueOf(str[1]) < grids.sublat[col])) {
							col++;
						} else
							break;
					}
					point[row][col].value++;
				} else {
					continue;
				}
				tmp = str;
			}
			bufferedReader.close();

		}

		// 将网格Point放到一维数组,同时统计有点的格子数目valnum（这里的条件是格子里的点数大于0）
		// long min = linenum/(m*n);
		Points[] vpoint = new Points[m * n];
		int valnum = 0; // 用来记录有效的点数
		for (int k1 = 0; k1 < m; k1++) {
			for (int k2 = 0; k2 < n; k2++) {
				if (point[k1][k2].value > 0) { // 此处为统计有效点数目valnum
					vpoint[valnum] = point[k1][k2];
					valnum++;
				}
			}
		}

		// valpoint数组记录的是valnum个网格内有点的Point
		Points valpoint[] = new Points[valnum];
		for (int i = 0; i < valnum; i++) {
			valpoint[i] = vpoint[i];
		}

		// 选择排序选出前rate%的数据
		Points tmppoint = new Points();
		int size = valpoint.length;
		for (int i = 0; i < size; i++) {
			int k = i;
			for (int k2 = size - 1; k2 > i; k2--) {
				if (valpoint[k2].value < valpoint[k].value) {
					k = k2;
				}
				tmppoint = valpoint[i];
				valpoint[i] = valpoint[k];
				valpoint[k] = tmppoint;
			}
		}

		int realnumofpoints = (int) (valpoint.length * rate);
		Points validpoint[] = new Points[realnumofpoints];
		for (int i = 0; i < realnumofpoints; i++) {
			validpoint[i] = valpoint[valpoint.length - i - 1]; // validpoint是按照比例筛选出来的格子
			validpoint[i].getcore();
		}
		// for (int i = 0; i < validpoint.length; i++) {
		// int a1 = validpoint[i].getvalue();
		// System.out.println(a1);
		// }
		return validpoint;
	}

	public static void WriteToFile(KdTreePartitioner kdtree, String filepath) throws IOException {
		java.text.DecimalFormat df = new java.text.DecimalFormat("#.000000");
		FileWriter writer = new FileWriter(filepath);
		BufferedWriter bw = new BufferedWriter(writer);
		String x1, y1, x2, y2;
		CellInfo[] Cinfo = new CellInfo[10000];
		int level = 0;
		String[] strings = new String[10000];
		for (int i = 0; i < kdtree.splits.length; i++) {
			Cinfo[i] = kdtree.getPartitionAt(i);
			x1 = df.format(Cinfo[i].x1);
			x2 = df.format(Cinfo[i].x2);
			y1 = df.format(Cinfo[i].y1);
			y2 = df.format(Cinfo[i].y2);
			strings[i] = x1 + " " + y1 + " " + x2 + " " + y2 + " " + level + "\n";
			bw.write(strings[i]);
		}
		bw.close();
		writer.close();
		System.out.println("KDtree Writing Completed !");
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

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		double x1 = 115.750000;
		double y1 = 39.500000;
		double x2 = 117.2;
		double y2 = 40.5;
		// m,n 用来控制地图方格分割的块数
		String pointin = "/home/yang/data/allpoints/finalpointbytotal.txt";
		String writepath = "/home/yang/data/KDtree/final1000.txt"; // file
																	// output
		File pointfile = new File(pointin);
		Point valpoints[] = Filetopoints(pointfile);

		KdTreePartitioner kd = new KdTreePartitioner();
		int numofparts = 1000;
		kd.createFromPoints(new Rectangle(x1, y1, x2, y2), valpoints, numofparts);
		WriteToFile(kd, writepath);
		System.out.println(valpoints.length);
		System.out.println("!!!!!!!!!!!!!!!!!!!!!");
		for (int i = 0; i < 4; i++) {
			System.out.println(kd.getPartitionAt(i));
		}
	}

}
