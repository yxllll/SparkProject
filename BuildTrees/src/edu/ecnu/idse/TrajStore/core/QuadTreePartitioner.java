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
import java.util.Queue;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class QuadTreePartitioner extends Partitioner {
	private static final Log LOG = LogFactory.getLog(QuadTreePartitioner.class);

	protected Rectangle mbr;
	protected int[] leafNodeIDs;

	public QuadTreePartitioner() {

	}

	public void createFromPoints(Rectangle mbr, Point[] points, int numPartitions) {
		this.mbr = mbr;
		long[] zValues = new long[points.length];
		for (int i = 0; i < points.length; i++)
			zValues[i] = ZCurvePartitioner.computeZ(mbr, points[i].x, points[i].y);

		createFromZvalues(zValues, numPartitions);

	}

	protected void createFromZvalues(final long[] zValues, int partitions) {
		int nodeCapacity = zValues.length / partitions;
		Arrays.sort(zValues);
		class QuadTreeNode {
			int fromIndex, toIndex;
			long minZ, maxZ;
			int nodeID;
			int depth;

			public QuadTreeNode(int fromIndex, int toIndex, long minZ, long maxZ, int nodeID, int depth) {
				this.fromIndex = fromIndex;
				this.toIndex = toIndex;
				this.minZ = minZ;
				this.maxZ = maxZ;
				this.nodeID = nodeID;
				this.depth = depth;
			}
		}

		long minZ = ZCurvePartitioner.computeZ(mbr, mbr.x1, mbr.y1); // always
																		// zero
		long maxZ = ZCurvePartitioner.computeZ(mbr, mbr.x2, mbr.y2);
		QuadTreeNode root = new QuadTreeNode(0, zValues.length, minZ, maxZ, 1, 1);
		Queue<QuadTreeNode> nodesToSplit = new ArrayDeque<>();
		nodesToSplit.add(root);

		Vector<Integer> leafNodeIDs = new Vector<Integer>();

		while (!nodesToSplit.isEmpty()) {
			QuadTreeNode nodeToSplit = nodesToSplit.remove();
			if (nodeToSplit.toIndex - nodeToSplit.fromIndex <= nodeCapacity) {
				leafNodeIDs.add(nodeToSplit.nodeID);
			} else {
				// The position of the lowest of the two bits that change for
				// these
				// children in the Z-order
				// For the root, we change the two highest bits in the zOrder
				int changedBits = KdTreePartitioner.getNumberOfSignificantBits(ZCurvePartitioner.Resolution) * 2
						- nodeToSplit.depth * 2;
				// need to split into four children
				long childMinZ = nodeToSplit.minZ;
				int childFromIndex = nodeToSplit.fromIndex;
				for (int iChild = 0; iChild < 4; iChild++) {
					long childMaxZ = nodeToSplit.minZ + ((iChild + 1L) << changedBits);
					int childToIndex = Arrays.binarySearch(zValues, nodeToSplit.fromIndex, nodeToSplit.toIndex,
							childMaxZ);
					if (childToIndex < 0) {
						childToIndex = -(childToIndex + 1);
					}
					QuadTreeNode childNode = new QuadTreeNode(childFromIndex, childToIndex, childMinZ, childMaxZ,
							nodeToSplit.nodeID * 4 + iChild, nodeToSplit.depth + 1);
					nodesToSplit.add(childNode);
					childMinZ = childMaxZ;
					childFromIndex = childToIndex;
				}
			}
		}

		this.leafNodeIDs = new int[leafNodeIDs.size()];
		for (int i = 0; i < leafNodeIDs.size(); i++) {
			this.leafNodeIDs[i] = leafNodeIDs.get(i);
		}
		Arrays.sort(this.leafNodeIDs);
	}

	public void write(DataOutput out) throws IOException {
		mbr.write(out);
		out.writeInt(leafNodeIDs.length);
		ByteBuffer bbuffer = ByteBuffer.allocate(leafNodeIDs.length * 4);
		for (int leafNodeID : leafNodeIDs)
			bbuffer.putInt(leafNodeID);
		if (bbuffer.hasRemaining())
			throw new RuntimeException("Did not calculate buffer size correctly");
		out.write(bbuffer.array(), bbuffer.arrayOffset(), bbuffer.position());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		if (mbr == null)
			mbr = new Rectangle();
		mbr.readFields(in);
		int numberOfLeafNodes = in.readInt();
		leafNodeIDs = new int[numberOfLeafNodes];
		byte[] buffer = new byte[leafNodeIDs.length * 4];
		in.readFully(buffer);
		ByteBuffer bbuffer = ByteBuffer.wrap(buffer);
		for (int i = 0; i < leafNodeIDs.length; i++)
			leafNodeIDs[i] = bbuffer.getInt();
	}

	@Override
	public int overlapPartition(Shape shape) {
		if (shape == null || shape.getMBR() == null)
			return -1;

		Point queryPoint = shape.getMBR().getCenterPoint();
		int nodeToSearch = 1; // Start from the root
		Rectangle nodeMBR = mbr.clone();
		while (Arrays.binarySearch(leafNodeIDs, nodeToSearch) < 0) {
			if (nodeToSearch > leafNodeIDs[leafNodeIDs.length - 1]) {
				System.err.println("not found");
				return -1;
			}
			Point nodeCenter = nodeMBR.getCenterPoint();
			if (queryPoint.x < nodeCenter.x && queryPoint.y < nodeCenter.y) {
				nodeToSearch = nodeToSearch * 4;
				nodeMBR.x2 = nodeCenter.x;
				nodeMBR.y2 = nodeCenter.y;
			} else if (queryPoint.x < nodeCenter.x && queryPoint.y >= nodeCenter.y) {
				nodeToSearch = nodeToSearch * 4 + 1;
				nodeMBR.x2 = nodeCenter.x;
				nodeMBR.y1 = nodeCenter.y;
			} else if (queryPoint.x >= nodeCenter.x && queryPoint.y < nodeCenter.y) {
				nodeToSearch = nodeToSearch * 4 + 2;
				nodeMBR.x1 = nodeCenter.x;
				nodeMBR.y2 = nodeCenter.y;
			} else {
				nodeToSearch = nodeToSearch * 4 + 3;
				nodeMBR.x1 = nodeCenter.x;
				nodeMBR.y1 = nodeCenter.y;
			}
		}
		return nodeToSearch;
	}

	@Override
	public void overlapPartitions(Shape shape, ResultCollector<Integer> matcher) {
		if (shape == null || shape.getMBR() == null)
			return;
		Rectangle shapeMBR = shape.getMBR();
		Queue<CellInfo> nodesToSearch = new ArrayDeque<CellInfo>();
		nodesToSearch.add(new CellInfo(1, mbr));

		while (!nodesToSearch.isEmpty()) {
			// Go down as necessary
			CellInfo nodeToSearch = nodesToSearch.remove();
			if (shapeMBR.isIntersected(nodeToSearch)) {
				if (Arrays.binarySearch(leafNodeIDs, nodeToSearch.cellId) >= 0) {
					// Reached a leaf node that overlaps the given shape
					matcher.collect(nodeToSearch.cellId);
				} else {
					// Overlapping with a non-leaf node, go deeper to four
					// children
					Point centerPoint = nodeToSearch.getCenterPoint();
					nodesToSearch.add(new CellInfo(nodeToSearch.cellId * 4, nodeToSearch.x1, nodeToSearch.y1,
							centerPoint.x, centerPoint.y));
					nodesToSearch.add(new CellInfo(nodeToSearch.cellId * 4 + 1, nodeToSearch.x1, centerPoint.y,
							centerPoint.x, nodeToSearch.y2));
					nodesToSearch.add(new CellInfo(nodeToSearch.cellId * 4 + 2, centerPoint.x, nodeToSearch.y1,
							nodeToSearch.x2, centerPoint.y));
					nodesToSearch.add(new CellInfo(nodeToSearch.cellId * 4 + 3, centerPoint.x, centerPoint.y,
							nodeToSearch.x2, nodeToSearch.y2));
				}
			}
		}
	}

	@Override
	public int getPartitionCount() {
		return leafNodeIDs.length;
	}

	@Override
	public CellInfo getPartitionAt(int index) {
		return getPartition(leafNodeIDs[index]);
	}

	@Override
	public CellInfo getPartition(int partitionID) {
		CellInfo cellInfo = new CellInfo(partitionID, mbr);

		int partitionDepth = (KdTreePartitioner.getNumberOfSignificantBits(partitionID) + 1) / 2;

		for (int depth = 1; depth < partitionDepth; depth++) {
			int childNumber = (partitionID >> (2 * (partitionDepth - depth - 1))) & 3;
			Point center = cellInfo.getCenterPoint();
			switch (childNumber) {
			case 0:
				cellInfo.x2 = center.x;
				cellInfo.y2 = center.y;
				break;
			case 1:
				cellInfo.x2 = center.x;
				cellInfo.y1 = center.y;
				break;
			case 2:
				cellInfo.x1 = center.x;
				cellInfo.y2 = center.y;
				break;
			case 3:
				cellInfo.x1 = center.x;
				cellInfo.y1 = center.y;
				break;
			}
		}

		return cellInfo;
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
		// point[m-1][n-1].upboundlng = x2; //确保最后一个网格的坐标与矩形右上角重合
		// point[m-1][n-1].upboundlat = y2;
		// System.out.println(point[m-1][0].upboundlng + " " +
		// point[0][n-1].upboundlat + " " + point[m-1][0].lowboundlng + " " +
		// point[0][n-1].lowboundlat);
		// System.out.println(point[m-1][n-1].upboundlng + " " +
		// point[m-1][n-1].upboundlat + " " +point[m-1][n-1].lowboundlng + " " +
		// point[m-1][n-1].lowboundlat);
		
		
		
		
		// 大矩形四个角的网格权值
//		point[0][0].value = 10000;
//		point[0][n - 1].value = 10000;
//		point[m - 1][0].value = 10000;
//		point[m - 1][n - 1].value = 10000;
		
		

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

	public static void WriteToFile(QuadTreePartitioner qtree, String filepath) throws IOException {
		java.text.DecimalFormat df = new java.text.DecimalFormat("#.000000");
		FileWriter writer = new FileWriter(filepath);
		BufferedWriter bw = new BufferedWriter(writer);
		CellInfo[] Cinfo = new CellInfo[10000];
		String x1, y1, x2, y2;
		int level = 0;
		String[] strings = new String[10000];
		for (int i = 0; i < qtree.getPartitionCount(); i++) {
			Cinfo[i] = qtree.getPartitionAt(i);
			x1 = df.format(Cinfo[i].x1);
			x2 = df.format(Cinfo[i].x2);
			y1 = df.format(Cinfo[i].y1);
			y2 = df.format(Cinfo[i].y2);
			strings[i] = x1 + " " + y1 + " " + x2 + " " + y2 + " " + level + "\n";
			bw.write(strings[i]);
		}
		bw.close();
		writer.close();
		System.out.println("Quad Writing Completed !");
	}

	public static void WritePoints(Points[] points, String filepath) throws IOException {
		java.text.DecimalFormat df = new java.text.DecimalFormat("#.000000");
		FileWriter writer = new FileWriter(filepath);
		BufferedWriter bw = new BufferedWriter(writer);
		String x1, y1, x2, y2;
		int level = 0;
		String[] strings = new String[points.length];
		for (int i = 0; i < points.length; i++) {
			x1 = df.format(points[i].lowboundlng);
			x2 = df.format(points[i].upboundlng);
			y1 = df.format(points[i].lowboundlat);
			y2 = df.format(points[i].upboundlat);
			strings[i] = x1 + " " + y1 + " " + x2 + " " + y2 + " " + level + "\n";
			bw.write(strings[i]);
		}
		bw.close();
		writer.close();
	}

	public static void main(String[] args) throws NumberFormatException, IOException {
		// Point p0 = new Point(26, 60, 0);
		// Point p1 = new Point(45, 60, 0);
		// Point p2 = new Point(50, 75, 0);
		// Point p3 = new Point(50, 100, 0);
		// Point p4 = new Point(50, 120, 0);
		// Point p5 = new Point(70, 100, 0);
		// Point p6 = new Point(85, 140, 0);
		// Point p7 = new Point(30, 260, 0);
		// Point p8 = new Point(25, 400, 0);
		// Point p9 = new Point(45, 350, 0);
		// Point p10 = new Point(50, 275, 0);
		// Point p11 = new Point(60, 260, 0);
		// Point[] points = new Point[] { p0, p1, p2, p3, p4, p5, p6, p7, p8,
		// p9, p10, p11 };
		//
		// int numPartitions = 7;
		// QuadTreePartitioner qPartitioner = new QuadTreePartitioner();
		// Rectangle rect = new Rectangle(0, 0, 90, 400);
		// qPartitioner.createFromPoints(rect, points, numPartitions);
		// for (int i = 0; i < numPartitions; i++) {
		// System.out.println(qPartitioner.getPartitionAt(i));
		// }

//		Point p0 = new Point(116.394783,39.963839,0);
//		Point p1 = new Point(116.353389,39.900988,0);
//		Point p2 = new Point(116.406281,39.924896,0);
//		Point p3 = new Point(116.402257,39.904973,0);
//		Point p4 = new Point(116.329242,39.931535,0);
//		Point p5 = new Point(116.391333,39.846499,0);
//		Point p6 = new Point(116.41088,39.94614,0);
//		Point p7 = new Point(116.311995,39.846942,0);
//		Point p8 = new Point(116.395932,40.038123,0);
//		Point p9 = new Point(116.517815,39.924896,0);
//		Point pa = new Point(116.390183,39.909843,0);
//		Point pb = new Point(116.460323,39.81148,0);
//		Point pc = new Point(116.263702,39.957645,0);
//		Point pd = new Point(116.506316,39.843397,0);
//		Point[] points1 = new Point[] { p0, p1, p2, p3, p4, p5, p6, p7, p8,
//				 p9, pa, pb, pc, pd};
		
		
		double x1 = 115.750000;
		double y1 = 39.500000;
		double x2 = 117.2;
		double y2 = 40.5;
		// m,n 用来控制地图方格分割的块数
		int m = 200, n = 200;
		double rate = 1; // 这个比例是按照百分比筛选所需点数来构建Rtree
		String filepath = "/home/yang/data/points.txt";
		String writepath = "/home/yang/data/QTree/ALLQT40-500PURE.txt"; // file output
		//String writepoints = "/home/yang/data/OUTpoints.txt";

		File file = new File(filepath);

		int numPartitions = 500; // partition 个数
		QuadTreePartitioner qPartitioner = new QuadTreePartitioner();
		Points points[] = qPartitioner.Getnetgrids(m, n, x1, y1, x2, y2, file, rate);
		int num = points.length;
		System.out.println(num);

		Point valpoints[] = new Point[num];
		for (int i = 0; i < valpoints.length; i++) {
			valpoints[i] = new Point(); // new一个 否则空指针错误T_T!!!
			valpoints[i].x = (points[i].lowboundlng + points[i].upboundlng) / 2;
			valpoints[i].y = (points[i].lowboundlat + points[i].upboundlat) / 2;
			valpoints[i].z = 0;
		}

		
		System.out.println(valpoints.length);
		Rectangle rect = new Rectangle(x1, y1, x2, y2);
		qPartitioner.createFromPoints(rect, valpoints, numPartitions);
		WriteToFile(qPartitioner, writepath);
		//WritePoints(points, writepoints);
		for (int i = 0; i < numPartitions; i++) {
			System.out.println(qPartitioner.getPartitionAt(i));
		}
	}

}
