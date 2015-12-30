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

public class QuadtreeTest extends Partitioner {
	private static final Log LOG = LogFactory.getLog(QuadTreePartitioner.class);

	protected Rectangle mbr;
	protected int[] leafNodeIDs;

	private static BufferedReader bufferedReader;

	public QuadtreeTest() {

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
		//将list转化为数组
		Point points [] = (Point[])pointlist.toArray(new Point[pointlist.size()]);
		return points;
	}
	
	
	public static void main(String[] args) throws NumberFormatException, IOException {
	
		double x1 = 115.750000;
		double y1 = 39.500000;
		double x2 = 117.2;
		double y2 = 40.5;
		// m,n 用来控制地图方格分割的块数
		int m = 200, n = 200;
		double rate = 1; // 这个比例是按照百分比筛选所需点数来构建Rtree
		String pointin = "/home/yang/data/allpoints/finalpointbytotal.txt";
		String writepath = "/home/yang/data/QTree/finaltest.txt"; // file output
		//String writepoints = "/home/yang/data/OUTpoints.txt";
		File pointfile = new File(pointin);
		Point valpoints[] = Filetopoints(pointfile);
		int numPartitions = 500; // partition 个数
		QuadTreePartitioner qPartitioner = new QuadTreePartitioner();
		
		System.out.println(valpoints.length);
		Rectangle rect = new Rectangle(x1, y1, x2, y2);
		qPartitioner.createFromPoints(rect, valpoints, numPartitions);
		WriteToFile(qPartitioner, writepath);
		//WritePoints(points, writepoints);
//		for (int i = 0; i < numPartitions; i++) {
//			System.out.println(qPartitioner.getPartitionAt(i));
//		}
	}

}
