package BuildIndex;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import edu.ecnu.idse.TrajStore.core.CellInfo;
import edu.ecnu.idse.TrajStore.core.KdTreePartitioner;
import edu.ecnu.idse.TrajStore.core.Point;
import edu.ecnu.idse.TrajStore.core.QuadTreePartitioner;
import edu.ecnu.idse.TrajStore.core.Rectangle;
import edu.ecnu.idse.TrajStore.core.ZCurvePartitioner;

public class AllTreeIndex extends KdTreePartitioner {

	private static BufferedReader bufferedReader;

	public AllTreeIndex() {

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

	public void ChooseIndex(String str, Rectangle rec, Point[] points, int numPartitions, String file)
			throws IOException {
		switch (str) {
		case "KDtree":
			try {
				{
					KdTreePartitioner kd = new KdTreePartitioner();
					kd.createFromPoints(rec, points, numPartitions);
					KdTreePartitioner.WriteToFile(kd, file);
					int len = kd.getPartitionCount();
					CellInfo kdCellInfo[] = new CellInfo[len];
					for (int i = 0; i < len; i++) {
						kdCellInfo[i] = kd.getPartitionAt(i);
					}
					String outIndex = "/home/yang/Desktop/index/kd256.txt";
					WriteIndexToFile(outIndex, kdCellInfo);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;
		case "QTree":
			try {
				{
					QuadTreePartitioner quad = new QuadTreePartitioner();
					quad.createFromPoints(rec, points, numPartitions);
					QuadTreePartitioner.WriteToFile(quad, file);
					int len = quad.getPartitionCount();
					CellInfo quadCellInfo[] = new CellInfo[len];
					for (int i = 0; i < len; i++) {
						quadCellInfo[i] = quad.getPartitionAt(i);
					}
					String outIndex = "/home/yang/Desktop/index/quad2.txt";
					WriteIndexToFile(outIndex, quadCellInfo);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;
		case "Zcurve":
			try {
				{
					ZCurvePartitioner zcurve = new ZCurvePartitioner();
					zcurve.createFromPoints(rec, points, numPartitions);
					ZCurvePartitioner.WriteToFile(zcurve, file);
					int len = zcurve.getPartitionCount();
					CellInfo zcurveCellInfo[] = new CellInfo[len];
					for (int i = 0; i < len; i++) {
						zcurveCellInfo[i] = zcurve.getPartitionAt(i);
					}
					String outIndex = "/home/yang/Desktop/index/zcurve.txt";
					WriteIndexToFile(outIndex, zcurveCellInfo);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;
		/*
		 * case "R-tree": try { { MySTRTree_final rtree = new
		 * MySTRTree_final(500); MySTRTree_final.BuildTreeByPoint(points,
		 * rtree); MySTRTree_final.WriteToFile(rtree, file); } } catch
		 * (Exception e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); } break;
		 */

		default: {
			System.out.println("Wrrong input!!!");
		}
			break;
		}
	}

	public static void WriteIndexToFile(String file, CellInfo[] cellinfo) throws IOException {
		FileWriter fw = new FileWriter(file);
		java.text.DecimalFormat df = new java.text.DecimalFormat("#.000000");
		for (int i = 0; i < cellinfo.length; i++) {
			fw.write(cellinfo[i].cellId + "\t" + df.format(cellinfo[i].x1) + " " + df.format(cellinfo[i].y1) + "\t"
					+ df.format(cellinfo[i].x2) + " " + df.format(cellinfo[i].y2) + "\n");
		}
		fw.close();
	}

	public static void main(String[] args) throws NumberFormatException, IOException {
		double x1 = 115.750000;
		double y1 = 39.500000;
		double x2 = 117.2;
		double y2 = 40.5;
		Rectangle rec = new Rectangle(x1, y1, x2, y2);
		// read file
		String pointin = "/home/yang/data/allpoints/finalpointbytotal.txt";
		File pointfile = new File(pointin);
		Point valpoints[] = Filetopoints(pointfile);

		// choose indexes and Partition:KDtree , QTree , Zcurve
		String choose = "QTree";
		int numPartitions = 500;
		AllTreeIndex index = new AllTreeIndex();
		String fileout = "/home/yang/data/" + choose + "/" + Integer.toString(numPartitions) + ".txt";

		// Write to the file directory
		index.ChooseIndex(choose, rec, valpoints, numPartitions, fileout);

	}
}
