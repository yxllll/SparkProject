package edu.ecnu.idse.TrajStore.core;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

/*
 * 将网格按照其中的点数排序
 * 然后将其除以100获得权重
 * 除以一百后的权重weight
 * 在每个网格中随即获取weight个点
 * 
 */

public class GetPointsToFile {

	public GetPointsToFile () {
		
	}
	public Points[] Getnetgrids(int m, int n, double x1, double y1, double x2, double y2, File file, double rate)
			throws NumberFormatException, IOException {
		GetGrids grids = new GetGrids(x1, y1, x2, y2);
		grids.setGrid(x1, y1, x2, y2, m, n);
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
		System.out.println(validpoint.length);
//		 for (int i = 0; i < validpoint.length; i++) {
//		 int a1 = validpoint[i].getvalue();
//		 System.out.println(a1);
//		 }
		 
		 
		//查看每个百分比网格里的点数
		 int a,b;double q = validpoint.length;
		 for (int i = 1; i < 100; i++) {
			 double p = ((double)i)/100;
			 double r = p*q;
			 a = (int) r;
			 b = validpoint[a].getvalue();
			 System.out.println("前% "+ i + ": " + b);
		 }
		 
		 
		 
		 
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
	}

	public static void WriteFinalPoints(Point[] point, String filepath) throws IOException {
		java.text.DecimalFormat df = new java.text.DecimalFormat("#.000000");
		FileWriter writer = new FileWriter(filepath);
		BufferedWriter bw = new BufferedWriter(writer);
		String x1, y1;
		String[] strings = new String[point.length];
		for (int i = 0; i < point.length; i++) {
			x1 = df.format(point[i].x);
			y1 = df.format(point[i].y);
			strings[i] = x1 + " " + y1 + " " + "\n";
			bw.write(strings[i]);
		}
		bw.close();
		writer.close();
	}

	public static void main(String[] args) throws NumberFormatException, IOException {		
		
		double x1 = 115.750000;
		double y1 = 39.500000;
		double x2 = 117.2;
		double y2 = 40.5;
		// m,n 用来控制地图方格分割的块数
		int m = 200, n = 200;
		double rate = 1; // 这个比例是按照百分比筛选所需点数来构建Rtree
		String filepath = "/home/yang/data/allpoints/total.txt";
		String writepath = "/home/yang/data/allpoints/finalpointbytotal.txt"; // file output

		File file = new File(filepath);
		GetPointsToFile gettofile = new GetPointsToFile();
		Points points[] = gettofile.Getnetgrids(m, n, x1, y1, x2, y2, file, rate);
		int num = points.length;
		System.out.println(num);

		int dividenum = 100;
		GetRandomPoints getRandomPoints = new GetRandomPoints();
		Point valpoints[] = getRandomPoints.getRandomPoint(points, dividenum);
		
		System.out.println(valpoints.length);
		WriteFinalPoints(valpoints, writepath);
	}

}
