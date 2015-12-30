package BuildIndex;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Vector;

import edu.ecnu.idse.TrajStore.core.CellInfo;

public class Test {

	public static void main(String[] args) throws IOException {
		String filepath = "/home/yang/Desktop/index/quad2.txt";
		File quadfile = new File(filepath);
		String encoding = "GBK";
		InputStreamReader reader = new InputStreamReader(new FileInputStream(quadfile), encoding);
		BufferedReader bufferedReader = new BufferedReader(reader);
		String line = null;
		double x1 = 115.750000;
		double y1 = 39.500000;
		double x2 = 117.2;
		double y2 = 40.5;
		CellInfo Mbr = new CellInfo(0, x1, y1, x2, y2);
		QuadTreeIndex quad = new QuadTreeIndex(Mbr);

		while ((line = bufferedReader.readLine()) != null) {
			String[] str = line.split("\\s+");
			double X1 = Double.valueOf(str[1]);
			double Y1 = Double.valueOf(str[2]);
			double X2 = Double.valueOf(str[3]);
			double Y2 = Double.valueOf(str[4]);
			QuadTreeNode qinfo = new QuadTreeNode(X1, Y1, X2, Y2);
			System.out.println(line);
			quad.insert(qinfo);
		}
		bufferedReader.close();

		// 遍历
		Vector<QuadTreeNode> qNodes = new Vector<QuadTreeNode>();
		quad.TraverseQuadTree(qNodes, quad.root);
		for (int i = 0; i < qNodes.size(); i++) {
			if (qNodes.get(i).hasChirdNode == false) {
				System.out.println(qNodes.get(i).cellId + " " + qNodes.get(i).layer + " " + qNodes.get(i).x1 + " "
						+ qNodes.get(i).y1 + " " + qNodes.get(i).x2 + " " + qNodes.get(i).y2);
			}

		}

		System.out.println("finished!!!");

	}

}
