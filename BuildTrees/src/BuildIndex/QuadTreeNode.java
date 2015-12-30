package BuildIndex;

import edu.ecnu.idse.TrajStore.core.CellInfo;
import edu.ecnu.idse.TrajStore.core.Point;

public class QuadTreeNode extends CellInfo {
	public static final int NODE_NUMS = 4;
	public static final int NW = 0, NE = 1, SW = 2, SE = 3;
	/*
	 * a quadrant defined below: 
	 * 		NW(0) | NE(1)
	 * -----------|----------- 
	 * 		SW(2) | SE(3)
	 */

	private static int Num = 0;
	public short layer;
	public boolean hasChirdNode;
	public QuadTreeNode[] node;

	public QuadTreeNode(double xx1, double yy1, double xx2, double yy2) {
		this.x1 = xx1;
		this.x2 = xx2;
		this.y1 = yy1;
		this.y2 = yy2;
		this.cellId = Num;
		hasChirdNode = false;
		node = null;
		Num++;
	}

	public void InitQuadIndex(CellInfo cellinfo) {
		this.x1 = cellinfo.x1;
		this.x2 = cellinfo.x2;
		this.y1 = cellinfo.y1;
		this.y2 = cellinfo.y2;
		this.cellId = Num;
		hasChirdNode = false;
		node = null;
		Num++;
	}

	public void CreatChird() {
		double x1 = this.x1;
		double x2 = this.x2;
		double y1 = this.y1;
		double y2 = this.y2;
		double width = (x2 - x1) / 2;
		double hight = (y2 - y1) / 2;
		this.hasChirdNode = true;
		this.node = new QuadTreeNode[NODE_NUMS];

		this.node[0] = new QuadTreeNode(x1, y1 + hight, x1 + width, y2);

		this.node[1] = new QuadTreeNode(x1 + width, y1 + hight, x2, y2);

		this.node[2] = new QuadTreeNode(x1, y1, x1 + width, y1 + hight);

		this.node[3] = new QuadTreeNode(x1 + width, y1, x2, y1 + hight);

		for (int i = 0; i < node.length; i++) {
			node[i].layer = (short) (this.layer + 1);
		}
	}

	public boolean HasChild() {
		return hasChirdNode;

	}

	public void insertNode(QuadTreeNode qtree, CellInfo qinfo) {

		System.out.println(qtree.toString());
		if (qtree.hasChirdNode == false) {
			qtree.CreatChird();
		}

		int dir = -1;
		Point cPoint = qinfo.getCenterPoint();
		System.out.println(cPoint.toString());

		for (int i = 0; i < NODE_NUMS; i++) {
			if (qtree.node[i].contains(cPoint)) {
				dir = i;
				System.out.println(qtree.node[i].toString());
				break;
			}
		}
		System.out.println(qtree.layer);
		if (!qtree.node[dir].CellEqual(qinfo)) {
			insertNode(qtree.node[dir], qinfo);
		}

	}

	public boolean CellEqual(CellInfo q) {
		double m1 = this.x1;
		double m2 = this.y1;
		double m3 = this.x2;
		double m4 = this.y2;
		double n1 = q.x1;
		double n2 = q.y1;
		double n3 = q.x2;
		double n4 = q.y2;
		double err = 0.000001;
		System.out.println("lb x diff = " + Math.abs(n1 - m1));
		System.out.println("lb y diff =  " + Math.abs(n2 - m2));
		System.out.println("rt x diff = " + Math.abs(n3 - m3));
		System.out.println("rt y diff =  " + Math.abs(m4 - n4));
		if ((Math.abs(n1 - m1) < err) && (Math.abs(n2 - m2) < err) && (Math.abs(n3 - m3) < err)
				&& (Math.abs(n4 - m4) < err)) {
			return true;
		} else {
			return false;
		}
	}

}
