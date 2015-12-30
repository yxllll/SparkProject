package BuildIndex;

import java.util.Vector;

import edu.ecnu.idse.TrajStore.core.CellInfo;

public class QuadTreeIndex {

	public QuadTreeNode root;
	public int depth;
	public static int debugNum;

	/* Init a QuadTree Index */
	public QuadTreeIndex(CellInfo cell) {
		depth = 1;
		root = new QuadTreeNode(cell.x1, cell.y1, cell.x2, cell.y2);
		root.InitQuadIndex(cell);
		root.layer = 0;
		root.hasChirdNode = false;
	}

	/* inserts a node into QuadTree */
	public void insert(QuadTreeNode qinfo) {
		root.insertNode(root, qinfo);
	}

	/* Preorder Traversal a QuadTree */
	public void TraverseQuadTree(Vector<QuadTreeNode> record, QuadTreeNode qnode) {
		if (qnode != null) {
			record.addElement(qnode);
			if (qnode.hasChirdNode) {
				TraverseQuadTree(record, qnode.node[0]);
			}
			if (qnode.hasChirdNode) {
				TraverseQuadTree(record, qnode.node[1]);
			}
			if (qnode.hasChirdNode) {
				TraverseQuadTree(record, qnode.node[2]);
			}
			if (qnode.hasChirdNode) {
				TraverseQuadTree(record, qnode.node[3]);
			}
		}
	}

}
