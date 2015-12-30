package edu.ecnu.idse.TrajStore.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

/*
 * 根据网格的点数，按照一定比例缩小点数，并且随机生成这些点
 * 
 * 
 */
public class GetRandomPoints {

	public GetRandomPoints() {
		
	}
	
	public Point[] getRandomPoint(Points[] points, int rate) throws IOException {
		
		java.util.List<Point> pointlist = new ArrayList<Point>();
		double lng, lat, x, y;
		lng = points[0].upboundlng - points[0].lowboundlng;
		lat = points[0].upboundlat - points[0].lowboundlat;
		Random ran = new Random();
		for (int i = 0; i < points.length; i++) {
			int val = (int)((points[i].value+rate/2)/rate);
			for (int j = 0; j < val; j++) {
				x = points[i].lowboundlng + ran.nextDouble()*lng;
				y = points[i].lowboundlat + ran.nextDouble()*lat;
				pointlist.add(new Point(x, y, 0));
			}
			
		}
		
		
		//记录有用点
//		Point [] finalpoints; 
//		finalpoints = pointlist.toArray(new Point[pointlist.size()]);
//		java.text.DecimalFormat df = new java.text.DecimalFormat("#.000000");
//		FileWriter writer = new FileWriter("/home/yang/data/allpoints/finalpoints.txt");
//		BufferedWriter bw = new BufferedWriter(writer);
//		String x1,y1;
//		String strings[] = new String[100000];
//		for (int i = 0; i < finalpoints.length; i++) {
//			x1 = df.format(finalpoints[i].x);
//			y1 = df.format(finalpoints[i].y);
//			strings[i] = x1 + " " + y1 + "\n";
//			bw.write(strings[i]);
//		}
//		bw.close();
//		writer.close();
		
				
		return pointlist.toArray(new Point[pointlist.size()]);	
	}
	
}
