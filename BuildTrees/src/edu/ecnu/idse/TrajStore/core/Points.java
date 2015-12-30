package edu.ecnu.idse.TrajStore.core;

import java.io.IOException;

public class Points {

	public double lat;
	public double lng;
	
	double lowboundlng;
	double lowboundlat;
	double upboundlng;
	double upboundlat;
	
	int value;
	
	public Points(double x, double y){
		this.lat = x;
		this.lng = y;
	}
	
	public Points() {
		this.lat = 0;
		this.lng = 0;
		this.upboundlat = 0;
		this.upboundlng = 0;
		this.lowboundlng = 0;
		this.lowboundlat = 0;
		this.value = 0;
	}
	
	public void setPoint (double x, double y, int n) throws IOException{
		this.lng = x;
		this.lat = y;
		this.value = n;
	}
	
	public Points (Points point){
		this.lng = point.lng;
		this.lat = point.lat;
		this.upboundlng = point.upboundlng;
		this.upboundlat = point.upboundlat;
		this.lowboundlng = point.lowboundlng;
		this.lowboundlat = point.lowboundlat;
		this.value = point.value;
	}
	
	public double getlat() {
		return lat;
		
	}
	public double getlng() {
		return lng;
	}
	public int getvalue() {
		return value;
	}
	
	public void getPoint(double x1, double y1, double x2, double y2, int val) {
		this.lng = (x1+x2)/2;
		this.lat = (y1+y2)/2;
		this.value = val;
	}
	
	public void getcore() {
		this.lng = (this.lowboundlng + this.upboundlng)/2;
		this.lat = (this.lowboundlat + this.upboundlat)/2;
	}
}
