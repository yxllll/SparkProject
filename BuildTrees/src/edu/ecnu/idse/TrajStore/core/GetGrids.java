package edu.ecnu.idse.TrajStore.core;

import java.io.IOException;

/**
 * 定义点信息
 * @author yang
 *
 */
//点数据结构


public class GetGrids {
	
	//左下坐标点，右上坐标
	public double lng1;
	public double lat1;
	public double lng2;
	public double lat2;

	double sublng [];
	double sublat [];
	
	public GetGrids(){
		
		this.lng1 = 0;
		this.lat1 = 0;
		this.lng2 = 0;
		this.lat2 = 0;
		this.sublat = null;
		this.sublng = null;
		
	}
	
	public GetGrids(double x1, double y1, double x2, double y2){
		this.lng1 = x1;
		this.lat1 = y1;
		this.lng2 = x2;
		this.lat2 = y2;
	}
	
	//将地图划分为m*n的小块
	public void setGrid(double x1, double y1, double x2, double y2, int m, int n){

		//m是经度划分的份数；n是纬度划分的份数
		this.lng1 = x1;
		this.lat1 = y1;
		this.lng2 = x2;
		this.lat2 = y2;
		
		double[] array1 = new double[m+1];	//因为将经纬度切分后计算刻度时会比切分的份数多1
		double[] array2 = new double[n+1];
		this.sublng = array1;
		this.sublat = array2;
		
		double a , b;
		a = (x2-x1)/m;
		b = (y2-y1)/n;
		
		for (int i=0;i < m;i++ ){
			sublng[i] = lng1 + a*i;
		}
		
		for (int i=0;i < n;i++ ){
			sublat[i] = lat1 + b*i;
		}
		
		sublng[m] = x2;
		sublat[n] = y2;
	}
	
}

