package edu.ecnu.idse.TrajStore.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sun.org.apache.xalan.internal.xsltc.compiler.sym;

public class DataFormatCheck extends Configured implements Tool{

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		ToolRunner.run(new DataFormatCheck(), args);
	}
	
	
	/*
	 * check data with grid indexes
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
	//	FSDataInputStream fin = fs.open(new Path("/home/zzg/testGridIndex/_master.grid"));
		FSDataInputStream fin = fs.open(new Path("/home/zzg/testGridIndex/part-00000_data_00000"));
		BufferedReader in = null;
		String line;
		int count  = 0;
		String [] tokens  = null;
		float xMax =0, yMax =0, zMax =0;
		float xMin =-1000f, yMin =-1000f, zMin =-1000f;
		float x,y,z;
		System.out.println("begin to run");
		try {
			in = new BufferedReader(new InputStreamReader(fin));
			while ((line = in.readLine())!=null) {
				tokens = line.split(",");
				x = Float.parseFloat(tokens[0]);
				y = Float.parseFloat(tokens[1]);
				z = Float.parseFloat(tokens[2]);
				if(x > xMax){
					xMax = x;
				}
				if(y > yMax){
					yMax = y;
				}
				if(z > zMax){
					zMax = z;
				}
		//		System.out.println(line);
				count++;
				if(count%1000000 == 0){
					System.out.println("count :"+count);
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			in.close();
		}
		System.out.println("x_MAX: "+xMax);
		System.out.println("y_MAX: "+ yMax);
		System.out.println("z_MAX: "+zMax);
		System.out.println(count);
		return 1;
	}
	
	/* 
	 * ckeck data without indexes
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
/*	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream fin = fs.open(new Path("/home/zzg/test/test/part-00000_data_00001"));
		BufferedReader in = null;
		String line;
		int count  = 0;
		String [] tokens  = null;
		float xMax =0, yMax =0, zMax =0;
		float x,y,z;
		System.out.println("begin to run");
		try {
			in = new BufferedReader(new InputStreamReader(fin));
			while ((line = in.readLine())!=null) {
				tokens = line.split(",");
				x = Float.parseFloat(tokens[0]);
				y = Float.parseFloat(tokens[1]);
				z = Float.parseFloat(tokens[2]);
				if(x > xMax){
					xMax = x;
				}
				if(y > yMax){
					yMax = y;
				}
				if(z > zMax){
					zMax = z;
				}
		//		System.out.println(line);
				count++;
				if(count%1000000 == 0){
					System.out.println("count :"+count);
				}
				
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			
		}finally{
			if(in !=null)
				in.close();
		}
		System.out.println("x_MAX: "+xMax);
		System.out.println("y_MAX: "+ yMax);
		System.out.println("z_MAX: "+zMax);
		System.out.println(count);
		return 1;
	}
*/
}
