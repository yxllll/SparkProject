package mc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;


public class ReadDistance {
	private static final double EARTH_RADIUS = 6378137.0;

	/*
	public static void main(String []args){
		
		try{
			double dis = Distance("121.40925322638617,31.218578391604954", "121.5730972290039,31.144390106201172");
			System.out.println(dis);
		}catch(Exception e){
			System.out.println(e);
		}
		
	}
	*/
	
	public static void main(String []args) throws Exception{
		File liveFile = new File("/home/zzg/workspace/LWDistance/lw");
		File workFile = new File("/home/zzg/workspace/LWDistance/w");
		File dis = new File("out");
		Map<String,String> liveMap = new HashMap<>();
		
		BufferedReader br = new BufferedReader(new FileReader(liveFile));
		String line = null;
		String [] tokens = null;
		while((line = br.readLine())!=null){
			tokens = line.split("\t");
			liveMap.put(tokens[0], tokens[1]);
		}
		System.out.println("load finished");
		br.close();
		
		BufferedReader br1 = new BufferedReader(new FileReader(workFile));
		BufferedWriter bw = new BufferedWriter(new FileWriter(dis));
		double distance= 0.0;
		int count = 0;
		String [] works = null;
		double maxDis = 0;
		while((line = br1.readLine())!=null){
			tokens = line.split("\t");
			count++;
			if(liveMap.containsKey(tokens[0])){
				String liveString  = liveMap.get(tokens[0]);
				String [] plcaes = liveString.split(";");
				maxDis = 0;
				for(String p:plcaes){
					try {
						works = tokens[1].split(";");
						for(int i=0;i<works.length;i++){
							distance =	Distance(p, works[i]);
							if(distance > maxDis){
								maxDis = distance;
							}
						}
					
					} catch (Exception e) {
						// TODO: handle exception
						e.printStackTrace();
					}
					bw.write(maxDis+"\n");
				}
				liveMap.remove(tokens[0]);
			}
			if(count %100000==0){
				System.out.println(count);
			}
		}
		
		br1.close();
		bw.close();
		System.out.println("finished!");
	}
	
	public static double Distance(String begin, String end) throws Exception{
		String [] tokens = begin.split(",");
		double xbegin = Double.parseDouble(tokens[0]);
		double ybegin = Double.parseDouble(tokens[1]);
		tokens = end.split(",");
		double xend = Double.parseDouble(tokens[0]);
		double yend = Double.parseDouble(tokens[1]);
		
		double radLat1 = (xbegin * Math.PI/180.0);
		double radLat2 = (xend * Math.PI/180.0);
		
		double a = radLat1 -radLat2;
		double b = (ybegin -yend) * Math.PI /180.0;
		double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a/2), 2)
				+ Math.cos(radLat1)* Math.cos(radLat2)*Math.pow(Math.sin(b/2),2)));
		
		s = s * EARTH_RADIUS;
		s = Math.round(s*10000)/10000;
		return s;
	}
}
