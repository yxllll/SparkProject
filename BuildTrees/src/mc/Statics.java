package mc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

public class Statics {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		File file = new File("out");
		BufferedReader bReader = new BufferedReader(new FileReader(file));
		String line = null;
		int MOD  =1000;
		int dis = 0;
		int value = 0;
		Map<Integer, Integer> map = new HashMap<>();
		while((line = bReader.readLine()) != null){
			dis = Math.round(Float.parseFloat(line));
			// count every MOD meters
			if(map.containsKey(dis/MOD)){
				value = map.get(dis/MOD);
				map.put(dis/MOD, value+1);
			}else{
				map.put(dis/MOD, 1);
			}
		}
		bReader.close();
		int toatalNum = 0;
		float Less2 = 0, Less4=0, Less6=0, Less8=0,Less10=0, over10=0 ;
		
		for(java.util.Map.Entry<Integer,Integer> entry : map.entrySet()){
			System.out.println(entry.getKey()+"	"+entry.getValue());
			toatalNum += entry.getValue();
		}
		Less2=map.get(0)+map.get(1);
		Less4=map.get(2)+map.get(3);
		Less6=map.get(4)+map.get(5);
		Less8=map.get(6)+map.get(7);
		Less10=map.get(8)+map.get(9);
		over10 = toatalNum - Less2-Less4-Less6-Less8-Less10;
		System.out.println(Less2/toatalNum);
		System.out.println(Less4/toatalNum);
		System.out.println(Less6/toatalNum);
		System.out.println(Less8/toatalNum);
		System.out.println(Less10/toatalNum);
		System.out.println(over10/toatalNum);
	}

}
