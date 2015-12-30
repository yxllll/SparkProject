package edu.ecnu.idse.TrajStore.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;


public class IterativelyQuery {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		File root = new File("/home/zzg/workspace/TrajStore/src/");
	//	File root = new File("/home/zzg/workspace/TrajStore/src/edu/ecnu/idse/TrajStore");
		String queryWords="master file is null !!!!";
	try{
			RecursivelyQuery(root,queryWords);
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}
	
	public static void RecursivelyQuery(File dir, String queryWords) throws Exception{
		File[] fs = dir.listFiles();
		for(int i=0;i<fs.length;i++){
			if(fs[i].isDirectory()){
				try{
					RecursivelyQuery(fs[i],queryWords);
				}catch (Exception e){
					e.printStackTrace();
				}
			}else{
				QueryWord(fs[i], queryWords);
			}
		}
	}

	public static void QueryWord(File file, String queryWord) throws Exception{
		BufferedReader bReader = null;
		try {
			bReader = new BufferedReader(new FileReader(file));
			String line = null;
			int lineNumber = 0;
			while((line = bReader.readLine())!=null){
				if(line.contains(queryWord)){
					System.out.println(file.getName()+" "+lineNumber+": "+line);
				}
				lineNumber++;
			}
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}finally{
			bReader.close();
		}
	}
}
