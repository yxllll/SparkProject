package edu.ecnu.idse.TrajStore.Test;

import org.apache.hadoop.io.Text;

public class TestTool {

	public static void main(String [] main){
		TestText();
	}
	
	public static  void TestText(){
		byte[] bs = new byte[]{'a','b','c','d'};
		Text text = new Text();
		text.set(bs, 0, 2);
		System.out.println(text.toString());
	}
}
