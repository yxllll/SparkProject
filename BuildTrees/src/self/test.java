package self;

import java.util.HashSet;
import java.util.Set;

public class test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String  a = "2014-12-11 12:12:12";
		String b = "2014-12-11 121:11:11";
		Set<String> set = new HashSet<String>();
		set.add(a.substring(0,10));
		set.add(b.substring(0,10));
		System.out.println(a.substring(0,10));
		System.out.println(set.size());
		
	}

}
