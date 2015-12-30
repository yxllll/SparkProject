package self;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class RegexPathFilter implements PathFilter{
	private final String regex;
	public RegexPathFilter(String regex){
		this.regex = regex;
	}
	
	@Override 
	public boolean accept(Path path){
		return path.toString().endsWith(".gz");
	}
}
