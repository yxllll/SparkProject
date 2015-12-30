package edu.ecnu.idse.TrajStore.operations;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ProgramDriver;

public class Main {

	/**
	 * @param args
	 */
	static{
		Configuration.addDefaultResource("core-site.xml");
		Configuration.addDefaultResource("hdfs-site.xml");
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();
		try {
			pgd.addClass("index", Repartition.class, "Builds an index on an input file");
			pgd.addClass("index", Indexer.class, "Spatially partition a file using a specific partitioner");
			exitCode = 0;
			
		} catch (Throwable e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		System.exit(exitCode);
	}

}
