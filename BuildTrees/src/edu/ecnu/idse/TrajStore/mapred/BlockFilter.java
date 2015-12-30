package edu.ecnu.idse.TrajStore.mapred;

import org.apache.hadoop.mapred.JobConf;

import edu.ecnu.idse.TrajStore.core.GlobalIndex;
import edu.ecnu.idse.TrajStore.core.Partition;
import edu.ecnu.idse.TrajStore.core.ResultCollector;
import edu.ecnu.idse.TrajStore.core.ResultCollector2;

public interface BlockFilter {

	public void configure(JobConf job);
	
	/*
	 * select the blocks that need to be processed before a MapReduce job
	 */
	public void selectCells(GlobalIndex<Partition> gIndex, 
				ResultCollector<Partition> output);
	
	public void selectCellPairs(GlobalIndex<Partition> gIndex1,
			GlobalIndex<Partition> gIndex2, 
			ResultCollector2<Partition, Partition> output);
}
