package edu.ecnu.idse.TrajStore.core;

/*
 * @author zzg
 */
public interface ResultCollector2<R,S> {

	public void collect(R r, S s);
}
