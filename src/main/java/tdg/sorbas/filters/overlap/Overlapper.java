package tdg.sorbas.filters.overlap;

import tdg.sorbas.filters.AbstractSorbasFilterManager;

public class Overlapper extends AbstractSorbasFilterManager{

	public Overlapper(Double threshold) {
		super("overlap", threshold, "NEIGHBORS_LINKED");
		
		this.metric = new OverlapMetric( this.inputTable, this.metricTable, this.filterName);
		this.filter = new OverlapFilter(threshold, this.metricTable, this.filterTable, this.filterName);
		
	}

	
	
}
