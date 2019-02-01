package tdg.sorbas.filters.rule_vote;

import tdg.sorbas.filters.AbstractSorbasFilterMetric;

public class RuleVoteMetric extends AbstractSorbasFilterMetric{

	public RuleVoteMetric(String inputTable, String resultsTable, String filterMetricName) {
		super(inputTable, resultsTable, filterMetricName);
	}

	/*
	 * VERSION 1.0 - NO PATH ACUMULATOR
	
	@Override
	public String applyFilter() {
		StringBuffer queryMetricFilter = null;
		// Prepare query to create a new view with the filter metric
		queryMetricFilter = new StringBuffer();
		queryMetricFilter.append("SELECT  SUPPORTING_RULE, COUNT(*) AS ").append(this.metricName);
		queryMetricFilter.append(" FROM ").append(this.inputTable).append(" GROUP BY SUPPORTING_RULE");
		// Execute query
		this.execute(queryMetricFilter.toString());
		
		return this.resultsTable;
	} */

	@Override
	public String applyFilter() {
		StringBuffer queryMetricFilter = null;
		// Prepare query to create a new view with the filter metric
		queryMetricFilter = new StringBuffer();
		queryMetricFilter.append("SELECT NEIGHBOR_SOURCE_PATH, NEIGHBOR_TARGET_PATH, SUPPORTING_RULE, COUNT(*) AS ").append(this.metricName);
		queryMetricFilter.append(" FROM ").append(this.inputTable).append(" GROUP BY SUPPORTING_RULE, NEIGHBOR_SOURCE_PATH, NEIGHBOR_TARGET_PATH");
		// Execute query
		this.execute(queryMetricFilter.toString());
		
		return this.resultsTable;
	}
	
}
