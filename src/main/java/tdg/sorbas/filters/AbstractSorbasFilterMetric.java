package tdg.sorbas.filters;

import tdg.sorbas.cache.H2Cache;

public abstract class AbstractSorbasFilterMetric implements SorbasFilterComponent{


	protected String inputTable, resultsTable;
	protected String metricName;
	
	public AbstractSorbasFilterMetric(String inputTable, String resultsTable, String filterMetricName) {
		
		if(filterMetricName==null || filterMetricName.isEmpty() ||inputTable==null || inputTable.isEmpty() || resultsTable==null || resultsTable.isEmpty())
			throw new IllegalArgumentException();
		
		this.inputTable = inputTable;
		this.metricName = filterMetricName;
		this.resultsTable = resultsTable;
	
	}

	
	public String getResultsTable() {
		return resultsTable;
	}
		
	
	protected void execute(String query) {
		
		StringBuffer viewQuery = null;
		// init view query
		viewQuery = new StringBuffer();
		viewQuery.append("CREATE VIEW ").append(resultsTable).append(" AS ").append(query).append(" ");	
		// execute query to create a view
		H2Cache.updateTableQuery(viewQuery.toString());
		
	}
	
}
