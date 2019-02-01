package tdg.sorbas.filters;

import tdg.sorbas.cache.H2Cache;

public abstract class AbstractSorbasFilter implements SorbasFilterComponent{

	protected Double threshold;
	protected String inputTable, resultsTable;
	protected String filterName;
	
	public AbstractSorbasFilter(Double threshold, String inputTable, String resultsTable, String filterName) {
		
		if(threshold==null || filterName==null || filterName.isEmpty() ||inputTable==null || inputTable.isEmpty() || resultsTable==null || resultsTable.isEmpty())
			throw new IllegalArgumentException();
		
		this.inputTable = inputTable;
		this.filterName = filterName;
		this.resultsTable = resultsTable;
		
		this.threshold = threshold;
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
