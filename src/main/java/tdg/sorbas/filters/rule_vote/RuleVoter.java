package tdg.sorbas.filters.rule_vote;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import tdg.link_discovery.middleware.objects.Tuple;
import tdg.sorbas.cache.H2Cache;
import tdg.sorbas.filters.AbstractSorbasFilterManager;

public class RuleVoter extends AbstractSorbasFilterManager {

	
	/*
	 * Generate the table: rule, nº of links
	 */
	public RuleVoter(Double threshold, String inputTable) {
		super("d", threshold, inputTable);
		this.metric = new RuleVoteMetric(inputTable, this.metricTable, this.filterName);
		RuleVoteFilter.originalInputTable = inputTable;
		this.filter = new RuleVoteFilter(threshold, this.metricTable, this.filterTable, this.filterName);
	
	}

	public Map<Tuple<String,String>,String> filteredRules(double threshold){
		Map<Tuple<String,String>,String> resultSupportingRules = new HashMap<>();
		Connection connection = null;
		PreparedStatement statement = null;
		ResultSet queryResult = null;		
		StringBuffer query = new StringBuffer();
		query.append("SELECT NEIGHBOR_SOURCE_PATH, NEIGHBOR_TARGET_PATH, SUPPORTING_RULE, D FROM ").append(this.metricTable);
		Double maxD = RuleVoteFilter.maxD;
		try {
			// Create connection
			connection = H2Cache.getConnection();
			connection.setAutoCommit(false);
			// Prepare query to retrieve stored links
			statement = connection.prepareStatement(query.toString());
			statement.setFetchSize(10);
			// Execute query
			queryResult = statement.executeQuery();
			while (queryResult.next()) {
				// Retrieve supporting rules filtered
				String supportingSourcePath = queryResult.getString("NEIGHBOR_SOURCE_PATH");
				String supportingTargetPath = queryResult.getString("NEIGHBOR_TARGET_PATH");
				String supportingRule = queryResult.getString("SUPPORTING_RULE");
				Double value = Double.valueOf(queryResult.getString("D"))/maxD;
				if(value >= threshold)
					resultSupportingRules.put(new Tuple<String,String>(supportingSourcePath, supportingTargetPath),supportingRule);
			}

			// Close connection, statement, and query results
			queryResult.close();
			statement.close();
			connection.close();
	
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return resultSupportingRules;
	}
	
	
}
