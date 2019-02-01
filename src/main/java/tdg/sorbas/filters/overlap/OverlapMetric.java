package tdg.sorbas.filters.overlap;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.jena.ext.com.google.common.collect.Lists;
import org.apache.jena.ext.com.google.common.collect.Sets;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import tdg.link_discovery.middleware.objects.Tuple;
import tdg.sorbas.cache.H2Cache;
import tdg.sorbas.filters.AbstractSorbasFilterMetric;

public class OverlapMetric extends AbstractSorbasFilterMetric{

	public OverlapMetric(String inputTable, String resultsTable, String filterMetricName) {
		super(inputTable, resultsTable, filterMetricName);
		
	}
	
	private void initOverlapMetricTable() {
		Connection connection = null;
		PreparedStatement statement = null;
		try{
			// Create connection
			connection = H2Cache.getConnection();
			connection.setAutoCommit(false);
			// Create overlapping table
			statement = connection.prepareStatement("CREATE TABLE IF NOT EXISTS "+this.resultsTable+" ( "+
					" id int PRIMARY KEY AUTO_INCREMENT,\n" + 
					" rule varchar(1000),\n" + 
					" iri_source varchar(1000),\n" + 
					" iri_target varchar(1000),\n "+
					" neighbor_source_path varchar(1000) ,\n" + 
					" neighbor_target_path varchar(1000) ,\n" +
					" supporting_rule varchar(1000),\n" + 
					this.metricName+" double) ");
			
			// Execute query
			statement.executeUpdate();
			
			// Close connection and statement
			statement.close();
			connection.close();
	
		} catch(SQLException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public String applyFilter() {
		
		//create table if not exists; other classes are in charge of dropping it before calling this function
		initOverlapMetricTable();
		
		Connection connection = null; 
		PreparedStatement statement = null;
		ResultSet queryResult = null;
		
		try {
			// Create connection
			connection = H2Cache.getConnection();
			connection.setAutoCommit(false);
			// Prepare query to retrieve stored neighbor links
			statement = connection.prepareStatement("SELECT DISTINCT source_iri, target_iri, neighbor_source_path, neighbor_target_path, supporting_rule, main_rule FROM "+inputTable);
			statement.setFetchSize(10000);
			// Execute query
			queryResult =  statement.executeQuery();
			while (queryResult.next()){
				// Retrieve instances
				String iriSource = queryResult.getString("source_iri");
				String iriTarget = queryResult.getString("target_iri");
				String sourcePath = queryResult.getString("neighbor_source_path");
				String targetPath = queryResult.getString("neighbor_target_path");
				String rule = queryResult.getString("main_rule");
				String supportingRule = queryResult.getString("supporting_rule");
				// Apply overlap and store score in respective table
				Double overlapScore = retrieveNeighbourIris(iriSource, iriTarget, sourcePath, targetPath, supportingRule);
				addLinkWithOverlapping(rule, iriSource, iriTarget, sourcePath, targetPath, supportingRule, overlapScore);	
			}
			
			// Close connection, statement, and query results
			queryResult.close();
			statement.close();
			connection.close();
			
		} catch(SQLException e) {
			e.printStackTrace();
		}
		
		return this.resultsTable;
	}
	
	
	
	private Double retrieveNeighbourIris(String iriSource, String iriTarget, String sourcePath, String targetPath, String supportingRule) {
		Connection connection = null; 
		PreparedStatement statement = null;
		ResultSet queryResult = null;
		Integer neighborsSourceSize =  null;
		Integer neighborsTargetSize =  null;
		Set<Tuple<String,String>> neighbors =  null;
		Double overlapScore = null;
		
		neighborsSourceSize = 0;
		neighborsTargetSize = 0;
		neighbors = Sets.newHashSet();
		try {
			// Create connection
			connection = H2Cache.getConnection();
			connection.setAutoCommit(false);
			// Prepare query to retrieve stored neighbor links
			statement = connection.prepareStatement("SELECT DISTINCT neighbor_source_iri, neighbor_target_iri, neighbor_source_size, neighbor_target_size   FROM "+this.inputTable+" WHERE source_iri='"+fixH2Literals(iriSource)+"' and target_iri='"+fixH2Literals(iriTarget)+"' and supporting_rule='"+fixH2Literals(supportingRule)+"' and neighbor_source_path='"+fixH2Literals(sourcePath)+"' and neighbor_target_path='"+fixH2Literals(targetPath)+"'");
			statement.setFetchSize(10000);
			// Execute query
			queryResult =  statement.executeQuery();
			while (queryResult.next()){
				// Retrieve neighbors of a given two pair of resources
				String neighborSourceIri = queryResult.getString("neighbor_source_iri");
				String neighborTargetIri = queryResult.getString("neighbor_target_iri");
				String neighborSourceSize = queryResult.getString("neighbor_source_size");
				String neighborTargetSize = queryResult.getString("neighbor_target_size");
				
				neighborsSourceSize = Integer.valueOf(neighborSourceSize);
				neighborsTargetSize = Integer.valueOf(neighborTargetSize);
				neighbors.add(new Tuple<String,String>(neighborSourceIri,neighborTargetIri));
			}
			
			// Close connection, statement, and query results
			queryResult.close();
			statement.close();
			connection.close();
			
			//Compute overlap
			overlapScore = getOverlapScore(neighborsSourceSize, neighborsTargetSize, neighbors);
			
		} catch(SQLException e) {
			e.printStackTrace();
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		return overlapScore;
	}

	

	public Double getOverlapScore(Integer sizeSetA, Integer sizeSetB, Set<Tuple<String,String>> irisLinked) throws Exception {
		Multimap<String,String> sourceLinks = ArrayListMultimap.create(); // initialization required by lambda expression
		Multimap<String,String> targetLinks = ArrayListMultimap.create(); // initialization required by lambda expression
		Integer modifiedSizeA = null;
		Integer modifiedSizeB = null;
		Integer intersection = null;
		Double overlapScore = null;
		
		irisLinked.stream().forEach(link -> sourceLinks.put(link.getFirstElement(), link.getSecondElement()));
		irisLinked.stream().forEach(link -> targetLinks.put(link.getFirstElement(), link.getSecondElement()));
		
		List<Set<String>> equivalencesSource = transitiveClousureRequired(sourceLinks);
		List<Set<String>> equivalencesTarget= transitiveClousureRequired(targetLinks);
		if(equivalencesSource.size() == equivalencesTarget.size()) {
			intersection = equivalencesSource.size();
			modifiedSizeA = adjustNeighborsSizesFromLinks(sizeSetA, equivalencesSource);
			modifiedSizeB = adjustNeighborsSizesFromLinks(sizeSetB, equivalencesTarget);
			if(intersection> modifiedSizeA || intersection>modifiedSizeB) {
				System.out.println("Error in overlap, check the transitive clouse [002]");
				throw new Exception();
			}else {
				overlapScore = metric(modifiedSizeA, modifiedSizeB, intersection);
			}
			
		}else {
			System.out.println("Error in overlap, check the transitive clouse [001]");
			throw new Exception();
		}
		
		
		return overlapScore;
	}
	
	
	
	private static Integer adjustNeighborsSizesFromLinks(Integer size, List<Set<String>> equivalences) {
		Integer adjustedSize = null;
		
		adjustedSize = size;
		for(Set<String> iris : equivalences) {
			adjustedSize += 1 - iris.size();
		}
		
		return adjustedSize;
	}
	
	private static List<Set<String>> transitiveClousureRequired(Multimap<String, String> links) {
		List<Set<String>> irisEquivalences = null;
		
		irisEquivalences = Lists.newArrayList();
		// Iterate over values if there is a key with the same value group
		for(String iriLiked: links.values()) {	
			Set<String> sameIris = links.entries().stream().filter(entry -> entry.getValue().equals(iriLiked)).map(entry -> entry.getKey()).collect(Collectors.toSet());
			Integer index = getSameIrisIndex(irisEquivalences, sameIris);
			if(index!=null) {
				irisEquivalences.get(index).addAll(sameIris);
			}else {
				irisEquivalences.add(sameIris);
			}
		}
		return irisEquivalences;
	}


	private static Integer getSameIrisIndex(List<Set<String>> irisEquivalences, Set<String> sameIris) {
		Integer index = null;
		
		for(Set<String> equivalences:irisEquivalences) {
			Boolean contained = equivalences.stream().anyMatch( iri -> sameIris.contains(iri));
			if(contained)
				index  =irisEquivalences.indexOf(equivalences);
		}
		
		return index;
	}
	

	public Double metric(Integer a, Integer b, Integer intersection) {
		return intersection*1.0/Math.min(a, b);
	}


	public void addLinkWithOverlapping(String rule, String iriSource, String iriTarget, String sourcePath, String targetPath,String supporting_rule, Double overlapping)  {
		Connection connection = null;
		PreparedStatement statement = null;
		try {
			// Create connection
			connection = H2Cache.getConnection();
			connection.setAutoCommit(true); // set commit to true to save data
			// Prepare query to insert new data
			statement = connection.prepareStatement("INSERT INTO "+this.resultsTable+" (rule, iri_source, iri_target, neighbor_source_path, neighbor_target_path, supporting_rule, "+this.metricName+") VALUES ('"+fixH2Literals(rule)+"', '"+fixH2Literals(iriSource)+"', '"+fixH2Literals(iriTarget)+"', '"+fixH2Literals(sourcePath)+"', '"+fixH2Literals(targetPath)+"', '"+fixH2Literals(supporting_rule)+"', '"+overlapping+"') ");
			// Execute query
			if(overlapping!=null)
				statement.executeUpdate();
			// Close connection and statement
			statement.close();
			connection.close();
			
		} catch(SQLException e) {
			e.printStackTrace();
		}
	}

	private String fixH2Literals(String literalToInsert) {
		return literalToInsert.replace("'", "''");
	}
	
}
