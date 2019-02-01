package tdg.sorbas;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.jena.ext.com.google.common.collect.Sets;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.tdb.TDBFactory;
import tdf.pathfinder.model.Path;
import tdg.link_discovery.connector.sparql.evaluator.arq.linker.factory.SPARQLFactory;
import tdg.link_discovery.framework.algorithm.individual.ISpecification;
import tdg.link_discovery.middleware.objects.Tuple;
import tdg.link_discovery.middleware.utils.FrameworkUtils;
import tdg.pathfinder.PathFinder;
import tdg.pathfinder.parameters.PathFinderParameters;
import tdg.sorbas.cache.H2Cache;
import tdg.sorbas.parameters.SorbasParameters;
import tdg.sorbas.rules.linker.LinkerK;
import tdg.sorbas.rules.translators.TranslatorStringRuleToSparql;

public class SorbasRL  {

	private SorbasParameters parameters;
	private PathFinderParameters  searchParameters;
	private LinkerK linker;
	private String goldStandard;
	
	public SorbasRL(SorbasParameters parameters, PathFinderParameters  searchParameters, String goldStandard) {
		if(parameters == null)
			throw new IllegalArgumentException("TeideParameters passed as argument cannot be null");
		
		this.parameters = parameters;
		this.searchParameters = searchParameters;
		this.goldStandard = goldStandard;
		// initialize the linker
		linker = new LinkerK();
		linker.setDatasetSource(this.parameters.getSourceDataset());
		linker.setDatasetTarget(this.parameters.getTargetDataset());
	}
	
	
	
	@SuppressWarnings("unchecked")
	public Set<Tuple<String, String>> execute() {
		Set<Path> sourcePaths = null;
		Set<Path> targetPaths = null;
		Set<Tuple<String,String>> filteredLinks = null;
		
		// Apply main rule
		filteredLinks = Sets.newHashSet();
		applyMainLinkRule();
		
		// Prune previous links relying on the supporting rules
		for(ISpecification<String> supporting : parameters.getSupportingRules()) {
			// Retrieve paths that connect mainRule with supporting
			sourcePaths = retrieveConnectingPaths(parameters.getSourceDataset(), parameters.getMainRule().getSourceRestrictions(), supporting.getSourceRestrictions());
			targetPaths = retrieveConnectingPaths(parameters.getTargetDataset(), parameters.getMainRule().getTargetRestrictions(), supporting.getTargetRestrictions());
			
			// If paths were found
			if(!sourcePaths.isEmpty() && !targetPaths.isEmpty()) {
				// Combine every possible path
				Set<List<Path>> navigablePaths = Sets.cartesianProduct(sourcePaths,targetPaths);
			
				for(List<Path> paths : navigablePaths) {
					// link neighbors
					linkNeighbors(supporting, paths.get(0),  paths.get(1));
				}
			}
		}
	
		
		
		return filteredLinks;
	}
	





	/** 
	 *  Applies the main rule and stores its links if required
	 */
	private void applyMainLinkRule() {
		TranslatorStringRuleToSparql translator = null;
		Tuple<String,String> sparqlQueries = null;
		
		translator = new TranslatorStringRuleToSparql();
		ISpecification<String> rule = parameters.getMainRule();
		sparqlQueries = translator.translate(rule);
		
		List<String> sampleLinks = FrameworkUtils.readGoldLinks(this.goldStandard);			
		for(int index=0; index < sampleLinks.size(); index++) {
			String slink = sampleLinks.get(index);
			String[] links = null;
			if(slink.contains("differentFrom")) {
				links = slink.replaceAll(">\\s*\\.", ">").replace("<", "").replace(">", "").split("http://www.w3.org/2002/07/owl#differentFrom");
				linker.linkInstances(rule, links[0].trim(), links[1].trim(), sparqlQueries);
			}else if(slink.contains("sameAs")) {
				links = slink.replaceAll(">\\s*\\.", ">").replace("<", "").replace(">", "").split("http://www.w3.org/2002/07/owl#sameAs");
				linker.linkInstances(rule, links[0].trim(), links[1].trim(), sparqlQueries);
			}else {
				System.out.println("Something wierd happened with "+slink+" when applying the rule "+sparqlQueries);
			}
			
		}
			
	}
	
	
	
	
	/**
	 * Finds the set of shortest paths that connect the intialClasses with the targetClasses within the input dataset
	 */
	private Set<Path> retrieveConnectingPaths(String dataset, Collection<String> intialClasses, Collection<String> targetClasses){
		PathFinder pathFinder = null;
		Set<Path> paths = null;
		
		pathFinder = new PathFinder(searchParameters);
		pathFinder.searchPaths(dataset, intialClasses, targetClasses, searchParameters.getMaxSearchDepth());
		paths = pathFinder.getShortesPaths();
		
		return paths;
	}
	
	/*
	 * Overlaps
	 */

	
	/**
	 * Applies the specification to the set of instances linked by the main rule
	 */
	private Set<Tuple<String,String>> linkNeighbors(ISpecification<String> rule, Path sourcePath, Path targetPath){
		
		Connection connection = null; 
		PreparedStatement statement = null;
		java.sql.ResultSet queryResult = null;

		// Access cached data
	
		try {
			// Create connection
			connection = H2Cache.getConnection();
			connection.setAutoCommit(false);
			// Prepare query to retrieve stored links
			statement = connection.prepareStatement("SELECT DISTINCT iri_source, iri_target FROM links");
			statement.setFetchSize(10000); //TODO: not sure about this parameter
			// Execute query
			queryResult =  statement.executeQuery();
			while (queryResult.next()){
				// Retrieve main iris previously linked
				String iriSource = queryResult.getString("iri_source");
				String iriTarget = queryResult.getString("iri_target");
				// Link the neighbors of such instances
				linkIrisNeighbors(rule, sourcePath, targetPath, iriSource, iriTarget);
			}
			// Close connection, statement, and query results
			queryResult.close();
			statement.close();
			connection.close();
		} catch(SQLException e) {
			e.printStackTrace();
		}
		return null;
	}
	

	public void linkIrisNeighbors(ISpecification<String> rule, Path sourcePath, Path targetPath, String sourceIri, String targetIri) {
		// Retrieve neighbors
		Integer sourceNeighborSize = retrieveNeighborsSizes(parameters.getSourceDataset(), rule, sourceIri, sourcePath, rule.getSourceRestrictions());
		Integer targetNeighborSize = retrieveNeighborsSizes(parameters.getTargetDataset(), rule, targetIri, targetPath, rule.getTargetRestrictions());
		
		// Check whether there are no neighbors
		if(sourceNeighborSize > 0 && targetNeighborSize > 0) {
			// Link neighbors
			Tuple<String,String> sparqlQueries = translateToQueryWithPath(rule, sourcePath, targetPath, sourceIri, targetIri);
			linker.linkNeighbors(parameters.getMainRule(), rule, sourceIri, targetIri, sourcePath, targetPath, sourceNeighborSize, targetNeighborSize, sparqlQueries);
			
		}else {
			//TODO: what are we going to do if there is no context? Give a warning and store in a warning_links
			// take into account that may happend that there is no context for a path, but there is for another
			//System.out.println("Not applicable: "+sourceIri+" "+targetIri);
		}
		
		
	}
	
	private Integer retrieveNeighborsSizes(String datasetName, ISpecification<String> rule, String iri, Path path, List<String> clazzes) {
		Integer neighborsCounter = null;
		String query = null;
		// Initialize database
		Dataset datasetSource = TDBFactory.createDataset(datasetName);
		datasetSource.begin(ReadWrite.READ);
		// Initialize variables
		query = connectIrisCounterQuery(iri, path, clazzes); // create query
		neighborsCounter = 0;		
		// Execute query
		try {				
			QueryExecution qexec = QueryExecutionFactory.create(query, datasetSource);
			ResultSet results = qexec.execSelect();
			// Retrieve neighbors
			while(results.hasNext()) {
				QuerySolution querySolution = results.next();
				if(querySolution!= null && querySolution.contains("?count")) {
					neighborsCounter = querySolution.get("?count").asLiteral().getInt();
				}
			}
			qexec.close();
		}catch (Exception e) {
			e.printStackTrace();
		}finally {
			datasetSource.close();
		}
		
		return neighborsCounter;
	}
	
	private String connectIrisCounterQuery(String iri, Path path, List<String> rdfClasses) {
		StringBuffer query = new StringBuffer();
		
		query.append("SELECT DISTINCT (count(distinct ").append(path.getLastVariable()).append(") as ?count) {\n");
		// Add path components to the query
		path.getPathComponents().stream().forEach(element -> query.append("\t").append(element.toSPARQL()).append(" \n"));
		// Add rdf:type classes of neighbors to the query
		rdfClasses.stream().forEach(clazz -> query.append("\t").append(path.getLastVariable()).append(" <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ").append(SPARQLFactory.fixIRIS(clazz)).append(" .\n"));
		query.append("}");
		return query.toString().replace(path.getInitialVariable(), SPARQLFactory.fixIRIS(iri));
	}
	
	
	private Tuple<String,String> translateToQueryWithPath(ISpecification<String> rule, Path sourcePath, Path targetPath, String sourceIri, String targetIri) {
		TranslatorStringRuleToSparql translator = null;
		Tuple<String, String> ruleQueries = null;
		String sourceQuery = null;
		String targetQuery = null;
		Tuple<String,String> queries = null;
		
		// Retrieve link rule queries
		queries = new Tuple<String,String>();
		translator = new TranslatorStringRuleToSparql();
		ruleQueries = translator.translate(rule);
				
		// Integrate the path into the query matching variables in both
		sourceQuery = integratePathInLinkRule(ruleQueries.getFirstElement(), sourcePath, sourceIri);
		targetQuery = integratePathInLinkRule(ruleQueries.getSecondElement(), targetPath, targetIri);
		queries = new Tuple<String,String>(sourceQuery,targetQuery);
		
		return queries;
	}
	
	private String integratePathInLinkRule(String query, Path path, String iri) {
		String[] mainVariable = null;
		String lastPathVariable = null;
		String initialPathVariable = null;
		String pathSparql = null;
		
		// Retrieve varaibles to replace in both path and query
		mainVariable = SPARQLFactory.getMainVariable(query);
		lastPathVariable = path.getLastVariable();
		initialPathVariable = path.getInitialVariable();
		// Replace variables in path, i.e., connect path with query variables
		pathSparql = path.toSPARQL();
		pathSparql = pathSparql.replace(lastPathVariable, mainVariable[0]);

		// Append path to query
		query = query.replace("{", "{\n"+pathSparql)
					 .replace(initialPathVariable, SPARQLFactory.fixIRIS(iri));

		return query;
	}
	
	
	// --------------------------------------
	
}
