package tdg.sorbas.rules.linker;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jena.ext.com.google.common.collect.Lists;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.tdb.TDBFactory;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import tdf.pathfinder.model.Path;
import tdg.link_discovery.connector.sparql.evaluator.arq.linker.factory.SPARQLFactory;
import tdg.link_discovery.framework.algorithm.individual.ISpecification;
import tdg.link_discovery.middleware.objects.Tuple;
import tdg.link_discovery.middleware.utils.StreamUtils;
import tdg.sorbas.cache.H2Cache;


public class LinkerK{
	
	protected String datasetSourceName, datasetTargetName;	
	private H2Cache cache;
	public static int DEFAULT_THREAD_POOL_SIZE = 100;
	
	private ISpecification<?> linkingRule;
	// Variables to store linked neighbors in cache
	protected Boolean linkNeighbors = false;
	private String sourcePath, targetPath, mainIriSource, mainIriTarget; 
	private Integer sourceNeighborSize, targetNeighborSize;
	private String supportingRule;
	
	public LinkerK(){
		super();
		datasetSourceName="";
		datasetTargetName ="";
		cache = new H2Cache();
		cache.clearCache(); // TODO: make this work as static
	}
	

	/*
	 * Getters and setters
	 */

	public void setDatasetSource(String datasetSource) {
		this.datasetSourceName = datasetSource;
	}


	public void setDatasetTarget(String datasetTarget) {
		this.datasetTargetName = datasetTarget;
	}

	public Set<Tuple<String,String>> getInstancesLinked(){
		return cache.getLinks();
	}



	public void setLinkNeighbors(Boolean linkNeighbors) {
		this.linkNeighbors = linkNeighbors;
	}
	
	
	/*
	 * Link two datasets
	 */
	
	public void linkNeighbors(ISpecification<String> mainRule, ISpecification<String> supportingRule, String sourceIri, String targetIri, Path sourcePath, Path targetPath, Integer sourceNeighborSize, Integer targetNeighborSize, Tuple<String, String> sparqlQueries) {
		this.linkingRule = mainRule;
		this.linkNeighbors = true;
		this.sourcePath = sourcePath.toString();
		this.targetPath = targetPath.toString();
		this.mainIriSource =sourceIri;
		this.mainIriTarget = targetIri;
		this.sourceNeighborSize = sourceNeighborSize;
		this.targetNeighborSize= targetNeighborSize;
		this.supportingRule = supportingRule.toString();
		if(nonEmptyQueries(sparqlQueries)){
			linkInstances(supportingRule, null, null, sparqlQueries);
		}
		
		
		this.linkNeighbors = false;
	}
	
	
	
	public void linkDatasets(ISpecification<?> rule, Tuple<String, String> queries) {
		linkingRule = rule;
		if(nonEmptyQueries(queries)){
			linkInstances(rule, null, null, queries);
		}
	}	

	public void linkInstances(ISpecification<?> rule, String sourceInstance, String targetInstance, Tuple<String,String> queries) {
		linkingRule = rule;
		Dataset datasetSource = TDBFactory.createDataset(datasetSourceName);
		datasetSource.begin(ReadWrite.READ);
		
		// Retrieving variables
		Tuple<String, List<String>> retrievedVariables = retrieveVariablesFromSourceQuery(queries.getFirstElement());
		String mainVariable = retrievedVariables.getFirstElement();
		List<String> variables = retrievedVariables.getSecondElement(); // Does not contain the main var
		// Prepare query to be executed
		String queryString = formatSourceQuery(queries.getFirstElement(), mainVariable, sourceInstance);
		ExecutorService executor = Executors.newFixedThreadPool(DEFAULT_THREAD_POOL_SIZE);
		try {			
			// Execute query
			queryString = queryString.replaceAll(">\\d+", ">");
			Query query = QueryFactory.create(queryString);
			QueryExecution qexec = QueryExecutionFactory.create(query, datasetSource);
			ResultSet results = qexec.execSelect();
		
			while(results.hasNext()) {
				QuerySolution querySolution = results.next();
				handleSourceResultSet(querySolution, sourceInstance, targetInstance, queries, mainVariable, variables);
				
			}
			
			// Retrieve and process results in parallel
			qexec.close();
			
		} catch (Exception e) {
			System.out.println("Error with: "+queryString);
			e.printStackTrace();
		}finally {
			datasetSource.end();
			// Shutdown executor
			executor.shutdown();
		}
		
	}
	
	private Tuple<String, List<String>> retrieveVariablesFromSourceQuery(String query){
		String [] vars = SPARQLFactory.getMainVariable(query);
		List<String> variables = Lists.newArrayList();
		String mainVariable = vars[0];
		variables.addAll(Arrays.asList(vars));
		variables.remove(mainVariable);
		return new Tuple<String,List<String>>(mainVariable, variables);
	}

	private String formatSourceQuery(String query, String mainVariable, String sourceInstance){
		String queryString = query;
		// if sourceInstance !=null means we are linking examples, hece, embed the example iri in the query
		if(sourceInstance != null){
			queryString = queryString.replace("DISTINCT "+mainVariable, "DISTINCT ");
			queryString = queryString.replace(mainVariable, SPARQLFactory.fixIRIS(sourceInstance));
		}// Other wise don't embed anything.
		queryString = queryString.replace("DISTINCT", ""); // Not removing the DISTINCT throws error in some datasets due to the data structure and incompleteness
		return queryString;
	}
	
	

	private Set<Tuple<String,String>> handleSourceResultSet(QuerySolution soln, String sourceInstance, String targetInstance, Tuple<String, String> queries, String mainVariable, List<String> variables) {
		Set<Tuple<String,String>> links = Sets.newHashSet();
		if (soln != null) {
			// In case sourceInstance is null, hence we are linking the
			// datasets. Retrieve the current instance iri
			if (sourceInstance == null && soln.contains(mainVariable))
				sourceInstance = soln.getResource(mainVariable).toString();
			// Retrieving literal values
			Multimap<String, String> literals = retrieveSourceLiteralsFromQuerySolution(soln, mainVariable, variables);
			
			// Query second dataset
			if (sourceInstance != null && !literals.isEmpty()) {
				links = queryDatasetTarget(sourceInstance, targetInstance, literals,queries);
			}
			sourceInstance = null; // this line is mandatory to correctly
									// retrieve the next source iri
		}
	
		return links;
	}
	
	private Multimap<String, String> retrieveSourceLiteralsFromQuerySolution(QuerySolution soln, String mainVariable, List<String> variables){
		Multimap<String, String> literals = ArrayListMultimap.create();

		variables.stream().forEach(variable ->{
			if (soln.contains(variable) && soln.get(variable)!=null) {
				String literal = soln.get(variable).toString();
				if (!literals.containsEntry(variable, literal) && !literal.isEmpty())
					literals.put(variable, literal);
			}
		});
		
		return literals;
	}
	

	private Set<Tuple<String,String>> queryDatasetTarget(String sourceInstance, String targetInstance, Multimap<String, String> literals, Tuple<String, String> queries) {
		Dataset datasetTarget = TDBFactory.createDataset(datasetTargetName);
		datasetTarget.begin(ReadWrite.READ);
		Set<Tuple<String,String>> links = Sets.newHashSet();
		String queryString = replaceVariablesWithLiterals(queries.getSecondElement(), literals);
		// Obtaining relevant variables from query: main var and score var
		String scoreVariable = getTargetScoreVariable(queryString);
		String mainVar = SPARQLFactory.getMainVariable(queryString)[0];
		// Formatting query
		
		queryString = formatTargetQuery(queryString, mainVar,scoreVariable, targetInstance);
		queryString = queryString.replaceAll(">\\d+", ">");
		try {
			Query query = QueryFactory.create(queryString);
			QueryExecution qexec = QueryExecutionFactory.create(query, datasetTarget);
			ResultSet results = qexec.execSelect();
			while(results.hasNext()) {
				QuerySolution solution = results.next();
				links.add(handleTargetQuerySolution(solution, sourceInstance, targetInstance, mainVar, scoreVariable));
			}
			//StreamUtils.asStream(results).forEach(solution -> links.add(handleTargetQuerySolution(solution, sourceInstance, targetInstance, mainVar, scoreVariable)));
			qexec.close();
		} catch (Exception e) {
			//System.out.println("Failed executing target query in " + this.datasetTargetName + " the query:\n" + queryString+" \n\n ORIGINAL QUERY"+queries.getSecondElement());
			System.out.println("---------Error in linkerK [236]--------------");
			System.out.println("Original query:\n"+queryString+"\n");
			System.out.println("\n######\n");
			System.out.println("main var"+mainVar);
			System.out.println("score var: "+scoreVariable);
			System.out.println("target istance"+targetInstance);
			queryString = formatTargetQuery(queryString, mainVar,scoreVariable, targetInstance);
			System.out.println("\n######\n");
			System.out.println(queryString);
			System.out.println("-----------------------");
			//e.printStackTrace();
		}
		datasetTarget.end();
		
		return links;
	}
	
	private String getTargetScoreVariable(String targetQuery){
		String scoreVariable = targetQuery.substring(targetQuery.indexOf("FILTER")+6, targetQuery.lastIndexOf("0"));
		scoreVariable = scoreVariable.substring(scoreVariable.indexOf("?"), scoreVariable.lastIndexOf(">")).trim();
		return scoreVariable;
	}
	
	private String formatTargetQuery(String targetQuery, String mainVar, String scoreVariable, String targetInstance){
		String queryString = targetQuery;
	
		if(targetInstance!=null){
			// Adding score variable to SELECT statement & replacing mainVar for the instance iri
			// The regex that follows the main var is in case we havae as main var ?hc and then a var ?hcX. Withouth the regex an error would occur
			// because the non-main var would be partialy replaced with the iri of the main var rather than a literañ
			queryString = queryString.replaceFirst("\\"+mainVar+"[^\\,0-9a-zA-Z]", scoreVariable);  
			queryString = queryString.replaceAll("\\"+mainVar+"[^\\,0-9a-zA-Z]", SPARQLFactory.fixIRIS(targetInstance)+" ");
			
		}else{
			// We are linking the datasets, hence we don't replace the main var for the score 
			StringBuilder str = new StringBuilder();
			str.append("\\").append(mainVar);
			queryString = queryString.replaceFirst(str.toString()+"[^\\,]", str.append(" ").append(scoreVariable).toString());
		}
		
		queryString = queryString.replace("DISTINCT", ""); // Not removing the DISTINCT throws error in some datasets due to the data structure and incompleteness
		
		return queryString;
	}
	
	private Tuple<String, String> handleTargetQuerySolution(QuerySolution soln, String sourceInstance, String targetInstance, String mainVariable, String scoreVariable) {
		Tuple<String, String> instancesToLink =  null;
		if (soln.contains(scoreVariable)) {
			
			Double score = soln.get(scoreVariable).asLiteral().getDouble();
			if (targetInstance == null && soln.contains(mainVariable))
				targetInstance = soln.getResource(mainVariable).toString();
			if (score != null && targetInstance != null) {
				instancesToLink = new Tuple<String, String>( sourceInstance, targetInstance);
				try{
				
					if(linkNeighbors) {
						cache.addNeighbourLink(linkingRule.toString(), supportingRule, mainIriSource, mainIriTarget, sourceInstance, targetInstance, sourcePath, targetPath, sourceNeighborSize, targetNeighborSize);
					}else {
						cache.addLink(linkingRule.toString(), sourceInstance, targetInstance);
					}
					
					
				}catch(Exception e){
					e.printStackTrace();
				}
				targetInstance = null; // Set this to null in order to update the targetInstance variable in the next iteration
			}
		}
		return instancesToLink;
	}
	
	public void clearCache() {
		this.cache.clearCache();
	}
	
	/*
	 * Ancilliary methods
	 */

	private Boolean nonEmptyQuery(String query){
		Pattern pattern = Pattern.compile("\\{\\s*\\}");
        Matcher matcher = pattern.matcher(query);
		return  !matcher.find();
	}
	
	private Boolean nonEmptyQueries(Tuple<String,String> queries){
		return nonEmptyQuery(queries.getFirstElement()) && nonEmptyQuery(queries.getSecondElement());
	}
	
	private String replaceVariablesWithLiterals(String queryString, Multimap<String, String> literals) {
		String query = queryString;
		for(String var:literals.keySet()){
			for(String literal:literals.get(var)){
				String literalFixed = SPARQLFactory.fixLiterals(literal);
				StringBuffer literalToReplace = new StringBuffer();
				literalToReplace.append("\"").append(literalFixed).append("\"");
				query = query.replace(var,literalToReplace.toString());
			
			}
		}
		return query;
	}
	
	




	
	
	
	
	
}
