package tdg.sorbas.sample_generator;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.tdb.TDBFactory;

import com.google.common.collect.Sets;

import tdg.evaluator.parameters.LinksEvaluatorParameters;
import tdg.evaluator.parameters.LinksEvaluatorParametersReader;
import tdg.link_discovery.middleware.utils.FrameworkUtils;
import tdg.sorbas.cache.H2Cache;
import tdg.sorbas.parameters.InputReader;
import tdg.sorbas.parameters.SorbasParameters;

public class SampleGeneratorMain {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		
		String parametersFile= args[0];//"restaurants-input.json";
		System.out.println("File: "+parametersFile);
		SorbasParameters parameters = InputReader.parseFromJSON(parametersFile);
		LinksEvaluatorParameters parametersEvaluator =  LinksEvaluatorParametersReader.parseFromJSON(parametersFile);
		List<String> sourceRestrictions = parameters.getMainRule().getSourceRestrictions();
		List<String> targetRestrictions = parameters.getMainRule().getTargetRestrictions();
		System.out.println("Gold links file: "+parametersEvaluator.getGoldStandard());
		Set<String> goldLinks = FrameworkUtils.readGoldLinks(parametersEvaluator.getGoldStandard()).stream().map(link -> cleanSameAs(link)).collect(Collectors.toSet());			
		// Retrieve IRIs from restrictions
		Set<String> sourceIris = Sets.newHashSet(retrieveRestrictionIris(sourceRestrictions, parameters.getSourceDataset()));
		Set<String> targetIris = Sets.newHashSet(retrieveRestrictionIris(targetRestrictions, parameters.getTargetDataset()));
		// Generate all possible links
		FileWriter writer = null;
		try {
			writer = new FileWriter(parametersFile.replace(".json", "-plinks.nt")); 
			for(String gLink:goldLinks) {
				writer.write("<"+gLink.replace("#http://", "> <http://www.w3.org/2002/07/owl#sameAs> <http://")+"> .\n");
			}
			writer.close();
			writer = new FileWriter(parametersFile.replace(".json", "-nlinks.nt")); 
			for(String sourceIri:sourceIris) {
				for(String targetIri:targetIris){
					String link = sourceIri.trim()+"#"+targetIri.trim();
						if(!goldLinks.contains(link)) {
							writer.write("<"+link.replace("#http://", "> <http://www.w3.org/2002/07/owl#differentFrom> <http://")+"> .\n");
						}
						
				}
			}
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			try {
				if(writer!=null)
					writer.close();
			}catch(Exception e) {
				e.printStackTrace();
			}
		}
		
		/*Set<String> allLinks = Sets.cartesianProduct(sourceIris, targetIris).stream().map(tuple -> tuple.get(0)+"#"+tuple.get(1)).collect(Collectors.toSet());
		System.out.println("all: "+allLinks.size());
		System.out.println("positive: "+goldLinks.size());
		Set<String> allNegativeLinks = Sets.difference(allLinks, goldLinks);
		System.out.println("negative: "+allNegativeLinks.size());
		
		FileWriter writer = null;
		try {
			writer = new FileWriter(parametersFile.replace(".json", "-links.nt")); 
			for(String str: goldLinks) {
				writer.write("<"+str.replace("#http://", "> <http://www.w3.org/2002/07/owl#sameAs> <http://")+"> .\n");
			}
			for(String str: allNegativeLinks) {
			  writer.write("<"+str.replace("#http://", "> <http://www.w3.org/2002/07/owl#differentFrom> <http://")+"> .\n");
			}
					
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			try {
				if(writer!=null)
					writer.close();
			}catch(Exception e) {
				e.printStackTrace();
			}
		}*/
	}

	private static String cleanSameAs(String link) {
		return link.replaceAll("\\s*", "").replace(">.", "").replace("<", "").replace(">", "").replace("http://www.w3.org/2002/07/owl#sameAs", "#").toString();
	}
	
	private static List<String> retrieveRestrictionIris(List<String> restrictions, String datasetSourceName){
		StringBuilder queryString = new StringBuilder("SELECT ?iri {");
		restrictions.forEach(restriction -> queryString.append("?iri a <").append(restriction).append("> .\n"));
		queryString.append("}");
		Dataset datasetSource = TDBFactory.createDataset(datasetSourceName);
		datasetSource.begin(ReadWrite.READ);
		List<String> queryResults = new ArrayList<>();
		try {			
			// Execute query
			Query query = QueryFactory.create(queryString.toString());
			QueryExecution qexec = QueryExecutionFactory.create(query, datasetSource);
			ResultSet results = qexec.execSelect();
			
			while(results.hasNext()) {
				QuerySolution querySolution = results.next();
				queryResults.add(querySolution.get("?iri").toString());
			}
		}catch(Exception e) {
			e.printStackTrace();	
		}
		
		return queryResults;
	}
}
