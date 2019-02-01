package tdg.sorbas;

import java.io.File;
import java.io.FileWriter;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import tdg.evaluator.model.ConfusionMatrix;
import tdg.evaluator.parameters.LinksEvaluatorParameters;
import tdg.evaluator.parameters.LinksEvaluatorParametersReader;
import tdg.link_discovery.middleware.framework.configuration.FrameworkConfiguration;
import tdg.link_discovery.middleware.log.Logger;
import tdg.link_discovery.middleware.objects.Tuple;
import tdg.link_discovery.middleware.utils.FrameworkUtils;
import tdg.pathfinder.parameters.PathFinderParameters;
import tdg.pathfinder.parameters.PathFinderParametersReader;
import tdg.sorbas.cache.H2Cache;
import tdg.sorbas.filters.overlap.Overlapper;
import tdg.sorbas.filters.rule_vote.RuleVoter;
import tdg.sorbas.parameters.InputReader;
import tdg.sorbas.parameters.SorbasParameters;

public class SorbasRLMain {

	private static String parametersFile;

	
	public static void main(String[] args) throws Exception {
		// Disable jena logs and use ours
		org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.OFF);
		// -- Teide
		SorbasParameters parameters = null;
		SorbasRL sorbas = null; 
		LinksEvaluatorParameters parametersEvaluator = null;
		// -- PathFinder
		PathFinderParameters parametersSearch = null;
		// -- Thresholds
		Double overlap = null;
		Double d = null;
		
		File db = new File("h2cache.mv.db");
		if(db.exists())
			db.delete();
		
		// Retrieve configuration file
		parametersFile =  args[0].trim(); //"restaurants-input.json";//
		overlap =  Double.parseDouble(args[1].trim());
		d = Double.parseDouble(args[2].trim());// 0.01;//

		
		// Execute Teide and store results
		parameters = InputReader.parseFromJSON(parametersFile);
		parametersSearch = PathFinderParametersReader.parseFromJSON(parametersFile);
		parametersEvaluator =  LinksEvaluatorParametersReader.parseFromJSON(parametersFile);
		FrameworkConfiguration.traceLog = new Logger(new StringBuffer(parameters.getResultsFolder()).append("/execution-log.txt").toString(), 1);
		sorbas = new SorbasRL(parameters, parametersSearch,parametersEvaluator.getGoldStandard());
		FrameworkConfiguration.traceLog.addLogLine(SorbasRLMain.class.getCanonicalName(), "Executing Teide Algorithm");
		long startTime = System.nanoTime();
		
		// Execute core
		sorbas.execute();
		
		// Apply filters
		Overlapper overlaper = new Overlapper(overlap);
		String overlapperResults = overlaper.applyFilter();
		RuleVoter ruleVoter = new RuleVoter(d, overlapperResults);
		String ruleVoteResults = ruleVoter.applyFilter();
		
		// Evaluate
		String results = evaluateResults(parametersEvaluator, ruleVoteResults,  parametersFile, parameters);
		FileWriter writer = new FileWriter(LinksEvaluatorParametersReader.parseFromJSON(parametersFile).getResultsFile()); 
		writer.write("\"technique\",\"tp\",\"tn\",\"fp\",\"fn\",\"P\",\"R\",\"F\"\n");
		writer.write(results);
		writer.close();
		
		// Store filtered rules
		try {
			Map<Tuple<String,String>, String> supportingRules  = ruleVoter.filteredRules(d);
			String resultsContentFile = formatResultsJSONFile(parameters, parametersSearch, parametersEvaluator, supportingRules);
			FileWriter supportingRulesWriter = new FileWriter(parametersFile.replaceAll(".json", "-filtered.json")); 
			supportingRulesWriter.write(resultsContentFile);
			supportingRulesWriter.close();
		}catch(Exception e) {
				e.printStackTrace();
			}
		
		long stopTime = System.nanoTime();
	    long elapsedTime = (stopTime - startTime)/1000000;
	    FrameworkConfiguration.traceLog.addLogLine(SorbasRLMain.class.getCanonicalName(), "Executed in "+elapsedTime+" (ms)");
	
	}
	
	
	private static String evaluateResults(LinksEvaluatorParameters parametersEvaluator, String tableNameD, String parametersFile, SorbasParameters parameters) {
		String result = null;
		FileWriter writer = null;
		List<String> goldLinks = Lists.newArrayList();
		try {
			
			// Retrieve links
			goldLinks = FrameworkUtils.readGoldLinks(parametersEvaluator.getGoldStandard());			
			Set<String> links = Sets.newHashSet(H2Cache.retrieveLinks(tableNameD));
						
			writer = new FileWriter(parameters.getFilteredLinksFile()); 
			for(String str: links) {
			  writer.write(str);
			}
			// Evaluate links
			
			Set<String> mainRuleLinks =Sets.newHashSet(H2Cache.retrieveLinks("links"));
			System.out.println("Main Rule:\n\t-Links:"+mainRuleLinks.size());
			System.out.println("Sup. Rules:\n\t-Links:"+links.size());
			ConfusionMatrix matrix1 = computeConfustionMatrix(goldLinks, mainRuleLinks);
			ConfusionMatrix matrix2 = computeConfustionMatrix(goldLinks, links);
			
			result = "\"main rule\"," +matrix1.toCSVLine() +"\n\"sorbasRL\","+ matrix2.toCSVLine();
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			try {
				if(writer!=null)
					writer.close();
			}catch(Exception e) {
				e.printStackTrace();
			}
		}
		
		try {
			writer = new FileWriter(parameters.getOutputMainRuleLinksFile()); 
			for(String str: Sets.newHashSet(H2Cache.retrieveLinks("links"))) {
			  writer.write(str);
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
		
		
		return result;
	}

	
	public static ConfusionMatrix computeConfustionMatrix(List<String> samplesFile, Set<String> links) {
		ConfusionMatrix matrix = new ConfusionMatrix();
		Set<String> positiveSamples = samplesFile.stream().filter(link -> link.contains("sameAs")).map(link -> cleanSameAs(link)).collect(Collectors.toSet());
		Set<String> negativeSamples = samplesFile.stream().filter(link -> link.contains("differentFrom")).map(link -> cleanDifferentFrom(link)).collect(Collectors.toSet());

		Set<String> forecastedLinks = links.stream().map(link -> cleanSameAs(link)).collect(Collectors.toSet());
		
		int tp = Sets.intersection(positiveSamples, forecastedLinks).size();

		int fp = forecastedLinks.size() - tp;
		
	
		int tn = negativeSamples.size() - fp;
		int fn = positiveSamples.size() - tp;
		matrix.setTruePositives(tp);
		matrix.setFalsePositives(fp);
		matrix.setTrueNegatives(tn);
		matrix.setFalseNegatives(fn);
		return matrix;
	}
	
	private static String cleanSameAs(String link) {
		return link.replaceAll("\\s*", "").replace(">.", "").replace("<", "").replace(">", "").replace("http://www.w3.org/2002/07/owl#sameAs", "#").toString();
	}
	
	private static String cleanDifferentFrom(String link) {
		return link.replaceAll("\\s*", "").replace(">.", "").replace("<", "").replace(">", "").replace("http://www.w3.org/2002/07/owl#sameAs", "#").toString();
	}
	
	private static String formatResultsJSONFile(SorbasParameters parameters, PathFinderParameters parametersSearch, LinksEvaluatorParameters parametersEvaluator, Map<Tuple<String,String>, String> supportingRules) {
		StringBuilder builder = new StringBuilder("{\n");
		builder.append("\t\"links-evaluator\" : {\n");  
		builder.append("\t		\"resultsFile\" : \"").append(parametersEvaluator.getResultsFile()).append("\",\n");  
		builder.append("\t		\"goldStandardFile\" : \"").append(parametersEvaluator.getGoldStandard()).append("\",\n");   
		builder.append("\t		\"linksFile\" : \"").append(parametersEvaluator.getLinksFile()).append("\"\n");  
		builder.append("\t},\n");  
		builder.append("\t\"resultsFolder\":\"").append(parameters.getResultsFolder()).append("\",\n");
		builder.append("\t\"outputMainRuleLinks\":\"").append(parameters.getOutputMainRuleLinksFile()!=null && !parameters.getOutputMainRuleLinksFile().isEmpty()).append("\",\n");
		builder.append("\t\"outputMainRuleLinksFile\":\"").append(parameters.getOutputMainRuleLinksFile()).append("\",\n");
		builder.append("\t\"filteredLinksFile\":\"").append(parameters.getFilteredLinksFile()).append("\",\n");
		builder.append("\t\"sourceDataset\":\"").append(parameters.getSourceDataset()).append("\",\n");
		builder.append("\t\"targetDataset\":\"").append(parameters.getTargetDataset()).append("\",\n");
		builder.append("\t\"mainLinkRule\":{\n");
		builder.append("\t		\"sourceClasses\":").append(parameters.getMainRule().getSourceRestrictions().stream().map(rest -> "\""+rest+"\"").collect(Collectors.toList())).append(",\n");
		builder.append("\t		\"targetClasses\":").append(parameters.getMainRule().getTargetRestrictions().stream().map(rest -> "\""+rest+"\"").collect(Collectors.toList())).append(",\n");
		builder.append("\t		\"restriction\" : \"").append(parameters.getMainRule().getSpecificationRepresentation()).append("\"\n");
		builder.append("\t},\n");
		builder.append("\t\"supportingRules\" : [ ");
		for(Entry<Tuple<String,String>,String> entry:supportingRules.entrySet()) {
			Tuple<String,String> paths = entry.getKey();
			builder.append("{\n");
			builder.append("\t\t		\"sourcePath\":").append(paths.getFirstElement().replace("{", "[").replace("}", "]").replace("(", "\"").replace(")", "\"").trim()).append(",\n");
			builder.append("\t\t		\"targetPath\":").append(paths.getSecondElement().replace("{", "[").replace("}", "]").replace("(", "\"").replace(")", "\"").trim()).append(",\n");
			builder.append("\t\t		\"restriction\" : \"").append(entry.getValue()).append("\"\n");
			builder.append("\t\t},");
		}
		
		return builder.substring(0, builder.lastIndexOf(","))+"\n\t]\n}";
	}
}
