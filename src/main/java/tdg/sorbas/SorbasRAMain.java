package tdg.sorbas;

import java.io.FileWriter;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import tdg.evaluator.model.ConfusionMatrix;
import tdg.evaluator.parameters.LinksEvaluatorParameters;
import tdg.evaluator.parameters.LinksEvaluatorParametersReader;
import tdg.link_discovery.middleware.framework.configuration.FrameworkConfiguration;
import tdg.link_discovery.middleware.log.Logger;
import tdg.link_discovery.middleware.utils.FrameworkUtils;
import tdg.sorbas.cache.H2Cache;
import tdg.sorbas.filters.overlap.Overlapper;
import tdg.sorbas.parameters.InputReaderRA;
import tdg.sorbas.parameters.SorbasParameters;

public class SorbasRAMain {

	public static void main(String[] args) throws Exception {
		String parametersFile = args[0];
		Double overlap = Double.valueOf(args[1]);
		// -- Teide
		SorbasParameters parameters  = null;
		SorbasRA sorbasRA = null; 
		LinksEvaluatorParameters parametersEvaluator = null;
	
		// Run algorithm
		parameters = InputReaderRA.parseFromJSON(parametersFile);
		parametersEvaluator =  LinksEvaluatorParametersReader.parseFromJSON(parametersFile);
		FrameworkConfiguration.traceLog = new Logger(new StringBuffer(parameters.getResultsFolder()).append("/execution-log.txt").toString(), 1);
		sorbasRA = new SorbasRA(parameters, parametersEvaluator.getGoldStandard());
		FrameworkConfiguration.traceLog.addLogLine(SorbasRLMain.class.getCanonicalName(), "Executing Teide Algorithm");
		long startTime = System.nanoTime();
		
		// Execute core
		sorbasRA.execute();
		
		// Apply filters
		Overlapper overlaper = new Overlapper(overlap);
		String overlapperResults = overlaper.applyFilter();
		

		// Evaluate
		String results = evaluateResults(parametersEvaluator, overlapperResults,  parametersFile, parameters);
		FileWriter writer = new FileWriter(LinksEvaluatorParametersReader.parseFromJSON(parametersFile).getResultsFile()); 
		writer.write("\"technique\",\"tp\",\"tn\",\"fp\",\"fn\",\"P\",\"R\",\"F\"\n");
		writer.write(results);
		writer.close();
		
		
		
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
			
			result = "\"main rule\"," +matrix1.toCSVLine() +"\n\"sorbas\","+ matrix2.toCSVLine();
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
	
	
	
}
