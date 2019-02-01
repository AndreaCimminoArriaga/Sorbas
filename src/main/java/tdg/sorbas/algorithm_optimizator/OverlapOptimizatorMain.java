package tdg.sorbas.algorithm_optimizator;

import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import tdg.evaluator.model.ConfusionMatrix;
import tdg.evaluator.parameters.LinksEvaluatorParameters;
import tdg.evaluator.parameters.LinksEvaluatorParametersReader;
import tdg.link_discovery.middleware.utils.FrameworkUtils;
import tdg.sorbas.SorbasRLMain;
import tdg.sorbas.cache.H2Cache;
import tdg.sorbas.filters.overlap.Overlapper;
import tdg.sorbas.filters.rule_vote.RuleVoter;

public class OverlapOptimizatorMain {

	private static List<String> goldLinks = Lists.newArrayList();


	public static void main(String[] args) {
		cleanDatabse();
		String parametersFile = args[0];
		
		
		double i = 0.0;
		while( i<=1) {
			Overlapper overlaper = new Overlapper(i);
			String overlapperResults = overlaper.applyFilter();
			String result = evaluateResults(overlapperResults, parametersFile);
			System.out.println("["+overlapperResults+"] Overlap threshold: "+i+"   has "+result);
			i += 0.01;
		
		}
		
		cleanDatabse();
	}
	
	
	private static String evaluateResults(String tableName, String parametersFile) {
		LinksEvaluatorParameters parametersEvaluator = null;
		String result = null;
		try {
			parametersEvaluator =  LinksEvaluatorParametersReader.parseFromJSON(parametersFile);
			// Retrieve links
			if(goldLinks.isEmpty())
				goldLinks = FrameworkUtils.readGoldLinks(parametersEvaluator.getGoldStandard());
			Set<String> links = Sets.newHashSet(H2Cache.retrieveLinks(tableName));
			// Evaluate links
			ConfusionMatrix matrix = SorbasRLMain.computeConfustionMatrix(goldLinks, links);
			
			result = matrix.toString();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
		return result;
	}
	
	
	private static void cleanDatabse() {
		Overlapper overlaper = new Overlapper(0.1);
		RuleVoter ruleVoter = new RuleVoter(1.0, "overlap_filtered");
		
		ruleVoter.deleteViews();
		overlaper.deleteViews();
	}

}
