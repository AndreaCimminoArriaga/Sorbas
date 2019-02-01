package tdg.sorbas.parameters;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import tdg.link_discovery.framework.algorithm.individual.ISpecification;
import tdg.sorbas.rules.LinkRule;

public class InputReader {

	
	
	/** 
	 *  Reads the parameters required by Teide algorithm from a json file
	 *  
	 *  @param file A json file containing Teide parameters,
	 *  @throws Exception 
	 *  @returns A TeideParameters object encoding required data to execute the algorithm
	 */
	public static SorbasParameters parseFromJSON(String file ) throws Exception {
		// Json reading
		JSONParser parser = null;
		JSONObject jsonParameters = null;
		// Parameters
		SorbasParameters parameters = null;
		
		// Read file
        	parser = new JSONParser();
        jsonParameters = (JSONObject) parser.parse(new FileReader(file));
        parameters = new SorbasParameters();
        // Check correctness
        if(!isParametersFileCorrect(jsonParameters))    
        		throw new Exception("Provided input file lacks of a mandatory parameter");
        parameters = parseFromJson(jsonParameters);
        
        return parameters;
	}
	
	/** 
	 * Transforms a JSONObject coding the algorithms parameters into a TeideParameters object
	 * @throws Exception 
	 */
	private static SorbasParameters parseFromJson(JSONObject jsonParameters) throws Exception {
		String sourceDataset = null;
		String targetDataset = null;
		String resultsFolder = null;
		String outputMainRuleLinksFile = null;
		String filteredLinksFile = null;
		JSONObject jsonMainLinkRule = null;
		JSONArray jsonSupportingRulesArray = null;
		ISpecification<String> mainLinkRule = null;
		Set<ISpecification<String>> supportingLinkRules = null;
		SorbasParameters parameters = null;
		
		 parameters = new SorbasParameters();
		// Retrieve attributes
        sourceDataset = jsonParameters.get("sourceDataset").toString();
        targetDataset = jsonParameters.get("targetDataset").toString(); 
        resultsFolder = jsonParameters.get("resultsFolder").toString();
        outputMainRuleLinksFile = jsonParameters.get("outputMainRuleLinksFile").toString();
        filteredLinksFile = jsonParameters.get("filteredLinksFile").toString();
        jsonMainLinkRule = (JSONObject) jsonParameters.get("mainLinkRule");
        mainLinkRule =  createRuleFromJson(jsonMainLinkRule);
   
        jsonSupportingRulesArray = (JSONArray) jsonParameters.get("supportingRules");
        supportingLinkRules = createRulesFromJson(jsonSupportingRulesArray);
        
        // Create TeideParameters
        parameters.setSourceDataset(sourceDataset);
        parameters.setTargetDataset(targetDataset);
        parameters.setMainRule(mainLinkRule);
        parameters.setSupportingRules(supportingLinkRules);
        parameters.setResultsFolder(resultsFolder);
        parameters.setOutputMainRuleLinksFile(outputMainRuleLinksFile);
        parameters.setFilteredLinksFile(filteredLinksFile);
        
        return parameters;
	}
	

	/** 
	 *  Check whether a JSON containing a potential TeideParameters has all required attribute keys
	 */
	private static Boolean isParametersFileCorrect(JSONObject jsonParameters) {
		Boolean isCorrect = null;
		
		// Check all the required parameters are defined
		isCorrect = jsonParameters.containsKey("sourceDataset") 
					&& jsonParameters.containsKey("targetDataset") 
					&& jsonParameters.containsKey("resultsFolder") 
					&& jsonParameters.containsKey("outputMainRuleLinksFile") 
					&& jsonParameters.containsKey("filteredLinksFile") 
					&& jsonParameters.containsKey("mainLinkRule") 
					&& jsonParameters.containsKey("supportingRules"); 
		
		
		
		return isCorrect;
	}
	
	
	/** 
	 *  Translates a JSONArray of JSONObject rules into a ISpecification<String> objects
	 *  
	 *  @param jsonRule A JSONObject representing the rule,
	 *  @throws Exception 
	 *  @returns the input rule as an ISpecification<String> object
	 */
	@SuppressWarnings("unchecked")
	public static Set<ISpecification<String>> createRulesFromJson(JSONArray arrayOfRules) throws Exception {
		List<JSONObject> supportingJsonRules = Lists.newArrayList();
		Set<ISpecification<String>> supportingRules = null;
		
		supportingRules = Sets.newHashSet();
		arrayOfRules.forEach(jsonRule -> supportingJsonRules.add((JSONObject) jsonRule));
		for(JSONObject jsonRule:supportingJsonRules) {
			ISpecification<String> supportingRule = createRuleFromJson(jsonRule);
			supportingRules.add(supportingRule);
		}
		
		return supportingRules;
	}
	
	/** 
	 *  Translates a JSONObject rule into a ISpecification<String>
	 *  
	 *  @param jsonRule A JSONObject representing the rule,
	 *  @throws Exception 
	 *  @returns the input rule as an ISpecification<String> object
	 */
	public static ISpecification<String> createRuleFromJson(JSONObject jsonRule) throws Exception {
		ISpecification<String> rule = null;
		List<String> sourceRestrictions = null;
		List<String> targetRestrictions = null;
		String restriction = null;
		
		if(!isJsonRuleCorrect(jsonRule))
			throw new Exception("Provided json rule lacks of mandatory key attribute");
		
		// Parse rule's attributes
		sourceRestrictions = getRestrictionFromJsonRule( (JSONArray) jsonRule.get("sourceClasses"));
		targetRestrictions = getRestrictionFromJsonRule( (JSONArray) jsonRule.get("targetClasses"));
		restriction = (String) jsonRule.get("restriction");
		if(restriction.isEmpty())
			throw new Exception("link rule restriction cannot be empty");
		
		// Create the rule
		rule = new LinkRule();
		rule.setSourceRestrictions(sourceRestrictions);
		rule.setTargetRestrictions(targetRestrictions);
		rule.setSpecificationRepresentation(restriction);
	
		return rule;
	}

	private static boolean isJsonRuleCorrect(JSONObject jsonRule) {
		Boolean isCorrect =  null;
		
		isCorrect = jsonRule.containsKey("sourceClasses")
					&& jsonRule.containsKey("targetClasses")
					&& jsonRule.containsKey("restriction");
				
		return isCorrect;
	}
	
	@SuppressWarnings("unchecked")
	private static List<String> getRestrictionFromJsonRule(JSONArray array){
		List<String> restrictions = new ArrayList<String>();
		
		array.forEach(restriction -> restrictions.add((String) restriction));
		
		return restrictions;
	}
	
}
