package tdg.sorbas.parameters;

import java.util.Set;

import tdg.link_discovery.framework.algorithm.individual.ISpecification;

public class SorbasParameters {

	private ISpecification<String> mainRule;
	private Set<ISpecification<String>> supportingRules;
	private String sourceDataset; 
	private String targetDataset;
	private String resultsFolder;
	private String outputMainRuleLinksFile;
	private String filteredLinksFile;
	
	public ISpecification<String> getMainRule() {
		return mainRule;
	}
	public void setMainRule(ISpecification<String> mainRule) {
		this.mainRule = mainRule;
	}
	public Set<ISpecification<String>> getSupportingRules() {
		return supportingRules;
	}
	public void setSupportingRules(Set<ISpecification<String>> supportingRules) {
		this.supportingRules = supportingRules;
	}
	public String getSourceDataset() {
		return sourceDataset;
	}
	public void setSourceDataset(String sourceDataset) {
		this.sourceDataset = sourceDataset;
	}
	public String getTargetDataset() {
		return targetDataset;
	}
	public void setTargetDataset(String targetDataset) {
		this.targetDataset = targetDataset;
	}
	public String getResultsFolder() {
		return resultsFolder;
	}
	public void setResultsFolder(String resultsFolder) {
		this.resultsFolder = resultsFolder;
	}
	public String getOutputMainRuleLinksFile() {
		return outputMainRuleLinksFile;
	}
	public void setOutputMainRuleLinksFile(String outputMainRuleLinksFile) {
		this.outputMainRuleLinksFile = outputMainRuleLinksFile;
	}
	public String getFilteredLinksFile() {
		return filteredLinksFile;
	}
	public void setFilteredLinksFile(String filteredLinksFile) {
		this.filteredLinksFile = filteredLinksFile;
	} 
	
	
	
	
}
