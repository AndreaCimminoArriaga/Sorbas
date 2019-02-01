package tdg.sorbas.rules;

import tdg.link_discovery.framework.algorithm.individual.AbstractSpecification;


public class LinkRule extends AbstractSpecification<String>{

	
	public LinkRule(String linkRule){
		super(linkRule);
	}
	
	public LinkRule(){
		this.specificationRepresentation = "";
	}
	
}
