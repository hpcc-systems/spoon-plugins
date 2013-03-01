package org.hpccsystems.saltui.concept;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class ConceptRuleList {
	private ArrayList<ConceptRuleBO> rules = new ArrayList<ConceptRuleBO>();
	private ArrayList<String> displayTitles = new ArrayList<String>();
	private Set<IConceptRuleListViewer> changeListeners = new HashSet<IConceptRuleListViewer>();
	
	public ConceptRuleList(){
		
	}
	public ArrayList<ConceptRuleBO> getFields() {
		return rules;
	}

	public String[] getTitles(){
		return displayTitles.toArray(new String[displayTitles.size()]);
	}

	public void setRules(ArrayList<ConceptRuleBO> rules) {
		this.rules = rules;
	}

	public int getIndex(String rule){
		return displayTitles.indexOf(rule);
	}
	
	
	
	public void remove(int index){
		System.out.println("Removing: " + index);
		if(index >= 0 && index <= rules.size()-1){
			System.out.println("field size: " + rules.size());
			rules.remove(index);
			rules.trimToSize();
		}
		if(index >= 0 && index <= displayTitles.size()-1){
			System.out.println("displayTitles size: " + displayTitles.size());
			displayTitles.remove(index);
			displayTitles.trimToSize();
		}
	}
	
	public void add(ConceptRuleBO ft){
		rules.add(ft);
		displayTitles.add(ft.getDisplayTitle());
		
		Iterator<IConceptRuleListViewer> iterator = changeListeners.iterator();
        while (iterator.hasNext()){
                iterator.next().addFieldType(ft);
        }
	}
	public void update(int index, ConceptRuleBO ft){
		rules.set(index, ft);
		displayTitles.set(index, ft.getDisplayTitle());
		//Iterator<IFieldTypeListViewer> iterator = changeListeners.iterator();
		//while (iterator.hasNext())
		//	iterator.next().modifyFieldType(ft);
	}
	
	public ConceptRuleBO get(int index){
		return rules.get(index);
	}
	
	public void createDefault(){
		if(this.rules.size() == 0){
			ConceptRuleBO caps = new ConceptRuleBO("To Uppercase");
			caps.setCaps(true);
			caps.setTypename("caps");
			this.add(caps);
			
			ConceptRuleBO leftTrim = new ConceptRuleBO("Left Trim");
			leftTrim.setTypename("DEFAULT");
			leftTrim.setLefttrim(true);
			this.add(leftTrim);
		}
	}
	
	
}
