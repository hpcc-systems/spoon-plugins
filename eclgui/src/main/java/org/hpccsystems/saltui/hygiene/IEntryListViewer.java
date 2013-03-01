package org.hpccsystems.saltui.hygiene;

public interface IEntryListViewer {
	
	//Update the view after the record has been added to record list
	public void addEntry(org.hpccsystems.saltui.concept.EntryBO entry);
	
	//Update the view after the record has been removed from record list
	public void removeEntry(int index);
	
	//Update the view after the record has been modified in the record list
	public void modifyEntry(EntryBO entry);
}
