package org.hpccsystems.saltui.concept;

public interface IEntryListViewer {
	
	//Update the view after the record has been added to record list
	public void addEntry(EntryBO entry);
	
	//Update the view after the record has been removed from record list
	public void removeEntry(int index);
	
	//Update the view after the record has been modified in the record list
	public void modifyEntry(EntryBO entry);
}
