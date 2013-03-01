package org.hpccsystems.saltui.concept;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class EntryList {
	private ArrayList<EntryBO> entries = new ArrayList<EntryBO>();
	private Set<IEntryListViewer> changeListeners = new HashSet<IEntryListViewer>();

	public ArrayList<EntryBO> getEntries() {
		return entries;
	}

	public void setEntries(ArrayList<EntryBO> entries) {
		this.entries = entries;
	}
	
	public void add(EntryBO eb){
		entries.add(eb);
	}
	
	public EntryBO getEntry(int index){
		return entries.get(index);
	}
	
	public void updateEntry(int index, EntryBO r){
		System.out.println("Update Entry: " + index + " - " + r.getField());
		entries.set(index, r);
	}
	
	//this just updates the name stored to the entry for quick reference
	//this doesn't update indexes except if newName is "" then it assumes it was
	//deleted
	public void updateAll(String newName, String oldName){
		for(int i = 0; i<entries.size();i++){
			if(entries.get(i).getRuleName().equalsIgnoreCase(oldName)){
				entries.get(i).setRuleName(newName);
				if(newName.equals("")){
					entries.get(i).setHygieneRuleListIndex(-1);
				}
				System.out.println("Updating NewName: " + newName + " OldName: " + oldName);
			}//else{
			//	System.out.println("No Update: " + oldName);
			//}
			
		}
	}
	
	//Add a new Record to the existing list
	public void addEntry(int index) {
		EntryBO entry = new EntryBO();
		if(index >= 0){
			entries.add(index+1, entry);
		} else {
			entries.add(entries.size(), entry);
		}
		Iterator<IEntryListViewer> iterator = changeListeners.iterator();
		while (iterator.hasNext()){
			iterator.next().addEntry(entry);
		}
	}
	
	public int containsEntry(String name){
		int index = -1;
		//System.out.println("Looking for: " + name);
		for(int i = 0; i<entries.size();i++){
			//System.out.println(entries.get(i).getRuleName() + " vs " + name);
			if(entries.get(i).getRuleName().equalsIgnoreCase(name)){
				index = i;
			}//else{
			//	System.out.println("No Update: " + oldName);
			//}
			
		}
		
		return index;
	}

	public void removeEntry(int index) {
		if(entries.size()>index){
			entries.remove(index);
			Iterator<IEntryListViewer> iterator = changeListeners.iterator();
			while (iterator.hasNext())
				iterator.next().removeEntry(index);
		}
	}

	public void modifyEntry(EntryBO record) {
		Iterator<IEntryListViewer> iterator = changeListeners.iterator();
		while (iterator.hasNext())
			iterator.next().modifyEntry(record);
	}
	
	public void removeChangeListener(IEntryListViewer viewer) {
		changeListeners.remove(viewer);
	}

	public void addChangeListener(IEntryListViewer viewer) {
		changeListeners.add(viewer);
	}
        
     public void addEntryBO(EntryBO r){
    	entries.add(entries.size(), r);
        Iterator<IEntryListViewer> iterator = changeListeners.iterator();
        while (iterator.hasNext()){
                iterator.next().addEntry(r);
        }
    }
}
