package org.hpccsystems.mapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

public class Utils {

	public static Map<String, List<String>> getFunctionValueMap(){
		
		Map<String, List<String>> mapFunctions = new TreeMap<String, List<String>>();
		List<String> arlListMath = new ArrayList<String>();
		arlListMath.add("ABS");
		arlListMath.add("ACOS");
		arlListMath.add("ASIN");
		arlListMath.add("ATAN");
		arlListMath.add("ATAN2");
		arlListMath.add("COS");
		
		mapFunctions.put("Math", arlListMath);
		
		List<String> arlListLogical = new ArrayList<String>();
		arlListLogical.add("IF");
		arlListLogical.add("MAP");
		
		mapFunctions.put("Logical", arlListLogical);
		
		List<String> arlListString = new ArrayList<String>();
		arlListString.add("TRIM");
		arlListString.add("TRUNCATE");
		
		mapFunctions.put("String", arlListString);
		
		
		
		return mapFunctions;
	}
	
	public static void fillTreeForFunctions(Tree tree, Map<String, List<String>> mapTreeElements) {
		
		tree.setRedraw(false);
		
		for (Map.Entry<String, List<String>> entry : mapTreeElements.entrySet()) {
		    TreeItem item = new TreeItem(tree, SWT.NONE);
			item.setText(entry.getKey());
			//item.setImage(new Image(tree.getDisplay(), "c:\\icons\\folder_blue.gif"));
			
			List<String> arlList = entry.getValue();
			for (String strValue : arlList) {
				TreeItem child = new TreeItem(item, SWT.NONE);
				child.setText(strValue);
				//child.setImage(new Image(tree.getDisplay(), "c:\\icons\\folder_open_blue.gif"));
			}
		}
		
		// Turn drawing back on!
		tree.setRedraw(true);
	}
	
	public static void fillTree(Tree tree, Map<String, String[]> mapDataSets) {
		
		for (Map.Entry<String, String[]> entry : mapDataSets.entrySet()) {
		    String key = entry.getKey();
		    TreeItem item = new TreeItem(tree, SWT.NONE);
			item.setText(key);
		    
			String[] value = entry.getValue();
			for (int i = 0; i < value.length; i++) {
				TreeItem child = new TreeItem(item, SWT.NONE);
				child.setText(value[i]);
			}
		}
		// Turn drawing back on!
		tree.setRedraw(true);
	}
	
	public static void fillTreeForOperators(Tree tree) {
		// Turn off drawing to avoid flicker
		tree.setRedraw(false);
		
		String operatorList[] = {"+", "-", "*", "/", "%", "||", "(", ")", "=", "<>", ">", "<", "<=", ">="};
		// Create five root items
		for (int i = 0; i < operatorList.length; i++) {
			TreeItem item = new TreeItem(tree, SWT.NONE);
			item.setText(operatorList[i]);
		}
		// Turn drawing back on!
		tree.setRedraw(true);
	}
	
}
