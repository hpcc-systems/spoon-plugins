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
	

		
		
		
		
		//UNCatergorized basic ECL constructs
		List<String> arlListLogical = new ArrayList<String>();
		arlListLogical.add("AGGREGATE");
		arlListLogical.add("ALLNODES");
		arlListLogical.add("APPLY");
		arlListLogical.add("ASSERT");
		arlListLogical.add("BUILD");
		arlListLogical.add("BETWEEN");
		arlListLogical.add("CASE");
		arlListLogical.add("CHOOSE");
		arlListLogical.add("CHOOSEN");
		arlListLogical.add("CHOOSESETS");
		arlListLogical.add("COUNT");
		arlListLogical.add("COVARIANCE");
		arlListLogical.add("CRON");
		arlListLogical.add("DEFINE");
		arlListLogical.add("ERROR");
		arlListLogical.add("EVALUATE");
		arlListLogical.add("EXISTS");
		arlListLogical.add("EXP");
		arlListLogical.add("GETENV");
		arlListLogical.add("GROUP");
		arlListLogical.add("IF");
		arlListLogical.add("ISVALID");
		arlListLogical.add("LENGTH");
		arlListLogical.add("LIMIT");
		arlListLogical.add("LN");
		arlListLogical.add("NONEMPTY");
		arlListLogical.add("NORMALIZE");
		arlListLogical.add("MAP");
		arlListLogical.add("RANDMOM");
		arlListLogical.add("RANGE");
		arlListLogical.add("RANK");
		arlListLogical.add("REGEXFIND");
		arlListLogical.add("REGEXREPLACE");
		arlListLogical.add("SEQUENTIAL");
		arlListLogical.add("TRUNCATE");
		//arlListLogical.add("WHEN");
		arlListLogical.add("WHICH");
		mapFunctions.put("ECL Language", arlListLogical);
		
		
		//STD.Date functions
		List<String> arlListDate = new ArrayList<String>();
		arlListDate.add("STD.Date.Date_rec");
		arlListDate.add("STD.Date.Date_t");
		arlListDate.add("STD.Date.Days_t");
		arlListDate.add("STD.Date.Year");
		arlListDate.add("STD.Date.Month");
		arlListDate.add("STD.Date.Day");
		arlListDate.add("STD.Date.DateFromParts");
		arlListDate.add("STD.Date.IsLeapYear");
		arlListDate.add("STD.Date.FromGregorianYMD");
		arlListDate.add("STD.Date.ToGregorianYMD");
		mapFunctions.put("Date", arlListDate);
				
		// HASH Operations
		List<String> arlListHash = new ArrayList<String>();
		arlListHash.add("HASH");
		arlListHash.add("HASH32");
		arlListHash.add("HASH64");
		arlListHash.add("HASHMD5");
		
		mapFunctions.put("HASH", arlListHash);
		
		//math functions
		List<String> arlListMath = new ArrayList<String>();
		arlListMath.add("ABS");
		arlListMath.add("ACOS");
		arlListMath.add("ASIN");
		arlListMath.add("ATAN");
		arlListMath.add("ATAN2");
		arlListMath.add("AVE");
		arlListMath.add("COS");
		arlListMath.add("COSH");
		arlListMath.add("LOG");
		arlListMath.add("MAX");
		arlListMath.add("MIN");
		arlListMath.add("POWER");
		arlListMath.add("RANDOM");
		arlListMath.add("ROUND");
		arlListMath.add("ROUNDUP");
		arlListMath.add("SIN");
		arlListMath.add("SINH");
		arlListMath.add("SQRT");
		arlListMath.add("SUM");
		arlListMath.add("TAN");
		arlListMath.add("TANH");
		arlListMath.add("VARIANCE");
		mapFunctions.put("Math", arlListMath);
		
		//Numerical operations
		List<String> arlListNumber = new ArrayList<String>();
		arlListNumber.add("INTFORMAT");
		arlListNumber.add("REALFORMAT");
		arlListNumber.add("ROUND");
		arlListNumber.add("ROUNDUP");
		mapFunctions.put("Numerical", arlListNumber);
		
		//basic string functions
		List<String> arlListStringDefault = new ArrayList<String>();
		arlListStringDefault.add("TRIM");
		arlListStringDefault.add("ASCII");
		arlListStringDefault.add("EBCDIC");
		arlListStringDefault.add("FROMUNICODE");
		
		mapFunctions.put("String", arlListStringDefault);
		
		//STD.STR functiosn
		List<String> arlListString = new ArrayList<String>();
		arlListString.add("STD.Str.CleanSpaces");
		arlListString.add("STD.Str.CompareIgnoreCase");
		arlListString.add("STD.Str.Contains");
		arlListString.add("STD.Str.CountWords");
		arlListString.add("STD.Str.EditDistance");
		arlListString.add("STD.Str.EditDistanceWithinRadius");
		arlListString.add("STD.Str.EndsWith");
		arlListString.add("STD.Str.EqualIgnoreCase");
		arlListString.add("STD.Str.Extract");
		arlListString.add("STD.Str.Filter");
		arlListString.add("STD.Str.FilterOut");
		arlListString.add("STD.Str.Find");
		arlListString.add("STD.Str.FindCount");
		arlListString.add("STD.Str.FindReplace");
		arlListString.add("STD.Str.GetNthWord");
		arlListString.add("STD.Str.RemoveSuffix");
		arlListString.add("STD.Str.Reverse");
		arlListString.add("STD.Str.SplitWords");
		arlListString.add("STD.Str.SubstituteExcluded");
		arlListString.add("STD.Str.SubstituteIncluded");
		arlListString.add("STD.Str.StartsWith");
		arlListString.add("STD.Str.ToLowerCase");
		arlListString.add("STD.Str.ToTitleCase");
		arlListString.add("STD.Str.ToUpperCase");
		arlListString.add("STD.Str.WildMatch");
		arlListString.add("STD.Str.WordCount");
		
		
		mapFunctions.put("String (STD)", arlListString);
		
		//STD.Uni functions
		List<String> arlListStringUni = new ArrayList<String>();
		arlListStringUni.add("STD.Uni.CleanAccents");
		arlListStringUni.add("STD.Uni.CleanSpaces");
		arlListStringUni.add("STD.Uni.CompareAtStrength");
		arlListStringUni.add("STD.Uni.LocaleCompareAtStrength");
		arlListStringUni.add("STD.Uni.CompareIgnoreCase");
		arlListStringUni.add("STD.Uni.LocaleCompareIgnoreCase");
		arlListStringUni.add("STD.Uni.Contains");
		arlListStringUni.add("STD.Uni.EditDistance");
		arlListStringUni.add("STD.Uni.EditDistanceWithinRadius");
		arlListStringUni.add("STD.Uni.Extract");
		arlListStringUni.add("STD.Uni.Filter");
		arlListStringUni.add("STD.Uni.FilterOut");
		arlListStringUni.add("STD.Uni.Find");
		arlListStringUni.add("STD.Uni.LocaleFind");
		arlListStringUni.add("STD.Uni.LocaleFindAtStrength");
		arlListStringUni.add("STD.Uni.LocaleFindAtStrengthReplace");
		arlListStringUni.add("STD.Uni.FindReplace");
		arlListStringUni.add("STD.Uni.LocaleFindReplace");
		arlListStringUni.add("STD.Uni.GetNthWord");
		arlListStringUni.add("STD.Uni.Reverse");
		arlListStringUni.add("STD.Uni.SubstituteExcluded");
		arlListStringUni.add("STD.Uni.SubstituteIncluded");
		arlListStringUni.add("STD.Uni.ToLowerCase");
		arlListStringUni.add("STD.Uni.LocaleToLowerCase");
		arlListStringUni.add("STD.Uni.ToTitleCase");
		arlListStringUni.add("STD.Uni.LocaleToTitleCase");
		arlListStringUni.add("STD.Uni.ToUpperCase");
		arlListStringUni.add("STD.Uni.LocaleToUpperCase");
		arlListStringUni.add("STD.Uni.WildMatch");
		arlListStringUni.add("STD.Uni.WordCount");

		mapFunctions.put("String Unicode (STD)", arlListStringUni);
		
		
		
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
				child.setText("input." + value[i]);
			}
		}
		// Turn drawing back on!
		tree.setRedraw(true);
	}
	
	public static void fillTreeForOperators(Tree tree) {
		// Turn off drawing to avoid flicker
		tree.setRedraw(false);
		
		String operatorList[] = {":=","+", "-", "*", "/", "%", "||", "(", ")", "=", "<>", ">", "<", "<=", ">=","~","AND","IN","NOT","OR","XOR"};
		// Create five root items
		for (int i = 0; i < operatorList.length; i++) {
			TreeItem item = new TreeItem(tree, SWT.NONE);
			item.setText(operatorList[i]);
		}
		// Turn drawing back on!
		tree.setRedraw(true);
	}
	
}
