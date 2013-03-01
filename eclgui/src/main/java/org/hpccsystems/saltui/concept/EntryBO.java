package org.hpccsystems.saltui.concept;

import java.util.ArrayList;

import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;


public class EntryBO {
	private String field = "";
	private String ruleName = "";
	private ConceptRuleList hygieneRuleList = new ConceptRuleList();
	private int	hygieneRuleListIndex = -1;
	
	public EntryBO(){}
	
	public EntryBO(String in){
        super();
        fromCSV(in);
    }
	
	
	

	public int getHygieneRuleListIndex() {
		return hygieneRuleListIndex;
	}

	public void setHygieneRuleListIndex(int hygieneRuleListIndex) {
		this.hygieneRuleListIndex = hygieneRuleListIndex;
	}

	public String getField() {
		return field;
	}
	public void setField(String field) {
		this.field = field;
	}
	public String getRuleName() {
		return ruleName;
	}
	public void setRuleName(String ruleName) {
		this.ruleName = ruleName;
	}
	public ConceptRuleList getHygieneRuleList() {
		return hygieneRuleList;
	}

	public void setHygieneRuleList(ConceptRuleList hygieneRuleList) {
		this.hygieneRuleList = hygieneRuleList;
	}

	
		public String toCSV(){
			String csv = new String();
			
			csv += field;
			csv += ","+ruleName;
			csv += ","+hygieneRuleListIndex;
			//csv += ","+fieldTypeList.toPipeDelm();
			
            return csv;
        }
        
        public void fromCSV(String in){
            String[] strArr = in.split("[,]");//"\\,"
            //System.out.println("in ---- " + in);
            if(strArr.length == 3){
                field = strArr[0];
                ruleName = strArr[1];
                hygieneRuleListIndex = Integer.parseInt(strArr[2]);
                //fieldTypeList.fromPipeDelm(strArr[2]);
            } else{
            	field = "";
                ruleName = "";
                hygieneRuleListIndex = -1;
                //fieldTypeList.fromPipeDelm("");
            }
        }
	

}
