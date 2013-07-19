package com.hpccsystems.salt.helpers;

public class DatasetNode {

	private String fieldName = "";
	private String fieldType = "";
	private String specificity = "";
	private String caps = "";
	private String leftTrim = "";
	private String rightTrim = "";
	private String allow = "";
	private String onFail = "";
	
	public String getFieldName() {
		return fieldName;
	}
	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}
	public String getFieldType() {
		return fieldType;
	}
	public void setFieldType(String fieldType) {
		this.fieldType = fieldType;
	}
	public String getSpecificity() {
		return specificity;
	}
	public void setSpecificity(String specificity) {
		this.specificity = specificity;
	}
	
	public String getCaps() {
		return caps;
	}
	public void setCaps(String caps) {
		this.caps = caps;
	}
	public String getLeftTrim() {
		return leftTrim;
	}
	public void setLeftTrim(String leftTrim) {
		this.leftTrim = leftTrim;
	}
	public String getRightTrim() {
		return rightTrim;
	}
	public void setRightTrim(String rightTrim) {
		this.rightTrim = rightTrim;
	}
	public String getAllow() {
		return allow;
	}
	public void setAllow(String allow) {
		this.allow = allow;
	}
	public String getOnFail() {
		return onFail;
	}
	public void setOnFail(String onFail) {
		this.onFail = onFail;
	}
	public DatasetNode(String fieldName, String fieldType, String specificity){
		this.fieldName = fieldName;
		this.fieldType = fieldType;
		this.specificity = specificity;
	}
	
}
