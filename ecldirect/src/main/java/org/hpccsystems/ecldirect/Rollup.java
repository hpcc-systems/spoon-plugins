/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.ecldirect;

/**
 *
 * @author ChalaAX
 */
public class Rollup implements EclCommand {

    private String name;
    private String recordset;
    private String condition;
    private String transformName;
    private String transform;
    private String transformCall;
    private String fieldlist;
    private String group;
    private Boolean runLocal;//optional

    public String getTransformCall() {
        return transformCall;
    }

    public void setTransformCall(String transformCall) {
        this.transformCall = transformCall;
    }

    public String getTransformName() {
        return transformName;
    }

    public void setTransformName(String transformName) {
        this.transformName = transformName;
    }

    
    
    public String getGroup() {
        return group;
    }

    public void setGroup(String GROUP) {
        this.group = GROUP;
    }

    public Boolean getRunLocal() {
        return runLocal;
    }

    public void setRunLocal(Boolean LOCAL) {
        this.runLocal = LOCAL;
    }



    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public String getFieldlist() {
        return fieldlist;
    }

    public void setFieldlist(String fieldlist) {
        this.fieldlist = fieldlist;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getRecordset() {
        return recordset;
    }

    public void setRecordset(String recordset) {
        this.recordset = recordset;
    }

    public String getTransform() {
        return transform;
    }

    public void setTransform(String transform) {
        this.transform = transform;
    }

    

    @Override
    public String ecl() {
        
        String ecl = transformName + " := TRANSFORM \r\n" + transform + "\r\n" + "END; \r\n";
        
        ecl += name + ":=ROLLUP(" + recordset;
        //add local if its set if not its optional
        //ROLLUP(recordset, condition, transform [, LOCAL] )
        
        
        if(condition != null && !condition.equals("")){
            ecl+= "," +condition;
            if(transformCall != null && !transformCall.equals("")){
                ecl += "," + transformCall;
                if (runLocal != null && runLocal) {
                        ecl += ", local";
                    }
            }
        }else{
            if (group != null && !group.equals("")) {
                //ROLLUP(recordset, GROUP, transform )
                ecl += "," + group;
                if (transformCall != null && !transformCall.equals("")){
                    ecl += "," + transformCall;
                }
            }else{
                //ROLLUP(recordset, transform, fieldlist [, LOCAL] )
                if(transformCall != null && !transformCall.equals("")){
                    ecl += "," + transformCall;
                    if (fieldlist != null && !fieldlist.equals("")) {
                       ecl += "," + fieldlist;

                        if (runLocal != null && runLocal) {
                            ecl += ", local";
                        }
                    }
                }
            }
        }
        //close out the ecl call
        ecl += ");\r\n";
        
        return ecl;
    }

    @Override
    public CheckResult check() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
