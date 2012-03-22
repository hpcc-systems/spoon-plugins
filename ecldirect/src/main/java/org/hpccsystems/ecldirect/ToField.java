/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.ecldirect;

/**
 *
 * @author ChalaAX
 */
public class ToField implements EclCommand {
    private String name;
    private String inDS;
    private String outDS;

    public String getInDS() {
        return inDS;
    }

    public void setInDS(String inDS) {
        this.inDS = inDS;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOutDS() {
        return outDS;
    }

    public void setOutDS(String outDS) {
        this.outDS = outDS;
    }

   
    
    @Override
    public String ecl() {
        return "ml.ToField(" + inDS + "," + outDS + ");\r\n";
    }

    @Override
    public CheckResult check() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    
}
