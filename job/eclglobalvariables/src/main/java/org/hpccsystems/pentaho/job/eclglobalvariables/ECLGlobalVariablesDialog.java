/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclglobalvariables;



import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryDialogInterface;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.job.dialog.JobDialog;
import org.pentaho.di.ui.job.entry.JobEntryDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import java.io.*;

import org.hpccsystems.eclguifeatures.*;
import org.hpccsystems.ecljobentrybase.*;

import com.hpccsystems.resources.IncludeLibPropertiesReader;
import com.hpccsystems.resources.PropertiesReader;


/**
 *
 * @author ChambersJ
 */
public class ECLGlobalVariablesDialog extends ECLJobEntryDialog{//extends JobEntryDialog implements JobEntryDialogInterface {

    private ECLGlobalVariables jobEntry;
    
    private boolean includeSaltLib = true;
    private boolean includeMLLib = true;
    private Text jobEntryName;

    private Text userName;
    private Text password;
    private Text serverIP;
    private Text serverPort;
    private Text landingZone;
    
    private Text cluster;
    private Text jobName;
    private Text maxReturn;
    private Text eclccInstallDir;
    private Text mlPath;
    private Combo includeML;
    
    private Text compileFlags;
 /*
             *  private String mlPath = "ecl-ml";
    private String eclccInstallDir = "C:\\Program Files\\HPCC Systems\\HPCC\\bin\\ver_3_0\\";
    private String jobName = "Spoon-job";
    private String cluster = "hthor";
    private boolean includeML = true;
             */
                 
    
    
    
    private Text SALTPath;
    private Combo includeSALT;

    private Button wOK, wCancel, mlFileOpenButton, eclFileOpenButton,saltFileOpenButton;
    private boolean backupChanged;
    private SelectionAdapter lsDef;

    public ECLGlobalVariablesDialog(Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta) {
        super(parent, jobEntryInt, rep, jobMeta);
       
        if(IncludeLibPropertiesReader.getProperty("includeSalt").equalsIgnoreCase("true")){
        	includeSaltLib = true;
        }else{
        	includeSaltLib = false;
        }
        if(IncludeLibPropertiesReader.getProperty("includeML").equalsIgnoreCase("true")){
        	includeMLLib = true;
        }else{
        	includeMLLib = false;
        }
        
        jobEntry = (ECLGlobalVariables) jobEntryInt;
        if (this.jobEntry.getName() == null) {
            this.jobEntry.setName("Global Variables");
        }
    }

    public JobEntryInterface open() {
        Shell parentShell = getParent();
        Display display = parentShell.getDisplay();

        shell = new Shell(parentShell, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);

        props.setLook(shell);
        JobDialog.setShellImage(shell, jobEntry);

        ModifyListener lsMod = new ModifyListener() {

            public void modifyText(ModifyEvent e) {
                jobEntry.setChanged();
            }
        };
        
        String datasets[] = null;
        AutoPopulate ap = new AutoPopulate();
        try{
            //Object[] jec = this.jobMeta.getJobCopies().toArray();
            
            datasets = ap.parseDatasets(this.jobMeta.getJobCopies());
        }catch (Exception e){
            System.out.println("Error Parsing existing Datasets");
            System.out.println(e.toString());
            datasets = new String[]{""};
        }

        backupChanged = jobEntry.hasChanged();

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;


        shell.setLayout(formLayout);
        shell.setText("Global Variables");

        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;

        shell.setLayout(formLayout);
        shell.setText("Define an ECL Variables");

        FormLayout groupLayout = new FormLayout();
        groupLayout.marginWidth = 10;
        groupLayout.marginHeight = 10;

        // Stepname line
        Group generalGroup = new Group(shell, SWT.SHADOW_NONE);
        props.setLook(generalGroup);
        generalGroup.setText("General Details");
        generalGroup.setLayout(groupLayout);
        FormData generalGroupFormat = new FormData();
        generalGroupFormat.top = new FormAttachment(0, margin);
        generalGroupFormat.width = 400;
        generalGroupFormat.height = 50;
        generalGroupFormat.left = new FormAttachment(middle, 0);
        generalGroup.setLayoutData(generalGroupFormat);
        
        jobEntryName = buildText("Job Entry Name", null, lsMod, middle, margin, generalGroup);

        //All other contols
        //Distribute Declaration
        Group varGroup = new Group(shell, SWT.SHADOW_NONE);
        props.setLook(varGroup);
        varGroup.setText("Server Details");
        varGroup.setLayout(groupLayout);
        FormData varGroupFormat = new FormData();
        varGroupFormat.top = new FormAttachment(generalGroup, margin);
        varGroupFormat.width = 400;
        varGroupFormat.height = 305;
        varGroupFormat.left = new FormAttachment(middle, 0);
        varGroup.setLayoutData(varGroupFormat);

        //name = buildText("Distribute Name", null, lsMod, middle, margin, distributeGroup);

        userName = buildText("Server Username", null, lsMod, middle, margin, varGroup);
        password = buildPassword("Server Password", userName, lsMod, middle, margin, varGroup);
        serverIP = buildText("Server Host", password, lsMod, middle, margin, varGroup);
        serverPort = buildText("Server Port", serverIP, lsMod, middle, margin, varGroup);
        landingZone = buildText("Landing Zone Dir", serverPort, lsMod, middle, margin, varGroup);
        
        //move thes to Job Information
        cluster = buildText("Cluster", landingZone, lsMod, middle, margin, varGroup);
        jobName = buildText("Job Name", cluster, lsMod, middle, margin, varGroup);
        maxReturn = buildText("Preview Rows", jobName, lsMod, middle, margin, varGroup);
        //move these to Library(s)
        
        eclccInstallDir = buildText("eclcc Install Dir", maxReturn, lsMod, middle, margin, varGroup);
        this.eclFileOpenButton = buildButton("Choose Location", eclccInstallDir, lsMod, middle, margin, varGroup);
        controls.put("fOpen", eclccInstallDir);
        
        Listener eclFileOpenListener = new Listener() {

            public void handleEvent(Event e) {
                String newFile = buildDirectoryDialog();
                if(newFile != ""){
                	eclccInstallDir.setText(newFile);
                }
            }
        };
        this.eclFileOpenButton.addListener(SWT.Selection, eclFileOpenListener);
        
        this.compileFlags = this.buildMultiText("Compile Flags", eclFileOpenButton, lsMod, middle, margin, varGroup,30);
        
        Group mlGroup = null;
        if(includeMLLib){
        	mlGroup = new Group(shell, SWT.SHADOW_NONE);
            props.setLook(mlGroup);
            mlGroup.setText("ML Details");
            mlGroup.setLayout(groupLayout);
            FormData mlGroupData = new FormData();
            mlGroupData.top = new FormAttachment(varGroup, margin);
            mlGroupData.width = 400;
            mlGroupData.height = 95;
            mlGroupData.left = new FormAttachment(middle, 0);
            mlGroup.setLayoutData(mlGroupData);
            
	        includeML = buildCombo("Include ML Library?", compileFlags, lsMod, middle, margin, mlGroup, new String[]{"true", "false"});
	        mlPath = buildText("Path to ML Library", includeML, lsMod, middle, margin, mlGroup);
	        this.mlFileOpenButton = buildButton("Choose Location", mlPath, lsMod, middle, margin, mlGroup);
	        controls.put("fOpen", mlFileOpenButton);
	        
	        Listener mlFileOpenListener = new Listener() {
	
	            public void handleEvent(Event e) {
	                String newFile = buildDirectoryDialog();
	                if(newFile != ""){
	                	mlPath.setText(newFile);
	                }
	            }
	        };
	        this.mlFileOpenButton.addListener(SWT.Selection, mlFileOpenListener);
        }
        Group saltGroup = null;
        if(includeSaltLib){
        	saltGroup = new Group(shell, SWT.SHADOW_NONE);
            props.setLook(saltGroup);
            saltGroup.setText("SALT Details");
            saltGroup.setLayout(groupLayout);
            FormData saltGroupData = new FormData();
            if(includeMLLib){
            	saltGroupData.top = new FormAttachment(mlGroup, margin);
            }else{
            	saltGroupData.top = new FormAttachment(varGroup, margin);
            }
            saltGroupData.width = 400;
            saltGroupData.height = 95;
            saltGroupData.left = new FormAttachment(middle, 0);
            saltGroup.setLayoutData(saltGroupData);
            
        	includeSALT = buildCombo("Include SALT Library?", mlFileOpenButton, lsMod, middle, margin, saltGroup, new String[]{"true", "false"});
        	SALTPath = buildText("Path to SALT Library", includeSALT, lsMod, middle, margin, saltGroup);
        	// includeSALT.setVisible(false);
        	// SALTPath.setVisible(false);
        	this.saltFileOpenButton = buildButton("Choose Location", SALTPath, lsMod, middle, margin, saltGroup);
        	controls.put("fOpen", saltFileOpenButton);
        
        	Listener saltFileOpenListener = new Listener() {

        		public void handleEvent(Event e) {
        			String newFile = buildDirectoryDialog();
        			if(newFile != ""){
        				SALTPath.setText(newFile);
        			}
        		}
        	};
        	this.saltFileOpenButton.addListener(SWT.Selection, saltFileOpenListener);
        }
        
        
        wOK = new Button(shell, SWT.PUSH);
        wOK.setText("OK");
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText("Cancel");
        if(includeSaltLib){
        	BaseStepDialog.positionBottomButtons(shell, new Button[]{wOK, wCancel}, margin, saltGroup);
        }else if(includeMLLib){
        	BaseStepDialog.positionBottomButtons(shell, new Button[]{wOK, wCancel}, margin, mlGroup);
        }else{
        	BaseStepDialog.positionBottomButtons(shell, new Button[]{wOK, wCancel}, margin, varGroup);
        }
        // Add listeners
        Listener cancelListener = new Listener() {

            public void handleEvent(Event e) {
                cancel();
            }
        };
        Listener okListener = new Listener() {

            public void handleEvent(Event e) {
            	updatePaths();
            	boolean isReady = verifySettings();
            	if(isReady){
            		ok();
            	}else{
            		
            	}
            }
        };

        wCancel.addListener(SWT.Selection, cancelListener);
        wOK.addListener(SWT.Selection, okListener);

        lsDef = new SelectionAdapter() {

            public void widgetDefaultSelected(SelectionEvent e) {
            	updatePaths();
            	boolean isReady = verifySettings();
            	if(isReady){
            		ok();
            	}else{
            		
            	}
            }
        };


        // Detect X or ALT-F4 or something that kills this window...

        shell.addShellListener(new ShellAdapter() {

            public void shellClosed(ShellEvent e) {
                cancel();
            }
        });
        


        PropertiesReader.getApplicationProperties();
        
        //if (jobEntry.getJobName() != null) {
        //    jobEntryName.setText(jobEntry.getJobName());
        //}
        if (jobEntry.getName() != null) {
            jobEntryName.setText(jobEntry.getName());
        }
        if (jobEntry.getServerIP() != null && !jobEntry.getServerIP().equals("")) {
            serverIP.setText(jobEntry.getServerIP());
            System.out.println("IP -- using set value of : " + jobEntry.getServerIP());
        }else{
        	serverIP.setText(PropertiesReader.getProperty("hpcc.host"));
        	System.out.println("IP -- using default value of : " + PropertiesReader.getProperty("hpcc.host"));
        }
        if (jobEntry.getServerPort() != null && !jobEntry.getServerPort().equals("")) {
            serverPort.setText(jobEntry.getServerPort());
        }else{
        	serverPort.setText(PropertiesReader.getProperty("hpcc.port"));
        }
        
        if (jobEntry.getLandingZone() != null && !jobEntry.getLandingZone().equals("")) {
            landingZone.setText(jobEntry.getLandingZone());
        }else{
        	landingZone.setText(PropertiesReader.getProperty("hpcc.landingzone"));
        }

        
        if (jobEntry.getCluster() != null && !jobEntry.getCluster().equals("")) {
            cluster.setText(jobEntry.getCluster());
        }else{
        	cluster.setText(PropertiesReader.getProperty("hpcc.cluster"));
        }
        
        if (jobEntry.getJobName() != null) {
            jobName.setText(jobEntry.getJobName());
        }
        if (jobEntry.getMaxReturn() != null && !jobEntry.getMaxReturn().equals("")) {
            maxReturn.setText(jobEntry.getMaxReturn());
        }else{
        	maxReturn.setText(PropertiesReader.getProperty("hpcc.maxreturn"));
        }
        if (jobEntry.getEclccInstallDir() != null && !jobEntry.getEclccInstallDir().equals("")) {
            eclccInstallDir.setText(jobEntry.getEclccInstallDir());
        }else{
        	eclccInstallDir.setText(PropertiesReader.getProperty("hpcc.eclcc"));
        }
        if(includeMLLib){
	        if (jobEntry.getMlPath() != null && !jobEntry.getMlPath().equals("")) {
	            mlPath.setText(jobEntry.getMlPath());
	        }else{
	        	 mlPath.setText(PropertiesReader.getProperty("ml.lib"));
	        }
	        if (jobEntry.getIncludeML() != null) {
	            includeML.setText(jobEntry.getIncludeML());
	        }
        }
        if(includeSaltLib){
	        if (jobEntry.getSALTPath() != null && !jobEntry.getSALTPath().equals("")) {
	            SALTPath.setText(jobEntry.getSALTPath());
	        }else{
	        	SALTPath.setText(PropertiesReader.getProperty("salt.lib"));
	        }
	        if (jobEntry.getIncludeSALT() != null) {
	            includeSALT.setText(jobEntry.getIncludeSALT());
	        }
        }
        if (jobEntry.getUser() != null) {
            userName.setText(jobEntry.getUser());
        }
        if (jobEntry.getPass() != null) {
            password.setText(jobEntry.getPass());
        }
      
        if (jobEntry.getCompileFlags() != null) {
            compileFlags.setText(jobEntry.getCompileFlags());
        }
        
       
       

        shell.pack();
        shell.open();
        while (!shell.isDisposed()) {
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }

        return jobEntry;

    }
    private void updatePaths(){
    	String eclPath = eclccInstallDir.getText();
    	String slash = "\\";
    	if(eclPath.contains("/") && !eclPath.contains("\\")){
    		slash = "/";
    	}
    	boolean eclLast = false;
    	if(eclPath.lastIndexOf("\\") == (eclPath.length()-1)){
    		//has last \
    		eclLast = true;
    	}
    	if(!eclLast && eclPath.lastIndexOf("/") == (eclPath.length()-1)){
    		eclLast = true;
    	}
    	if(!eclLast){
    		eclccInstallDir.setText(eclccInstallDir.getText() + slash);
    	}
    	
    	if(includeML.getText().equals("true")){
	    	String mlP = mlPath.getText();
	    	boolean mlLast = false;
	    	
	    	if(mlP.lastIndexOf("\\") == (mlP.length()-1)){
	    		//has last \
	    		mlLast = true;
	    	}
	    	if(!mlLast && mlP.lastIndexOf("/") == (mlP.length()-1)){
	    		mlLast = true;
	    	}
	    	if(mlLast){	    		
	    		String noSlash = (mlPath.getText()).substring(0,(mlPath.getText()).length()-1);
	    		mlPath.setText(noSlash);
	    	}
    	}
    }
    private boolean verifySettings(){
    	boolean isReady = false;
    	boolean eclccExists = true;
    	boolean mlExists = true;
    	boolean saltExists = true;
    	
    	String errorTxt = "Some Fields Were Not Correct:\r\n";
    	
    	/*eclccExists = (new File(eclccInstallDir.getText())).exists();
    	if(!eclccExists){
    		//warn
    		System.out.println("no eclcc install found");
    		errorTxt += "The \"eclcc Install Dir\" could not be located\r\n";
    	}*/
    	if(includeMLLib){
	    	if(includeML.getText().equals("true")){
	    		mlExists = (new File(mlPath.getText())).exists();
	    		if(!mlExists){
	    			//warn
	    			errorTxt += "The \"Path to ML Library\" could not be located\r\n";
	    			System.out.println("No ML Library found");
	    		}
	    	}
	    	//eclccExists && 
	    	if(mlExists){
	    		isReady = true;
	    		System.out.println("paths validated");
	    	}else{
	    		Shell parentShell = getParent();
	            //Display display = parentShell.getDisplay();
	    		//final Shell dialog = new Shell (display, SWT.DIALOG_TRIM);
	    		final Shell dialog = new Shell(parentShell, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
	
	    		Label label = new Label (dialog, SWT.NONE);
	    		label.setText (errorTxt);
	    		Button okButton = new Button (dialog, SWT.PUSH);
	    		okButton.setText ("&OK");
	   
		        Listener cancelListener = new Listener() {
	
		            public void handleEvent(Event e) {
		                dialog.close();
		            }
		        };
		        
		        okButton.addListener(SWT.Selection, cancelListener);
		        
		        FormLayout form = new FormLayout ();
		    	form.marginWidth = form.marginHeight = 8;
		    	dialog.setLayout (form);
		    	FormData okData = new FormData ();
		    	okData.top = new FormAttachment (label, 8);
		    	okButton.setLayoutData (okData);
		    	
		        
		        dialog.setDefaultButton (okButton);
		    	dialog.pack ();
		    	dialog.open ();
	    	}
    	}
    	if(includeSaltLib){
			if(includeSALT.getText().equals("true")){
				saltExists = (new File(SALTPath.getText())).exists();
				if(!saltExists){
					//warn
					errorTxt += "The \"Path to SALT Library\" could not be located\r\n";
					System.out.println("No SALT Library found");
				}
			}
	    	if(saltExists && eclccExists && mlExists){
		    		isReady = true;
		    		System.out.println("paths validated");
			}else{
				Shell parentShell = getParent();
				//Display display = parentShell.getDisplay();
				//final Shell dialog = new Shell (display, SWT.DIALOG_TRIM);
				final Shell dialog = new Shell(parentShell, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
				System.out.println("No SALT Library found - dialog open");
				Label label = new Label (dialog, SWT.NONE);
				label.setText (errorTxt);
				Button okButton = new Button (dialog, SWT.PUSH);
				okButton.setText ("&OK");
	   
				Listener cancelListener = new Listener() {
	
					public void handleEvent(Event e) {
						dialog.close();
					}
				};
				
				okButton.addListener(SWT.Selection, cancelListener);
				
				FormLayout form = new FormLayout ();
				form.marginWidth = form.marginHeight = 8;
				dialog.setLayout (form);
				FormData okData = new FormData ();
				okData.top = new FormAttachment (label, 8);
				okButton.setLayoutData (okData);
				
				
				dialog.setDefaultButton (okButton);
				dialog.pack ();
				dialog.open ();
			
			
			}
    	}
    	return isReady;
    }


    private boolean validate(){
    	boolean isValid = true;
    	String errors = "";
    	
    	//only need to require a entry name
    	if(this.jobEntryName.getText().equals("")){
    		//one is required.
    		isValid = false;
    		errors += "You must provide a \"Job Entry Name\"!\r\n";
    	}
    	
    	if(this.serverIP.getText().equals("")){
    		//one is required.
    		isValid = false;
    		errors += "You must provide a \"Server Host\"!\r\n";
    	}
    	
    	if(this.serverPort.getText().equals("")){
    		//one is required.
    		isValid = false;
    		errors += "You must provide a \"Server Port\"!\r\n";
    	}
    	
    	if(this.landingZone.getText().equals("")){
    		//one is required.
    		isValid = false;
    		errors += "You must provide a \"Landing Zone Dir\"!\r\n";
    	}
    	
    	if(this.cluster.getText().equals("")){
    		//one is required.
    		isValid = false;
    		errors += "You must provide a \"Cluster\"!\r\n";
    	}
    	
    	if(this.jobName.getText().equals("")){
    		//one is required.
    		isValid = false;
    		errors += "You must provide a \"Job Name\"!\r\n";
    	}
    	/*if(this.eclccInstallDir.getText().equals("")){
    		//one is required.
    		isValid = false;
    		errors += "You must provide a \"eclcc Install Dir\"!\r\n";
    	}*/
    	
    	if(includeMLLib && this.includeML.getText().equals("true")){
    		if(this.mlPath.getText().equals("")){
        		//one is required.
        		isValid = false;
        		errors += "You must provide a \"Path to ML Library\" when Include ML Library is set to ture!\r\n";
        	}
    	}
    	
    	

		if(!isValid){
			ErrorNotices en = new ErrorNotices();
			errors += "\r\n";
			errors += "If you continue to save with errors you may encounter compile errors if you try to execute the job.\r\n\r\n";
			isValid = en.openValidateDialog(getParent(),errors);
		}
		return isValid;
		
	}
    
    private void ok() {
    	if(!validate()){
    		return;
    	}
    	
        //jobEntry.setJobName(jobEntryName.getText());
        jobEntry.setName(jobEntryName.getText());
        
        jobEntry.setServerIP(serverIP.getText());
        jobEntry.setServerPort(serverPort.getText());
        jobEntry.setLandingZone(landingZone.getText());
        
        jobEntry.setCluster(cluster.getText());
        jobEntry.setJobName(jobName.getText());
        jobEntry.setMaxReturn(maxReturn.getText());
        
        jobEntry.setEclccInstallDir(eclccInstallDir.getText());
        if(includeMLLib){
        	jobEntry.setMlPath(mlPath.getText());
        	jobEntry.setIncludeML(includeML.getText());
        }
        jobEntry.setUser(userName.getText());
        jobEntry.setPass(password.getText());
		if(includeSaltLib){
	        jobEntry.setSALTPath(SALTPath.getText());
	        jobEntry.setIncludeSALT(includeSALT.getText());
		}
        jobEntry.setCompileFlags(compileFlags.getText());
        dispose();
    }

    private void cancel() {
        jobEntry.setChanged(backupChanged);
        jobEntry = null;
        dispose();
    }

}
