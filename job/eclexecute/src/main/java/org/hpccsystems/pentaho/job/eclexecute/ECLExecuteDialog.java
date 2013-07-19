/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclexecute;

import java.io.File;
import java.util.ArrayList;
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
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Shell;






import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.job.JobHopMeta;

import org.pentaho.di.job.entry.JobEntryCopy;
import org.hpccsystems.eclguifeatures.*;
//JobEntry






import java.util.HashMap;
import org.eclipse.swt.widgets.DirectoryDialog;
import org.hpccsystems.ecljobentrybase.*;
/**
 *
 * @author ChambersJ
 */
public class ECLExecuteDialog extends ECLJobEntryDialog{//extends JobEntryDialog implements JobEntryDialogInterface {

    private ECLExecute jobEntry;
    private Text jobEntryName;

    private Button wOK, wCancel, fileOpenButton;
    private boolean backupChanged;
    private SelectionAdapter lsDef;
    private Combo debugLevel;
   
    //private Combo attributeName;
    private Text fileName;
    //private Text serverAddress;
    
    
    private HashMap controls = new HashMap();

    public ECLExecuteDialog(Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta) {
        super(parent, jobEntryInt, rep, jobMeta);
        jobEntry = (ECLExecute) jobEntryInt;
        if (this.jobEntry.getName() == null) {
            this.jobEntry.setName("Execute");
        }
        
        
    }

    

    
    public JobEntryInterface open() {
        System.out.println(" ------------ open ------------- ");
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

        backupChanged = jobEntry.hasChanged();

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;


        shell.setLayout(formLayout);
        shell.setText("Execute");

        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;

        shell.setLayout(formLayout);
        shell.setText("ECL Execute");

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
        generalGroupFormat.height = 65;
        generalGroupFormat.left = new FormAttachment(middle, 0);
        generalGroup.setLayoutData(generalGroupFormat);
        
        jobEntryName = buildText("Job Entry Name", null, lsMod, middle, margin, generalGroup);


           //All other contols
        //Output Declaration
        Group fileGroup = new Group(shell, SWT.SHADOW_NONE);
        props.setLook(fileGroup);
        fileGroup.setText("Configuration Details");
        fileGroup.setLayout(groupLayout);
        FormData fileGroupFormat = new FormData();
        fileGroupFormat.top = new FormAttachment(generalGroup, margin);
        fileGroupFormat.width = 400;
        fileGroupFormat.height = 150;
        fileGroupFormat.left = new FormAttachment(middle, 0);
        fileGroup.setLayoutData(fileGroupFormat);
        
        
        //this.serverAddress = buildText("Server Address", fileGroup, lsMod, middle, margin, fileGroup);
        //controls.put("serverAddress", serverAddress);
        this.debugLevel = buildCombo("Compile Check", null, lsMod, middle, margin, fileGroup, new String[]{"None", "Stop on Errors", "Stop on Errors or Warnings"});
        Label lb = buildLabel("Compile Check will occur before passing the code to the Cluster.\r\n\r\n", this.debugLevel, lsMod, 0, margin, fileGroup);

        
        this.fileName = buildText("Output File(s) Directory", lb, lsMod, middle, margin, fileGroup);
        controls.put("fileName", fileName);
        
        this.fileOpenButton = buildButton("Choose Location", fileName, lsMod, middle, margin, fileGroup);
        controls.put("fOpen", fileOpenButton);

        Listener fileOpenListener = new Listener() {

            public void handleEvent(Event e) {
                String newFile = buildDirectoryDialog();
                if(newFile != ""){
                    fileName.setText(newFile);
                }
            }
        };
        this.fileOpenButton.addListener(SWT.Selection, fileOpenListener);
        
        


        //controls.put("fOpen", fOpen);

        wOK = new Button(shell, SWT.PUSH);
        wOK.setText("OK");
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText("Cancel");

       BaseStepDialog.positionBottomButtons(shell, new Button[]{wOK, wCancel}, margin, fileGroup);

        // Add listeners
        Listener cancelListener = new Listener() {

            public void handleEvent(Event e) {
                cancel();
            }
        };
        Listener okListener = new Listener() {

            public void handleEvent(Event e) {
                ok();
            }
        };

        wCancel.addListener(SWT.Selection, cancelListener);
        wOK.addListener(SWT.Selection, okListener);

        lsDef = new SelectionAdapter() {

            public void widgetDefaultSelected(SelectionEvent e) {
                ok();
            }
        };


        // Detect X or ALT-F4 or something that kills this window...

        shell.addShellListener(new ShellAdapter() {

            public void shellClosed(ShellEvent e) {
                cancel();
            }
        });

        if (jobEntry.getName() != null) {
            jobEntryName.setText(jobEntry.getName());
        }


        if (jobEntry.getFileName() != null) {
            this.fileName.setText(jobEntry.getFileName());
        }
        if (jobEntry.getDebugLevel() != null) {
            this.debugLevel.setText(jobEntry.getDebugLevel());
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
    	String path = this.fileName.getText();
    	boolean lastIsSlash = false;
    	if(path.lastIndexOf("\\") == (path.length()-1)){
    		//has last \
    		lastIsSlash = true;
    	}
    	if(!lastIsSlash && path.lastIndexOf("/") == (path.length()-1)){
    		lastIsSlash = true;
    	}
    	if(lastIsSlash){
    		//remove the slash
    		String noSlashPath = path.substring(0,path.length()-1);
    		this.fileName.setText(noSlashPath);
    	}
    	
    	
    }
    
    private boolean validate(){
    	boolean isValid = true;
    	String errors = "";
    	
    	if(this.jobEntryName.getText().equals("")){
    		//one is required.
    		isValid = false;
    		errors += "You must provide a \"Job Entry Name\"!\r\n";
    	}
    	//recordset
    	if(this.fileName.getText().equals("")){
    		//one is required.
    		isValid = false;
    		errors += "You must provide a \"Output File(s) Directory\"!\r\n";
    	}else{
	    	boolean pathExists = (new File(this.fileName.getText())).exists();
	    	if(!pathExists){
	    		isValid = false;
	    		errors += "The \"Output File(s) Directory\" can't be found!\r\n";
	    	}
    	}
    	
    	// if skey require minskew ------ since its just a blank not needed yet will be needed in future version TODO:
    	
		if(!isValid){
			ErrorNotices en = new ErrorNotices();
			errors += "\r\n";
			errors += "If you continue to save with errors you may encounter compile errors if you try to execute the job.\r\n\r\n";
			isValid = en.openValidateDialog(getParent(),errors);
		}
		return isValid;
		
	}
    private void ok() {
    	updatePaths();
    	if(!validate()){
    		return;
    	}
        jobEntry.setName(jobEntryName.getText());
        
        AutoPopulate ap = new AutoPopulate();
        String serverHost = "";
        String serverPort = "";
            try{
            //Object[] jec = this.jobMeta.getJobCopies().toArray();
                
                serverHost = ap.getGlobalVariable(this.jobMeta.getJobCopies(),"server_ip");
                serverPort = ap.getGlobalVariable(this.jobMeta.getJobCopies(),"server_port");
                
            }catch (Exception e){
                System.out.println("Error Parsing existing Global Variables ");
                System.out.println(e.toString());
                
            }
            
        jobEntry.setServerAddress(serverHost);
        jobEntry.setServerPort(serverPort);
        
        jobEntry.setFileName(this.fileName.getText());
        jobEntry.setDebugLevel(this.debugLevel.getText());
        
        dispose();
    }

    private void cancel() {
        jobEntry.setChanged(backupChanged);
        jobEntry = null;
        dispose();
    }

    public void dispose() {
        WindowProperty winprop = new WindowProperty(shell);
        props.setScreen(winprop);
        shell.dispose();
    }
}
