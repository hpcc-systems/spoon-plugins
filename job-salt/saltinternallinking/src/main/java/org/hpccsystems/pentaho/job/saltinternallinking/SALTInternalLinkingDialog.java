/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.saltinternallinking;

import java.io.File;
import java.util.Iterator;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.layout.FillLayout;
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
import org.eclipse.swt.widgets.TabFolder;
import org.eclipse.swt.widgets.TabItem;
import org.eclipse.swt.widgets.Table;
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

import org.hpccsystems.eclguifeatures.AutoPopulate;
import org.hpccsystems.eclguifeatures.ErrorNotices;
import org.hpccsystems.recordlayout.CreateTable;
import org.hpccsystems.recordlayout.RecordBO;
import org.hpccsystems.recordlayout.RecordList;
import org.hpccsystems.ecljobentrybase.*;
/**
 *
 * @author ChambersJ
 */
public class SALTInternalLinkingDialog extends ECLJobEntryDialog{//extends JobEntryDialog implements JobEntryDialogInterface {

    private SALTInternalLinking jobEntry;
    private Text jobEntryName;
    private Text hygieneOutputFolder;
    private Text specificitiesOutputFolder;
    
    private Combo doAgain;
    private Text iteration;
    
    private Button wOK, wCancel,hygieneOpenButton,specificitiesOpenButton;
    private boolean backupChanged;
    private SelectionAdapter lsDef;
    



    public SALTInternalLinkingDialog(Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta) {
        super(parent, jobEntryInt, rep, jobMeta);
        jobEntry = (SALTInternalLinking) jobEntryInt;
        if (this.jobEntry.getName() == null) {
            this.jobEntry.setName("SaltDataProfling");
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
        shell.setText("SALt Internal Linking");

        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;


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
        fileGroupFormat.height = 200;
        fileGroupFormat.left = new FormAttachment(middle, 0);
        fileGroup.setLayoutData(fileGroupFormat);
        
        
        // private Text hygieneOutputFolder;
        // private Text specificitiesOutputFolder;
        //hygieneOpenButton,specificitiesOpenButton
        
        this.specificitiesOutputFolder = buildText("Specificities Output Directory", null, lsMod, middle, margin, fileGroup);
        controls.put("specificitiesOutputFolder", specificitiesOutputFolder);
        
        this.specificitiesOpenButton = buildButton("Choose Location", specificitiesOutputFolder, lsMod, middle, margin, fileGroup);
        controls.put("fOpen", specificitiesOpenButton);
        
        Listener specificitiesOpenListener = new Listener() {

            public void handleEvent(Event e) {
                String newFile = buildDirectoryDialog();
                if(newFile != ""){
                	specificitiesOutputFolder.setText(newFile);
                }
            }
        };
        this.specificitiesOpenButton.addListener(SWT.Selection, specificitiesOpenListener);
        
        
        this.hygieneOutputFolder = buildText("Hygiene Output Directory", specificitiesOpenButton, lsMod, middle, margin, fileGroup);
        controls.put("hygieneOutputFolder", hygieneOutputFolder);
        
        this.hygieneOpenButton = buildButton("Choose Location", hygieneOutputFolder, lsMod, middle, margin, fileGroup);
        controls.put("fOpen", hygieneOpenButton);
        
        Listener hygieneOpenListener = new Listener() {

            public void handleEvent(Event e) {
                String newFile = buildDirectoryDialog();
                if(newFile != ""){
                	hygieneOutputFolder.setText(newFile);
                }
            }
        };
        this.hygieneOpenButton.addListener(SWT.Selection, hygieneOpenListener);
        
        this.iteration = buildText("Iteration", hygieneOpenButton, lsMod, middle, margin, fileGroup);
        //controls.put("iteration", iteration);
        this.doAgain = buildCombo("Repeate Iteration", iteration, lsMod, middle, margin, fileGroup, new String[]{"no", "yes"});
        
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
        
        if (jobEntry.getHygieneOutputFolder() != null) {
            this.hygieneOutputFolder.setText(jobEntry.getHygieneOutputFolder());
        }
        if (jobEntry.getSpecificitiesOutputFolder() != null) {
            this.specificitiesOutputFolder.setText(jobEntry.getSpecificitiesOutputFolder());
        }
        if (jobEntry.getIteration() != null) {
            this.iteration.setText(jobEntry.getIteration());
        }

        if (jobEntry.getDoAgain() != null) {
            this.doAgain.setText(jobEntry.getDoAgain());
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

    private boolean validate(){
    	boolean isValid = true;
    	String errors = "";
    	
    	
    	
    	if(this.jobEntryName.getText().equals("")){
    		isValid = false;
    		errors += "\"Job Entry Name\" is a required field!\r\n";
    	}
    	if(specificitiesOutputFolder.getText().equals("")){
    		isValid = false;
    		errors += "Specificities Output Folder is required!\r\n";
    	}else{
    		if(!(new File(specificitiesOutputFolder.getText())).exists()){
    			isValid = false;
    			errors += "Specificities Folder Not found!\r\n";
    		}
    	}
    	if(!hygieneOutputFolder.getText().equals("")){
	    	Boolean exists = (new File(hygieneOutputFolder.getText())).exists();
			if(!exists){
				isValid = false;
				errors += "The \"Path to ML Library\" could not be located\r\n";
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
    private void updatePaths(){
    	String specPath = specificitiesOutputFolder.getText();
    	
    	boolean specLast = false;
    	if(specPath.lastIndexOf("\\") == (specPath.length()-1)){
    		//has last \
    		specLast = true;
    	}
    	if(!specLast && specPath.lastIndexOf("/") == (specPath.length()-1)){
    		specLast = true;
    	}
    	if(!specLast){
    		String slash = "/";
    		if(specPath.contains("\\")){
    			slash = "\\";
    		}
    		specificitiesOutputFolder.setText(specificitiesOutputFolder.getText() + slash);
    	}
    	
    	//hygieneOutputFolder
    	if(!hygieneOutputFolder.getText().equals("")){
	    	String hPath = hygieneOutputFolder.getText();
	    	boolean hLast = false;
	    	if(hPath.lastIndexOf("\\") == (hPath.length()-1)){
	    		//has last \
	    		hLast = true;
	    	}
	    	if(!hLast && hPath.lastIndexOf("/") == (hPath.length()-1)){
	    		hLast = true;
	    	}
	    	if(!hLast){
	    		String slash = "/";
	    		if(hPath.contains("\\")){
	    			slash = "\\";
	    		}
	    		hygieneOutputFolder.setText(hygieneOutputFolder.getText() + slash);
	    	}
    	}
    }
    private void ok() {
    	this.updatePaths();
    	if(!validate()){
    		return;
    	}
    	
        jobEntry.setName(jobEntryName.getText());
        jobEntry.setHygieneOutputFolder(hygieneOutputFolder.getText());
        jobEntry.setSpecificitiesOutputFolder(specificitiesOutputFolder.getText());

        jobEntry.setDoAgain(doAgain.getText());
        jobEntry.setIteration(iteration.getText());
        dispose();
    }

    private void cancel() {
        jobEntry.setChanged(backupChanged);
        jobEntry = null;
        dispose();
    }

}
