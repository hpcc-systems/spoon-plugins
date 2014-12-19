/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclpvalue;

import java.util.ArrayList;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TabFolder;
import org.eclipse.swt.widgets.TabItem;
import org.eclipse.swt.widgets.Text;
import org.hpccsystems.eclguifeatures.AutoPopulate;
import org.hpccsystems.eclguifeatures.ErrorNotices;
import org.hpccsystems.ecljobentrybase.ECLJobEntryDialog;
import org.pentaho.di.core.Const;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.job.dialog.JobDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
/**
 *
 *
 *
 * @author KeshavS
 */
public class ECLPValueDialog extends ECLJobEntryDialog{//extends JobEntryDialog implements JobEntryDialogInterface {
	
	public static final String NAME = "Name";
	public static final String INDEX = "Index";
	private ECLPValue jobEntry;
    private Text jobEntryName;
    private Text xvalue;
    private Text Output;
    private Combo datasetName;
    private Combo Column;
    private Button wOK, wCancel;
    private boolean backupChanged;
    String DSSelected = null;
    private String[] columns = new String[]{};
    @SuppressWarnings("unused")
	private SelectionAdapter lsDef;
    java.util.List fields;
    private int depends;
    
    public ECLPValueDialog(Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta) {
        super(parent, jobEntryInt, rep, jobMeta);
        jobEntry = (ECLPValue) jobEntryInt;
        if (this.jobEntry.getName() == null) {
            this.jobEntry.setName("PValue");
        }
    }

    public JobEntryInterface open() {
        Shell parentShell = getParent();
        final Display display = parentShell.getDisplay();
        
        String datasets[] = null;
        

        final AutoPopulate ap = new AutoPopulate();
        try{
            datasets = ap.parseDatasetsRecordsets(this.jobMeta.getJobCopies());
            
            
        }catch (Exception e){
            System.out.println("Error Parsing existing Datasets");
            System.out.println(e.toString());
            datasets = new String[]{""};
        }
        
        
        shell = new Shell(parentShell, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
        fields = new ArrayList();
        TabFolder tab = new TabFolder(shell, SWT.FILL | SWT.RESIZE | SWT.MIN | SWT.MAX);
        FormData datatab = new FormData();
        
        datatab.height = 420;
        datatab.width = 600;
        tab.setLayoutData(datatab);
        
        Composite compForGrp = new Composite(tab, SWT.NONE);
        //compForGrp.setLayout(new FillLayout(SWT.VERTICAL));
        compForGrp.setBackground(new Color(tab.getDisplay(),255,255,255));
        compForGrp.setLayout(new FormLayout());
        TabItem item1 = new TabItem(tab, SWT.NULL);
        
        item1.setText ("General");
        props.setLook(shell);
        JobDialog.setShellImage(shell, jobEntry);

        ModifyListener lsMod = new ModifyListener() {

            public void modifyText(ModifyEvent e) {
                jobEntry.setChanged();
            }
        };
        backupChanged = jobEntry.hasChanged();
        
        FormLayout layout = new FormLayout();
		layout.marginWidth = Const.FORM_MARGIN;
		layout.marginHeight = Const.FORM_MARGIN;
		
		int middle = props.getMiddlePct();
        int margin = Const.MARGIN;
        
		shell.setLayout(layout);
		shell.setText("PValue");
		
		FormLayout groupLayout = new FormLayout();
        groupLayout.marginWidth = 10;
        groupLayout.marginHeight = 10;


        // Stepname line
        Group generalGroup = new Group(compForGrp, SWT.SHADOW_NONE);
        props.setLook(generalGroup);
        generalGroup.setText("General Details");
        generalGroup.setLayout(groupLayout);
        FormData generalGroupFormat = new FormData();
        generalGroupFormat.top = new FormAttachment(0, margin);
        generalGroupFormat.width = 340;
        generalGroupFormat.height = 65;
        generalGroupFormat.left = new FormAttachment(middle, 0);
        generalGroupFormat.right = new FormAttachment(100, 0);
        generalGroup.setLayoutData(generalGroupFormat);
		
		jobEntryName = buildText("Job Entry Name :", null, lsMod, middle, margin, generalGroup);
		
        Group fieldsGroup = new Group(compForGrp, SWT.SHADOW_NONE);
        props.setLook(fieldsGroup);
        fieldsGroup.setText("Details");
        fieldsGroup.setLayout(groupLayout); 
        FormData fieldsGroupFormat = new FormData();
        fieldsGroupFormat.top = new FormAttachment(generalGroup, margin);
        fieldsGroupFormat.width = 340;
        fieldsGroupFormat.height = 250;
        fieldsGroupFormat.left = new FormAttachment(middle, 0);
        fieldsGroupFormat.right = new FormAttachment(100, 0);
        fieldsGroup.setLayoutData(fieldsGroupFormat);
        
        datasetName = buildCombo("Dataset Name:", jobEntryName, lsMod, middle, margin, fieldsGroup, datasets);
        
        
        Column = buildCombo("Column :", datasetName, lsMod, middle, margin, fieldsGroup, columns);
        
       
        
        
        //ap.getType(jobs, datasetValue)
        xvalue = buildText("X-value :", Column, lsMod, middle, margin, fieldsGroup);
        xvalue.setToolTipText("Set an integer or real bound");  
        Output = buildText("Output Name :", xvalue, lsMod, middle, margin, fieldsGroup);    
       
        
		item1.setControl(compForGrp);	   


        
        wOK = new Button(shell, SWT.PUSH);
        wOK.setText("OK");
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText("Cancel");

        BaseStepDialog.positionBottomButtons(shell, new Button[]{wOK, wCancel}, margin, tab);
        
        
        // Add listeners
        
       datasetName.addModifyListener(new ModifyListener() {
		
		@Override
		public void modifyText(ModifyEvent arg0) {
			Column.setItems(new String[0]);
			AutoPopulate ap = new AutoPopulate();
			String[] columns = new String[1];
			try {
				columns = ap.fieldsByDataset(datasetName.getText(), jobMeta.getJobCopies());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			Column.setItems(columns); 
		}
	});
          
        
        
        
        Listener cancelListener = new Listener() {

            public void handleEvent(Event e) {
                cancel();
            }
        };
        Listener okListener = new Listener() {

            public void handleEvent(Event e) {
            	/* to be implemented
            			
            			*
            			*
            			*
            			*
            			*
            			*/
            	               			
            	
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
        
               
        if (jobEntry.getDatasetName() != null) {
            datasetName.setText(jobEntry.getDatasetName());
        }
      
        if (jobEntry.getOutput() != null) {
            Output.setText(jobEntry.getOutput());
        }
        
        if (jobEntry.getField() != null) {
            Column.setText(jobEntry.getField());
        }
        
               
        if (jobEntry.getValueOfX() != null) {
            xvalue.setText(jobEntry.getValueOfX());
        }
      
            
     /*   if(jobEntry.getFields() != null){
        	fields = jobEntry.getFields();
        	tv.setInput(fields);        	
        }
                */
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
    	
    	
    	if(this.datasetName.getText().equals("")){
    		isValid = false;
    		errors += "\"Dataset Name\" is a required field!\r\n";
    	}
    	
    	if(this.Output.getText().equals("")){
    		isValid = false;
    		errors += "\"Output Name\" is a required field!\r\n";
    	}
    	
    	if(this.xvalue.getText().equals("")){
    		isValid = false;
    		errors += "\"Value of X\" is a required field!\r\n";
    	}
    	
    	if(this.Column.getText().equals("")){
    		isValid = false;
    		errors += "\"Value of X\" is a required field!\r\n";
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
    	
        jobEntry.setName(jobEntryName.getText());          
        jobEntry.setDatasetName(datasetName.getText());
        jobEntry.setOutput(Output.getText());          
        jobEntry.setValueOfX(xvalue.getText());
        jobEntry.setFields(fields);
        jobEntry.setField(Column.getText());
        
              
        dispose();
    }

    private void cancel() {
        jobEntry.setChanged(backupChanged);
        jobEntry = null;
        dispose();
    }

}