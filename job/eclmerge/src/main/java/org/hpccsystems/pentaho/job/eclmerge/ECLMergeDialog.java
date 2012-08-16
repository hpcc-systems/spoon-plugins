package org.hpccsystems.pentaho.job.eclmerge;

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
import org.hpccsystems.eclguifeatures.AutoPopulate;
import org.hpccsystems.eclguifeatures.ErrorNotices;
import org.hpccsystems.pentaho.job.eclmerge.ECLMerge;
import org.pentaho.di.core.Const;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryDialogInterface;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.job.dialog.JobDialog;
import org.pentaho.di.ui.job.entry.JobEntryDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.hpccsystems.ecljobentrybase.*;

public class ECLMergeDialog extends ECLJobEntryDialog{//extends JobEntryDialog implements JobEntryDialogInterface {

	private ECLMerge jobEntry;
	private Text jobEntryName;
	
	private Text recordsetName;
	private Combo recordsetSet;
	private Text recordsetList;
	private Text fieldList;
	private Combo dedup;
	private Combo runLocal;
	
	
	private Button wOK, wCancel;
	private boolean backupChanged;
	private SelectionAdapter lsDef;
	
	public ECLMergeDialog(Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta) {
        super(parent, jobEntryInt, rep, jobMeta);
        jobEntry = (ECLMerge) jobEntryInt;
        if (this.jobEntry.getName() == null) {
            this.jobEntry.setName("Merge");
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
        	
            datasets = ap.parseDatasetsRecordsets(this.jobMeta.getJobCopies());
            
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
		shell.setText("Merge");
		
		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;
		
		shell.setLayout(formLayout);
        shell.setText("Define an ECL Merge");

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
        generalGroupFormat.height = 100;
        generalGroupFormat.left = new FormAttachment(middle, 0);
        generalGroup.setLayoutData(generalGroupFormat);
        
        jobEntryName = buildText("Job Entry Name", null, lsMod, middle, margin, generalGroup);

        //All other contols
        Group mergeGroup = new Group(shell, SWT.SHADOW_NONE);
        props.setLook(mergeGroup);
        mergeGroup.setText("Merge Details");
        mergeGroup.setLayout(groupLayout);
        FormData mergeGroupFormat = new FormData();
        mergeGroupFormat.top = new FormAttachment(generalGroup, margin);
        mergeGroupFormat.width = 400;
        mergeGroupFormat.height = 300;
        mergeGroupFormat.left = new FormAttachment(middle, 0);
        mergeGroup.setLayoutData(mergeGroupFormat);
        
        
        recordsetName = buildText("Result Recordset", null, lsMod, middle, margin, mergeGroup);
        this.recordsetSet = buildCombo("Left Recordset Name", recordsetName, lsMod, middle, margin, mergeGroup,datasets);
        //recordsetSet = buildText("Recordset Set", recordsetName, lsMod, middle, margin, mergeGroup);
        recordsetList = buildText("Recordset List", recordsetSet, lsMod, middle, margin, mergeGroup);
       	fieldList = buildText("Field List", recordsetList, lsMod, middle, margin, mergeGroup);
       	dedup = buildCombo("DEDUP", fieldList, lsMod, middle, margin, mergeGroup, new String[] {"false","true"});
       	runLocal = buildCombo("LOCAL", dedup, lsMod, middle, margin, mergeGroup, new String[] {"false","true"});
        
        wOK = new Button(shell, SWT.PUSH);
        wOK.setText("OK");
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText("Cancel");
        
        
        BaseStepDialog.positionBottomButtons(shell, new Button[]{wOK, wCancel}, margin, mergeGroup);

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
        if (jobEntry.getRecordsetName() != null) {
        	recordsetName.setText(jobEntry.getRecordsetName());
        }
        if (jobEntry.getRecordsetSet() != null) {
            recordsetSet.setText(jobEntry.getRecordsetSet());
        }
         if (jobEntry.getRecordsetList() != null) {
            recordsetList.setText(jobEntry.getRecordsetList());
        }
        if (jobEntry.getFieldList() != null) {
            fieldList.setText(jobEntry.getFieldList());
        }
        if (jobEntry.isDedupString() != null) {
            dedup.setText(jobEntry.isDedupString());
        }
        if (jobEntry.runLocalString() != null) {
            runLocal.setText(jobEntry.runLocalString());
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
    	
    	//only need to require a entry name
    	if(this.jobEntryName.getText().equals("")){
    		//one is required.
    		isValid = false;
    		errors += "You must provide a \"Job Entry Name\"!\r\n";
    	}
    	
    	if(this.recordsetName.getText().equals("")){
    		//one is required.
    		isValid = false;
    		errors += "You must provide a \"Result Recordset\"!\r\n";
    	}
    	
    	//recordsetlist,fieldlist
    	
    	//recordsetset,fieldlist,sortedfl
    	
    	//TODO:  add required fields here once this is updated

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
        
        jobEntry.setRecordsetName(recordsetName.getText());
        jobEntry.setRecordsetSet(recordsetSet.getText());
        jobEntry.setRecordsetList(recordsetList.getText());
        jobEntry.setFieldList(fieldList.getText());
        jobEntry.setDedupString(dedup.getText());
        jobEntry.setRunLocalString(runLocal.getText());

        dispose();
    }

    private void cancel() {
        jobEntry.setChanged(backupChanged);
        jobEntry = null;
        dispose();
    }
}