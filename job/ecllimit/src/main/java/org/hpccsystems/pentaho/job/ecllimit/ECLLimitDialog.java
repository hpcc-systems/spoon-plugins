package org.hpccsystems.pentaho.job.ecllimit;

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
import org.hpccsystems.pentaho.job.ecllimit.ECLLimit;
import org.pentaho.di.core.Const;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryDialogInterface;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.job.dialog.JobDialog;
import org.pentaho.di.ui.job.entry.JobEntryDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.hpccsystems.eclguifeatures.ErrorNotices;
import org.hpccsystems.ecljobentrybase.*;

public class ECLLimitDialog extends ECLJobEntryDialog{//extends JobEntryDialog implements JobEntryDialogInterface {

	private ECLLimit jobEntry;
	private Text jobEntryName;
	
	private Text recordsetName;
	private Combo recordset;
	private Text maxRecs;
	private Text failClause;
	private Combo keyed;
	private Combo count;
	private Combo skip;
	private Text onFailTransform;
	
	
	private Button wOK, wCancel;
	private boolean backupChanged;
	private SelectionAdapter lsDef;
	
	public ECLLimitDialog(Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta) {
        super(parent, jobEntryInt, rep, jobMeta);
        jobEntry = (ECLLimit) jobEntryInt;
        if (this.jobEntry.getName() == null) {
            this.jobEntry.setName("Limit");
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
		shell.setText("Limit");
		
		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;
		
		shell.setLayout(formLayout);
        shell.setText("Define an ECL Limit");

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
        Group limitGroup = new Group(shell, SWT.SHADOW_NONE);
        props.setLook(limitGroup);
        limitGroup.setText("Limit Details");
        limitGroup.setLayout(groupLayout);
        FormData limitGroupFormat = new FormData();
        limitGroupFormat.top = new FormAttachment(generalGroup, margin);
        limitGroupFormat.width = 400;
        limitGroupFormat.height = 300;
        limitGroupFormat.left = new FormAttachment(middle, 0);
        limitGroup.setLayoutData(limitGroupFormat);
        
        recordsetName = buildText("Result Recordset", null, lsMod, middle, margin, limitGroup);
        recordset = buildCombo("Recordset", recordsetName, lsMod, middle, margin, limitGroup, datasets);
        maxRecs = buildText("Maximum Records", recordset, lsMod, middle, margin, limitGroup);
       	failClause = buildText("FAIL(<clause>)", maxRecs, lsMod, middle, margin, limitGroup);
       	keyed = buildCombo("KEYED", failClause, lsMod, middle, margin, limitGroup, new String[] {"false","true"});
       	count = buildCombo("COUNT", keyed, lsMod, middle, margin, limitGroup, new String[] {"false","true"});
       	skip = buildCombo("SKIP", count, lsMod, middle, margin, limitGroup, new String[] {"false","true"});
       	onFailTransform = buildText("ONFAIL(<transform>)", skip, lsMod, middle, margin, limitGroup);
        
        wOK = new Button(shell, SWT.PUSH);
        wOK.setText("OK");
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText("Cancel");
        
        
        BaseStepDialog.positionBottomButtons(shell, new Button[]{wOK, wCancel}, margin, limitGroup);

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
        if (jobEntry.getRecordset() != null) {
            recordset.setText(jobEntry.getRecordset());
        }
         if (jobEntry.getMaxRecs() != null) {
            maxRecs.setText(jobEntry.getMaxRecs());
        }
        if (jobEntry.getFailClause() != null) {
            failClause.setText(jobEntry.getFailClause());
        }
        if (jobEntry.getKeyedString() != null) {
            keyed.setText(jobEntry.getKeyedString());
        }
        if (jobEntry.getCountString() != null) {
            count.setText(jobEntry.getCountString());
        }
        if (jobEntry.getSkipString() != null) {
            skip.setText(jobEntry.getSkipString());
        }
        if (jobEntry.getOnFailTransform() != null) {
            onFailTransform.setText(jobEntry.getOnFailTransform());
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
    	//recset,maxrecs
    	
    	if(this.recordsetName.getText().equals("")){
    		//one is required.
    		isValid = false;
    		errors += "You must provide a \"Result Recordset\"!\r\n";
    	}
    	
    	if(this.recordset.getText().equals("")){
    		//one is required.
    		isValid = false;
    		errors += "You must provide a \"Recordset\"!\r\n";
    	}
    	
    	if(this.maxRecs.getText().equals("")){
    		//one is required.
    		isValid = false;
    		errors += "You must provide a value for \"Maximum Records\"!\r\n";
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
        
        jobEntry.setRecordsetName(recordsetName.getText());
        jobEntry.setRecordset(recordset.getText());
        jobEntry.setMaxRecs(maxRecs.getText());
        jobEntry.setFailClause(failClause.getText());
        jobEntry.setKeyedString(keyed.getText());
        jobEntry.setCountString(count.getText());
        jobEntry.setSkipString(skip.getText());
        jobEntry.setOnFailTransform(onFailTransform.getText());

        dispose();
    }

    private void cancel() {
        jobEntry.setChanged(backupChanged);
        jobEntry = null;
        dispose();
    }
}
