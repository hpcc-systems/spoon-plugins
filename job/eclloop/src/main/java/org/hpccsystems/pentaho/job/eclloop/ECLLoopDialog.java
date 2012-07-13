package org.hpccsystems.pentaho.job.eclloop;

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
import org.hpccsystems.pentaho.job.eclloop.ECLLoop;
import org.pentaho.di.core.Const;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryDialogInterface;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.job.dialog.JobDialog;
import org.pentaho.di.ui.job.entry.JobEntryDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public class ECLLoopDialog extends JobEntryDialog implements JobEntryDialogInterface {

	private ECLLoop jobEntry;
	private Text jobEntryName;
	
	private Text recordsetName;
	private Combo recordset;
	
	private Text loopCount;
	private Text loopBody;
	private Text iterations;
	private Text iterationList;
	private Text dfault;
	private Text loopFilter;
	private Text loopCondition;
	private Text rowFilter;
	
	private Button wOK, wCancel;
	private boolean backupChanged;
	private SelectionAdapter lsDef;
	
	public ECLLoopDialog(Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta) {
        super(parent, jobEntryInt, rep, jobMeta);
        jobEntry = (ECLLoop) jobEntryInt;
        if (this.jobEntry.getName() == null) {
            this.jobEntry.setName("Loop");
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
		shell.setText("Loop");
		
		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;
		
		shell.setLayout(formLayout);
        shell.setText("Define an ECL Loop");

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
        Group loopGroup = new Group(shell, SWT.SHADOW_NONE);
        props.setLook(loopGroup);
        loopGroup.setText("Distribute Details");
        loopGroup.setLayout(groupLayout);
        FormData loopGroupFormat = new FormData();
        loopGroupFormat.top = new FormAttachment(generalGroup, margin);
        loopGroupFormat.width = 400;
        loopGroupFormat.height = 200;
        loopGroupFormat.left = new FormAttachment(middle, 0);
        loopGroup.setLayoutData(loopGroupFormat);
        
        recordsetName = buildText("Result Recordset", null, lsMod, middle, margin, loopGroup);
        recordset = buildCombo("Recordset", recordsetName, lsMod, middle, margin, loopGroup, datasets);
        loopCount = buildText("Loop Count", recordset, lsMod, middle, margin, loopGroup);
        loopBody = buildText("Loop Body", loopCount, lsMod, middle, margin, loopGroup);
        loopFilter = buildText("Loop filter", dfault, lsMod, middle, margin, loopGroup);
        loopCondition = buildText("Loop Condition", loopFilter, lsMod, middle, margin, loopGroup);
        rowFilter = buildText("Row Filter", loopCondition, lsMod, middle, margin, loopGroup);
        
        //Run in PARALLEL?
        Group parallelGroup = new Group(shell, SWT.SHADOW_NONE);
        props.setLook(parallelGroup);
        parallelGroup.setText("PARALLEL");
        parallelGroup.setLayout(groupLayout);
        FormData parallelGroupFormat = new FormData();
        parallelGroupFormat.top = new FormAttachment(loopGroup, margin);
        parallelGroupFormat.width = 400;
        parallelGroupFormat.height = 150;
        parallelGroupFormat.left = new FormAttachment(middle, 0);
        parallelGroup.setLayoutData(parallelGroupFormat);
        
        iterations = buildText("Iterations", null, lsMod, middle, margin, parallelGroup);
        iterationList = buildText("Iteration List", iterations, lsMod, middle, margin, parallelGroup);
        dfault = buildText("Default", iterationList, lsMod, middle, margin, parallelGroup);
        
        wOK = new Button(shell, SWT.PUSH);
        wOK.setText("OK");
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText("Cancel");
        
        
        BaseStepDialog.positionBottomButtons(shell, new Button[]{wOK, wCancel}, margin, parallelGroup);

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
        
         if (jobEntry.getLoopCount() != null) {
            loopCount.setText(jobEntry.getLoopCount());
        }
         
        if (jobEntry.getLoopBody() != null) {
            loopBody.setText(jobEntry.getLoopBody());
        }
        if (jobEntry.getIterations() != null) {
            iterations.setText(jobEntry.getIterations());
        }
        if (jobEntry.getIterationList() != null) {
            iterationList.setText(jobEntry.getIterationList());
        }
        if (jobEntry.getDefault() != null) {
            dfault.setText(jobEntry.getDefault());
        }
        if (jobEntry.getLoopFilter() != null) {
            loopFilter.setText(jobEntry.getLoopFilter());
        }
        if (jobEntry.getLoopCondition() != null) {
            loopCondition.setText(jobEntry.getLoopCondition());
        }
        if (jobEntry.getRowFilter() != null) {
            rowFilter.setText(jobEntry.getRowFilter());
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
	
	private Text buildText(String strLabel, Control prevControl,
            ModifyListener lsMod, int middle, int margin, Composite groupBox) {
        // label
        Label fmt = new Label(groupBox, SWT.RIGHT);
        fmt.setText(strLabel);
        props.setLook(fmt);
        FormData labelFormat = new FormData();
        labelFormat.left = new FormAttachment(0, 0);
        labelFormat.top = new FormAttachment(prevControl, margin);
        labelFormat.right = new FormAttachment(middle, -margin);
        fmt.setLayoutData(labelFormat);

        // text field
        Text text = new Text(groupBox, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(text);
        text.addModifyListener(lsMod);
        FormData fieldFormat = new FormData();
        fieldFormat.left = new FormAttachment(middle, 0);
        fieldFormat.top = new FormAttachment(prevControl, margin);
        fieldFormat.right = new FormAttachment(100, 0);
        text.setLayoutData(fieldFormat);

        return text;
    }
	
	private Text buildMultiText(String strLabel, Control prevControl,
            ModifyListener lsMod, int middle, int margin, Composite groupBox) {
        // label
        Label fmt = new Label(groupBox, SWT.RIGHT);
        fmt.setText(strLabel);
        props.setLook(fmt);
        FormData labelFormat = new FormData();
        labelFormat.left = new FormAttachment(0, 0);
        labelFormat.top = new FormAttachment(prevControl, margin);
        labelFormat.right = new FormAttachment(middle, -margin);
        fmt.setLayoutData(labelFormat);

        // text field
        Text text = new Text(groupBox, SWT.MULTI | SWT.LEFT | SWT.BORDER);
        props.setLook(text);
        text.addModifyListener(lsMod);
        FormData fieldFormat = new FormData();
        fieldFormat.left = new FormAttachment(middle, 0);
        fieldFormat.top = new FormAttachment(prevControl, margin);
        fieldFormat.right = new FormAttachment(100, 0);
        fieldFormat.height = 50;
        text.setLayoutData(fieldFormat);

        return text;
    }

    private Combo buildCombo(String strLabel, Control prevControl,
            ModifyListener lsMod, int middle, int margin, Composite groupBox, String[] items) {
        // label
        Label fmt = new Label(groupBox, SWT.RIGHT);
        fmt.setText(strLabel);
        props.setLook(fmt);
        FormData labelFormat = new FormData();
        labelFormat.left = new FormAttachment(0, 0);
        labelFormat.top = new FormAttachment(prevControl, margin);
        labelFormat.right = new FormAttachment(middle, -margin);
        fmt.setLayoutData(labelFormat);

        // combo field
        Combo combo = new Combo(groupBox, SWT.MULTI | SWT.LEFT | SWT.BORDER);
        props.setLook(combo);
        combo.setItems(items);
        combo.addModifyListener(lsMod);
        FormData fieldFormat = new FormData();
        fieldFormat.left = new FormAttachment(middle, 0);
        fieldFormat.top = new FormAttachment(prevControl, margin);
        fieldFormat.right = new FormAttachment(100, 0);
        fieldFormat.height = 50;
        combo.setLayoutData(fieldFormat);

        return combo;
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
    	
    	//dataset
    	if(this.recordset.getText().equals("")){
    		//one is required.
    		isValid = false;
    		errors += "You must provide a \"Recordset\"!\r\n";
    	}
    	
    	//need to requrie one of loopcount,loopfilter,loopcondition
    	if(this.loopCount.getText().equals("") && this.loopFilter.getText().equals("") && this.loopCondition.getText().equals("")){
    		//they can't all be ""
    		isValid = false;
    		errors += "You must provide one of the following \"Loop Count\", \"Loop Filter\", \"Loop Condition\"!\r\n";
    	}
    	
    	//can't have booth loopcount and loopcondition
    	if(!this.loopCount.getText().equals("") && !this.loopCondition.getText().equals("")){
    		//can't have both
    		isValid = false;
    		errors += "You can not provide both a \"Loop Count\" and a \"Loop Condition\"!\r\n";
    	}
    	
    	
    	//loopcount, loopbody
    	//loopcount, loopfilter, loopbody
    	if(!this.loopCount.getText().equals("")){
    		//we have loop count require atleast loop body
    		if(this.loopBody.getText().equals("")){
        		//one is required.
        		isValid = false;
        		errors += "You must provide a \"Loop Body\" when providing a \"Loop Count\"!\r\n";
        	}
    		
    		
    	}else{
	    	
	    	//loopfilter, loopbody
	    	if(!this.loopFilter.getText().equals("")){
	    		//no loopcount but loop filter
	    		if(this.loopBody.getText().equals("")){
	        		//one is required.
	        		isValid = false;
	        		errors += "You must provide a \"Loop Body\" when providing a \"Loop Filter\"!\r\n";
	        	}
	    		//canht have iterations,iterationlist,default fields
	    		if(!this.iterations.getText().equals("")){
	    			isValid = false;
	        		errors += "You cannot set a \"Iterations\" when providing a \"Loop FIlter\" without a \"Loop Count\"!\r\n";
	    		}
	    		if(!this.iterationList.getText().equals("")){
	    			isValid = false;
	        		errors += "You cannot set a \"Iterations List\" when providing a \"Loop FIlter\" without a \"Loop Count\"!\r\n";
	    		}
	    		if(!this.dfault.getText().equals("")){
	    			isValid = false;
	        		errors += "You cannot set a \"Default\" when providing a \"Loop FIlter\" without a \"Loop Count\"!\r\n";
	    		}
	    	}
    	}
    	
    	//loopcondition, loopbody
    	if(!this.loopCount.getText().equals("")){
    		//can't have loopcount, loopfilter, iterations,iterationlist,default
    		
    		//require loopbody
    		if(this.loopBody.getText().equals("")){
        		//one is required.
        		isValid = false;
        		errors += "You must provide a \"Loop Body\" when providing a \"Loop Count\"!\r\n";
        	}
    	}
    	
    	//loopcondition, rowfilter, loopbody
    	
    	if(!this.loopCondition.getText().equals("")){
    		
    		if(this.loopBody.getText().equals("")){
        		//one is required.
        		isValid = false;
        		errors += "You must provide a \"Loop Body\" when providing a \"Loop Condition\"!\r\n";
        	}
    		
	    	if(!this.iterations.getText().equals("")){
				isValid = false;
	    		errors += "You cannot set a \"Iterations\" when providing a \"Loop Condition\"!\r\n";
			}
			if(!this.iterationList.getText().equals("")){
				isValid = false;
	    		errors += "You cannot set a \"Iterations List\" hen providing a \"Loop Condition\"!\r\n";
			}
			if(!this.dfault.getText().equals("")){
				isValid = false;
	    		errors += "You cannot set a \"Default\" hen providing a \"Loop Condition\"!\r\n";
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

        jobEntry.setName(jobEntryName.getText());

        jobEntry.setRecordsetName(recordsetName.getText());
        jobEntry.setRecordset(recordset.getText());
        jobEntry.setLoopCount(loopCount.getText());
        jobEntry.setLoopBody(loopBody.getText());
        jobEntry.setIterations(iterations.getText());
        jobEntry.setIterationList(iterationList.getText());
        jobEntry.setDefault(dfault.getText());
        jobEntry.setLoopFilter(loopFilter.getText());
        jobEntry.setLoopCondition(loopCondition.getText());
        jobEntry.setRowFilter(rowFilter.getText());

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
