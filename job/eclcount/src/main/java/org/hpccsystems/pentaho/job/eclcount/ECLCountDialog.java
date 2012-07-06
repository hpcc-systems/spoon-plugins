/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclcount;

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
import org.hpccsystems.pentaho.job.eclcount.ECLCount;
import org.pentaho.di.core.Const;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryDialogInterface;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.job.dialog.JobDialog;
import org.pentaho.di.ui.job.entry.JobEntryDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

/**
 *
 * @author SimmonsJA
 */
public class ECLCountDialog extends JobEntryDialog implements JobEntryDialogInterface {

	private ECLCount jobEntry;
	
	private Text jobEntryName;
	
	private Text recordsetName;
	private Combo recordset;
	private Text expression;
	private Text keyed;
	private Text valuelist;
	
	private Button wOK, wCancel;
	private boolean backupChanged;
	private SelectionAdapter lsDef;
	
	public ECLCountDialog(Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta) {
        super(parent, jobEntryInt, rep, jobMeta);
        jobEntry = (ECLCount) jobEntryInt;
        if (this.jobEntry.getName() == null)
            this.jobEntry.setName("Count");
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
		shell.setText("Count");
		
		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;
		
		shell.setLayout(formLayout);
        shell.setText("Define an ECL Count");

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
        //Distribute Declaration
        Group countGroup = new Group(shell, SWT.SHADOW_NONE);
        props.setLook(countGroup);
        countGroup.setText("Distribute Details");
        countGroup.setLayout(groupLayout);
        FormData datasetGroupFormat = new FormData();
        datasetGroupFormat.top = new FormAttachment(generalGroup, margin);
        datasetGroupFormat.width = 400;
        datasetGroupFormat.height = 300;
        datasetGroupFormat.left = new FormAttachment(middle, 0);
        countGroup.setLayoutData(datasetGroupFormat);
        
        
        recordsetName = buildText("Result Recordset", null, lsMod, middle, margin, countGroup);
        recordset = buildCombo("Recordset", recordsetName, lsMod, middle, margin, countGroup, datasets);
        expression = buildText("Expression", recordset, lsMod, middle, margin, countGroup);
        keyed = buildText("Keyed", expression, lsMod, middle, margin, countGroup);
        valuelist = buildText("Value List", keyed, lsMod, middle, margin, countGroup);
        
        wOK = new Button(shell, SWT.PUSH);
        wOK.setText("OK");
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText("Cancel");
        
        
        BaseStepDialog.positionBottomButtons(shell, new Button[]{wOK, wCancel}, margin, countGroup);

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
        
        if (jobEntry.getRecordSet() != null) {
            recordset.setText(jobEntry.getRecordSet());
        }
        
        if (jobEntry.getExpression() != null) {
            expression.setText(jobEntry.getExpression());
        }
        
         if (jobEntry.getKeyed() != null) {
            keyed.setText(jobEntry.getKeyed());
        }
         
        if (jobEntry.getValueList() != null) {
            valuelist.setText(jobEntry.getValueList());
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
    	//either recordset or valuelist is required but never both, if valuelist, not other allowed
    	
    	
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
    	
    	if(this.recordset.getText().equals("") && this.valuelist.getText().equals("")){
    		//one is required.
    		isValid = false;
    		errors += "You must provide either a \"Recordset\" or a \"Value List\"!\r\n";
    	}
    	
    	if(!this.recordset.getText().equals("") && !this.valuelist.getText().equals("")){
    		//one is required.
    		isValid = false;
    		errors += "You must provide either a \"Recordset\" or a \"Value List\" but not both!\r\n";
    	}else{
    		//ok assuming that one is set lets make sure they don't provide paraters to value list
    		if(!this.valuelist.getText().equals("")){
    			if(!this.expression.getText().equals("") ||
    			   !this.keyed.getText().equals("")){
    				isValid = false;
    	    		errors += "When using a \"Value List\" \"Expression\" and \"Keyed\" must be blank!\r\n";
    			}
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
        jobEntry.setRecordSet(recordset.getText());
        jobEntry.setExpression(expression.getText());
        jobEntry.setKeyed(keyed.getText());
        jobEntry.setValueList(valuelist.getText());

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
