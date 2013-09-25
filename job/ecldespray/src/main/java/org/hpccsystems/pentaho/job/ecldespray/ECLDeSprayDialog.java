package org.hpccsystems.pentaho.job.ecldespray;

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

public class ECLDeSprayDialog extends ECLJobEntryDialog{//extends JobEntryDialog implements JobEntryDialogInterface{

	private ECLDeSpray jobEntry;
	
	private Text jobEntryName;
	
	//private Text recordsetName;
	//private Combo recordset;
	
	private Text logicalName;
	private Text destinationIP;
	private Text destinationPath;
	private Text timeout;
	private Text espserverISPport;
	private Text maxConnections;
	private Combo allowOverwrite;
	
	private Button wOK, wCancel;
	private boolean backupChanged;
	private SelectionAdapter lsDef;
	
	public ECLDeSprayDialog(Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta) {
        super(parent, jobEntryInt, rep, jobMeta);
        jobEntry = (ECLDeSpray) jobEntryInt;
        if (this.jobEntry.getName() == null) {
            this.jobEntry.setName("DeSpray");
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
        
        /*
        String datasets[] = null;
        AutoPopulate ap = new AutoPopulate();
        try{

            datasets = ap.parseDatasets(this.jobMeta.getJobCopies());
            
        } catch (Exception e){
            System.out.println("Error Parsing existing Datasets");
            System.out.println(e.toString());
            datasets = new String[]{""};
        }
        */
        

        backupChanged = jobEntry.hasChanged();

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;


        shell.setLayout(formLayout);
        shell.setText("DeSpray");

        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;

        shell.setLayout(formLayout);
        shell.setText("Define an ECL DeSpray");

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
        Group desprayGroup = new Group(shell, SWT.SHADOW_NONE);
        props.setLook(desprayGroup);
        desprayGroup.setText("DeSpray Details");
        desprayGroup.setLayout(groupLayout);
        FormData datasetGroupFormat = new FormData();
        datasetGroupFormat.top = new FormAttachment(generalGroup, margin);
        datasetGroupFormat.width = 400;
        datasetGroupFormat.height = 300;
        datasetGroupFormat.left = new FormAttachment(middle, 0);
        desprayGroup.setLayoutData(datasetGroupFormat);



                    

        //recordsetName = buildText("Result Recordset", null, lsMod, middle, margin, desprayGroup);
        //recordset = buildCombo("Recordset", recordsetName, lsMod, middle, margin, desprayGroup,datasets);
        //logicalName = buildText("Logical Name", recordset, lsMod, middle, margin, desprayGroup);
        logicalName = buildText("Logical Name", null, lsMod, middle, margin, desprayGroup);
        destinationIP = buildText("Destination IP", logicalName, lsMod, middle, margin, desprayGroup);
        destinationPath = buildText("Destination Path/filename", destinationIP, lsMod, middle, margin, desprayGroup);
        //optional fields
        timeout = buildText("Timeout", destinationPath, lsMod, middle, margin, desprayGroup);
        espserverISPport = buildText("ESP Server ISP Port", timeout, lsMod, middle, margin, desprayGroup);
        //shoudl put a lable here with
        //protocal://IP:port/path
        Label espHelp = this.buildLabel("protocal://IP:port/path", espserverISPport, lsMod, middle, margin, desprayGroup);
        maxConnections = buildText("Max Connections", espHelp, lsMod, middle, margin, desprayGroup);
        allowOverwrite = buildCombo("Allow Overwrite", maxConnections, lsMod, middle, margin, desprayGroup,new String[]{"false", "true"});
     
        wOK = new Button(shell, SWT.PUSH);
        wOK.setText("OK");
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText("Cancel");

        BaseStepDialog.positionBottomButtons(shell, new Button[]{wOK, wCancel}, margin, desprayGroup);

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
        
        if (jobEntry.getLogicalName() != null) {
            logicalName.setText(jobEntry.getLogicalName());
        }
        
        if (jobEntry.getDestinationIP() != null) {
            destinationIP.setText(jobEntry.getDestinationIP());
        }
        
        
         if (jobEntry.getDestinationPath() != null) {
            destinationPath.setText(jobEntry.getDestinationPath());
        }
          if (jobEntry.getTimeout() != null) {
            timeout.setText(jobEntry.getTimeout());
        } else {
        	timeout.setText("-1");
        }
          

        
        if (jobEntry.getEspserverISPport() != null) {
            espserverISPport.setText(jobEntry.getEspserverISPport());
        }
       
        if (jobEntry.getMaxConnections() != null) {
            maxConnections.setText(jobEntry.getMaxConnections());
        } else {
        	maxConnections.setText("1");
        }
        
        if (jobEntry.isAllowOverwriteString() != null) {
            allowOverwrite.setText(jobEntry.isAllowOverwriteString());
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
    	//either recordset or valuelist is required but never both, if valuelist, not other allowed
    	
    	
    	if(this.jobEntryName.getText().equals("")){
    		//one is required.
    		isValid = false;
    		errors += "You must provide a \"Job Entry Name\"!\r\n";
    	}
    	//logicalname
    	if(this.logicalName.getText().equals("")){
    		//one is required.
    		isValid = false;
    		errors += "You must provide a \"Logical Name\"!\r\n";
    	}
    	//destinationIP
    	if(this.destinationIP.getText().equals("")){
    		//one is required.
    		isValid = false;
    		errors += "You must provide a \"Destination IP\"!\r\n";
    	}
    	//destinationpath
    	if(this.destinationPath.getText().equals("")){
    		//one is required.
    		isValid = false;
    		errors += "You must provide a \"Destination Path\"!\r\n";
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
        jobEntry.setLogicalName(logicalName.getText());
        jobEntry.setDestinationIP(destinationIP.getText());
        jobEntry.setDestinationPath(destinationPath.getText());
        jobEntry.setTimeout(timeout.getText());
        jobEntry.setEspserverISPport(espserverISPport.getText());
        jobEntry.setMaxConnections(maxConnections.getText());
        jobEntry.setAllowOverwriteString(allowOverwrite.getText());

        dispose();
    }

    private void cancel() {
        jobEntry.setChanged(backupChanged);
        jobEntry = null;
        dispose();
    }

}
