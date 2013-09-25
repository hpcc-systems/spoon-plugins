package org.hpccsystems.pentaho.job.eclindex;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Color;
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
import org.eclipse.swt.widgets.Text;
import org.hpccsystems.recordlayout.CreateTable;
import org.hpccsystems.eclguifeatures.AutoPopulate;
import org.hpccsystems.eclguifeatures.ErrorNotices;
import org.hpccsystems.recordlayout.RecordBO;
import org.hpccsystems.recordlayout.RecordList;
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

/**
 *
 * @author ChambersJ
 */
public class ECLIndexDialog extends ECLJobEntryDialog{//extends JobEntryDialog implements JobEntryDialogInterface {
    
    // TODO these belong in org.hpccsystems.javaecl.Index (?)
    private static final String COMPRESSION_TYPE_FIRST = "FIRST";
    private static final String COMPRESSION_TYPE_ROW   = "ROW";
    private static final String COMPRESSION_TYPE_LZW   = "LZW";
    private static final String COMBO_ITEM_YES         = "Yes";
    private static final String COMBO_ITEM_NO          = "No";
    
    private TabFolder tabFolder;
    private ECLIndex jobEntry;    
    private Text jobEntryName;
    
    private Combo baserecset;
    private Text indexfile;
    private Combo sorted;
    private Combo preload;
    private Combo opt;
    private Combo compressed;
    private Combo distributed;
    private Combo isDuplicate;
    private Combo overwrite;
    private Text index;
    private Text newindexfile;
    
    private Button wOK, wCancel;
    private boolean backupChanged;
    
    @SuppressWarnings("unused")
    private SelectionAdapter lsDef;
    
    private CreateTable keysCT = null;
    private CreateTable payloadCT = null;
    
    private RecordList keysRecordList = new RecordList();
    private RecordList payloadRecordList = new RecordList();
    
    private Validator validator;

    public ECLIndexDialog(Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta) {
        super(parent, jobEntryInt, rep, jobMeta);
        jobEntry = (ECLIndex) jobEntryInt;
        if (this.jobEntry.getName() == null) {
            this.jobEntry.setName("Index");
        }
        
        this.validator = new Validator();
    }
    
    public final void toggleEnable() {
        Color white = new Color(null, 255, 255, 255);
        Color grey = new Color(null, 245, 245, 245);

        if (isDuplicate.getText().equals(COMBO_ITEM_YES)) {

            baserecset.setEnabled(false);
            baserecset.setBackground(grey);
            indexfile.setEnabled(false);
            indexfile.setBackground(grey);
            sorted.setEnabled(false);
            sorted.setBackground(grey);
            preload.setEnabled(false);
            preload.setBackground(grey);
            opt.setEnabled(false);
            opt.setBackground(grey);
            compressed.setEnabled(false);
            compressed.setBackground(grey);
            distributed.setEnabled(false);
            distributed.setBackground(grey);

            index.setBackground(white);
            index.setEnabled(true);
            newindexfile.setBackground(white);
            newindexfile.setEnabled(true);

            tabFolder.getTabList()[1].setEnabled(false);
            tabFolder.getTabList()[2].setEnabled(false);

            tabFolder.redraw();
        } else {
            index.setBackground(grey);
            index.setEnabled(false);
            newindexfile.setBackground(grey);
            newindexfile.setEnabled(false);

            baserecset.setEnabled(true);
            baserecset.setBackground(white);
            indexfile.setEnabled(true);
            indexfile.setBackground(white);
            sorted.setEnabled(true);
            sorted.setBackground(white);
            preload.setEnabled(true);
            preload.setBackground(white);
            opt.setEnabled(true);
            opt.setBackground(white);
            compressed.setEnabled(true);
            compressed.setBackground(white);
            distributed.setEnabled(true);
            distributed.setBackground(white);

            tabFolder.getTabList()[1].setEnabled(true);
            tabFolder.getTabList()[2].setEnabled(true);

            tabFolder.redraw();

        }
    }

    public JobEntryInterface open() {
        Shell parentShell = getParent();
        Display display = parentShell.getDisplay();
        
        shell = new Shell(parentShell, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
        String[] columnNames = new String[] {"column_name"};
        keysCT = new CreateTable(shell);
        keysCT.setColumnNames(columnNames);
        payloadCT = new CreateTable(shell);
        payloadCT.setColumnNames(columnNames);
        
        tabFolder = new TabFolder (shell, SWT.FILL | SWT.RESIZE | SWT.MIN | SWT.MAX);
        FormData data = new FormData();
        
        data.height = 500;
        data.width = 690;
        tabFolder.setLayoutData(data);
        
        String datasets[] = null;
        AutoPopulate ap = new AutoPopulate();
        try{
            datasets = ap.parseDatasetsRecordsets(this.jobMeta.getJobCopies());
        }catch (Exception e){
            System.out.println("Error Parsing existing Datasets");
            System.out.println(e.toString());
            datasets = new String[]{""};
        }
        
        Composite compForGrp = new Composite(tabFolder, SWT.NONE);

        compForGrp.setBackground(new Color(tabFolder.getDisplay(),255,255,255));
        compForGrp.setLayout(new FormLayout());
                
                
        
        TabItem generalTab = new TabItem(tabFolder, SWT.NULL);
         generalTab.setText ("General");
        
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
        shell.setText("Index");

        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;

        shell.setLayout(formLayout);
        shell.setText("Define an ECL Index");

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
        generalGroupFormat.width = 400;
        generalGroupFormat.height = 95;
        generalGroupFormat.left = new FormAttachment(middle, 0);
        generalGroup.setLayoutData(generalGroupFormat);
        
        jobEntryName = buildText("Job Entry Name", null, lsMod, middle, margin, generalGroup);
        this.isDuplicate = buildCombo("Duplicate Existing Index?", jobEntryName, lsMod, middle, margin, generalGroup, new String[]{COMBO_ITEM_NO, COMBO_ITEM_YES});
        this.overwrite = buildCombo("Overwrite existing files?", isDuplicate, lsMod, middle, margin, generalGroup, new String[]{COMBO_ITEM_NO, COMBO_ITEM_YES});
        
        //All other contols
        //Join Declaration
        Group indexGroup = new Group(compForGrp, SWT.SHADOW_NONE);
        props.setLook(indexGroup);
        indexGroup.setText("Index Details");
        indexGroup.setLayout(groupLayout);
        FormData joinGroupFormat = new FormData();
        joinGroupFormat.top = new FormAttachment(generalGroup, margin);
        joinGroupFormat.width = 400;
        joinGroupFormat.height = 250;
        joinGroupFormat.left = new FormAttachment(middle, 0);
        indexGroup.setLayoutData(joinGroupFormat);

        
        //this.baserecset = buildText("Baserecset", null, lsMod, middle, margin, indexGroup);
        this.baserecset  = buildCombo("Baserecset", null, lsMod, middle, margin, indexGroup,datasets);
        
        
       
        //tblOutput
        
        
        this.indexfile = buildText("Index File", baserecset, lsMod, middle, margin, indexGroup);
        this.sorted = buildCombo("Is Sorted", indexfile, lsMod, middle, margin, indexGroup, new String[]{COMBO_ITEM_NO, COMBO_ITEM_YES});
        this.preload = buildCombo("Is Preload", sorted, lsMod, middle, margin, indexGroup, new String[]{COMBO_ITEM_NO, COMBO_ITEM_YES,});
        this.opt = buildCombo("Is OPT", preload, lsMod, middle, margin, indexGroup, new String[]{COMBO_ITEM_NO, COMBO_ITEM_YES});
        this.compressed = buildCombo("Compressed", opt, lsMod, middle, margin, indexGroup, new String[]{"", COMPRESSION_TYPE_LZW, COMPRESSION_TYPE_ROW, COMPRESSION_TYPE_FIRST});
        this.distributed = buildCombo("Is Distributed", compressed, lsMod, middle, margin, indexGroup, new String[]{COMBO_ITEM_NO, COMBO_ITEM_YES});
        
        this.index = buildText("Index", distributed, lsMod, middle, margin, indexGroup);
        this.newindexfile = buildText("New Indexfile", index, lsMod, middle, margin, indexGroup);
        
        generalTab.setControl(compForGrp);
         
        if(jobEntry.getKeys() != null){
            keysRecordList = jobEntry.getKeys();
            keysCT.setRecordList(jobEntry.getKeys());
            
            if(keysRecordList.getRecords() != null && keysRecordList.getRecords().size() > 0) {
                    for (Iterator<RecordBO> iterator = keysRecordList.getRecords().iterator(); iterator.hasNext();) {
                            iterator.next();
                    }
            }
        }
        
        this.isDuplicate.addSelectionListener(new SelectionAdapter() {
                    public void widgetSelected(SelectionEvent e) {
                       toggleEnable();  
                    };
            });
       
        keysCT.setIncludeCopyParent(true);
        keysCT.buildDefTab("Keys", tabFolder);
       
        
        
        if(jobEntry.getPayload() != null){
            payloadRecordList = jobEntry.getPayload();
            payloadCT.setRecordList(jobEntry.getPayload());
            
            if(payloadRecordList.getRecords() != null && payloadRecordList.getRecords().size() > 0) {
                    for (Iterator<RecordBO> iterator = payloadRecordList.getRecords().iterator(); iterator.hasNext();) {
                            iterator.next();
                    }
            }
        }
        
        baserecset.addSelectionListener(new SelectionListener() {

			@Override
			public void widgetDefaultSelected(SelectionEvent arg0) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void widgetSelected(SelectionEvent arg0) {
				// TODO Auto-generated method stub
				AutoPopulate ap = new AutoPopulate();
				try{
					RecordList fields = ap.rawFieldsByDataset(baserecset.getText(), jobMeta.getJobCopies());
					payloadCT.setParentLayout(fields);
					keysCT.setParentLayout(fields);
				}catch (Exception e){
					System.out.println(e);
				}
			}
        	
        });
        payloadCT.setIncludeCopyParent(true);
        
        payloadCT.buildDefTab("Payload", tabFolder);
        keysCT.redrawTable(true);
        
       
        wOK = new Button(shell, SWT.PUSH);
        wOK.setText("OK");
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText("Cancel");

        BaseStepDialog.positionBottomButtons(shell, new Button[]{wOK, wCancel}, margin, tabFolder);

        
       
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

        if (jobEntry.getBaserecset() != null) {
            baserecset.setText(jobEntry.getBaserecset());
        }

        if (jobEntry.getIndexfile() != null) {
            indexfile.setText(jobEntry.getIndexfile());
        }

        sorted.setText(ifBlank(jobEntry.getSorted(), COMBO_ITEM_NO));
        
        preload.setText(ifBlank(jobEntry.getPreload(), COMBO_ITEM_NO));

        opt.setText(ifBlank(jobEntry.getOpt(), COMBO_ITEM_NO));

        compressed.setText(ifBlank(jobEntry.getCompressed(), COMBO_ITEM_NO));
        
        distributed.setText(ifBlank(jobEntry.getDistributed(), COMBO_ITEM_NO));

        
        if (jobEntry.getIndex() != null) {
            index.setText(jobEntry.getIndex());
        }
        
        if (jobEntry.getNewindexfile() != null) {
            newindexfile.setText(jobEntry.getNewindexfile());
        }
        
        isDuplicate.setText(ifBlank(jobEntry.getIsDuplicate(), COMBO_ITEM_NO));
        toggleEnable();
        
        overwrite.setText(ifBlank(jobEntry.getOverwrite(), COMBO_ITEM_NO));
        
        shell.pack();
        shell.open();
        while (!shell.isDisposed()) {
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }

        return jobEntry;

    }
    
    private String ifBlank(String item, String defaultVal) {
        return StringUtils.isNotBlank(item) ? item : defaultVal;
    }


    private void ok() {
        
        if (! this.validator.validate()) {
            return;
        }

        jobEntry.setName(jobEntryName.getText());
        jobEntry.setBaserecset(baserecset.getText());
        jobEntry.setIndexfile(indexfile.getText());
        jobEntry.setSorted(sorted.getText());
        jobEntry.setPreload(preload.getText());
        jobEntry.setOpt(opt.getText());
        jobEntry.setCompressed(compressed.getText());
        jobEntry.setDistributed(distributed.getText());
        jobEntry.setIndex(index.getText());
        jobEntry.setNewindexfile(newindexfile.getText());
        
        jobEntry.setKeys(keysCT.getRecordList());
        jobEntry.setPayload(payloadCT.getRecordList());
        jobEntry.setIsDuplicate(isDuplicate.getText());
        jobEntry.setOverwrite(overwrite.getText());

        dispose();
    }

    private void cancel() {
        jobEntry.setChanged(backupChanged);
        jobEntry = null;
        dispose();
    }

    
   
    
    private class Validator {

        boolean validate() {
            boolean isValid = true;
            StringBuilder errors = new StringBuilder();

            if (StringUtils.isBlank(jobEntryName.getText())) {
                isValid = false;
                errors.append("You must provide a \"Job Entry Name\"\r\n");
            }

            // @formatter:off
            isValid = COMBO_ITEM_YES.equals(isDuplicate.getText()) 
                           ? validateForDupIndex(errors) 
                           : validateForNonDupIndex(errors);
            // @formatter:on

            if (!isValid) {
                ErrorNotices en = new ErrorNotices();
                errors.append("\r\n")
                        .append("If you continue to save with errors you may encounter compile errors if you try to execute the job.\r\n\r\n");
                isValid = en.openValidateDialog(getParent(), errors.toString());
            }

            return isValid;
        }

        private boolean validateForNonDupIndex(StringBuilder errors) {
            List<Boolean> isValid = new ArrayList<Boolean>();

            if (keysRecordList.getRecords().isEmpty()) {
                errors.append("You must select one or more \"Keys\"\r\n");
                isValid.add(Boolean.FALSE);
            }

            isValid.add(validateForText(indexfile.getText(), errors,
                    "You must provide an \"Index File\""));

            return testValidity(isValid);
        }

        private boolean validateForDupIndex(StringBuilder errors) {
            List<Boolean> isValid = new ArrayList<Boolean>();
            isValid.add(validateForText(index.getText(), errors,
                    "You must provide an \"Index\""));

            isValid.add(validateForText(newindexfile.getText(), errors,
                    "You must provide a \"New Index File\""));

            return testValidity(isValid);
        }

        private boolean validateForText(String text, StringBuilder errors, String message) {
            boolean isValid = true;
            if (StringUtils.isBlank(text)) {
                isValid = false;
                errors.append(message + "\r\n");
            }

            return isValid;
        }

        private boolean testValidity(List<Boolean> isValid) {
            boolean areAllValid = true;
            for (Boolean item : isValid) {
                if (item == false) {
                    areAllValid = false;
                    break;
                }
            }

            return areAllValid;
        }
    }
}
