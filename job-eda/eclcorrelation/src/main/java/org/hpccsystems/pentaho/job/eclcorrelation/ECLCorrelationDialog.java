/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclcorrelation;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.eclipse.jface.viewers.CellEditor;
import org.eclipse.jface.viewers.ICellModifier;
import org.eclipse.jface.viewers.ILabelProviderListener;
import org.eclipse.jface.viewers.IStructuredContentProvider;
import org.eclipse.jface.viewers.ITableLabelProvider;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.jface.viewers.TextCellEditor;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Item;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TabFolder;
import org.eclipse.swt.widgets.TabItem;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeColumn;
import org.eclipse.swt.widgets.TreeItem;
import org.hpccsystems.eclguifeatures.AutoPopulate;
import org.hpccsystems.eclguifeatures.ErrorNotices;
import org.hpccsystems.ecljobentrybase.ECLJobEntryDialog;
import org.hpccsystems.recordlayout.RecordBO;
import org.hpccsystems.recordlayout.RecordLabels;
import org.hpccsystems.recordlayout.RecordList;
import org.pentaho.di.core.Const;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.job.dialog.JobDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
/**
 *
 * @author KeshavS
 */
public class ECLCorrelationDialog extends ECLJobEntryDialog{//extends JobEntryDialog implements JobEntryDialogInterface {
	
	public static final String NAME = "Name";
	public static final String RULE = "Rule";
	public static final String[] PROP = { NAME, RULE };
    private ECLCorrelation jobEntry;
    private Text jobEntryName;
    private Combo Method;
    private Combo datasetName;
    private Button wOK, wCancel;
    private boolean backupChanged;
    @SuppressWarnings("unused")
	private SelectionAdapter lsDef;
    ArrayList<Player> fields;
    private Combo Rule;
    
    public Button chkBox;
    public static Text outputName;
    public static Label label;
    private String persist;
    private Composite composite;
    private String defJobName;
    private Text OutName;
    private String outlierRules[] = null;
    private RecordList recordList;
    
    public ECLCorrelationDialog(Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta) {
        super(parent, jobEntryInt, rep, jobMeta);
        jobEntry = (ECLCorrelation) jobEntryInt;
        if (this.jobEntry.getName() == null) {
            this.jobEntry.setName("Correlation");
        }
    }

    public JobEntryInterface open() {
        Shell parentShell = getParent();
        final Display display = parentShell.getDisplay();
        
        String datasets[] = null;
        String outlRules[] = null;

        final AutoPopulate ap = new AutoPopulate();
        try{
            datasets = ap.parseDatasetsRecordsets(this.jobMeta.getJobCopies());
            defJobName = ap.getGlobalVariable(this.jobMeta.getJobCopies(), "jobName");

        }catch (Exception e){
            System.out.println("Error Parsing existing Datasets");
            System.out.println(e.toString());
            datasets = new String[]{""};
        }
        
        try{
        	outlRules = ap.parseOutlierRules(this.jobMeta.getJobCopies());
        	//outlierRules = Arrays.asList(outlRules);
        }catch (Exception e){
            System.out.println("Error Parsing existing outlier rules");
            System.out.println(e.toString());
            outlRules = new String[]{""};
        }

        shell = new Shell(parentShell, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
        fields = new ArrayList();
        recordList = new RecordList();
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
		shell.setText("Correlation");
		
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
        fieldsGroupFormat.height = 110;
        fieldsGroupFormat.left = new FormAttachment(middle, 0);
        fieldsGroupFormat.right = new FormAttachment(100, 0);
        fieldsGroup.setLayoutData(fieldsGroupFormat);
        
        Group ruleGroup = new Group(compForGrp, SWT.SHADOW_NONE);
        props.setLook(ruleGroup);
        ruleGroup.setText("Outlier Rule");
        ruleGroup.setLayout(groupLayout);
        FormData ruleFormData = new FormData();
        ruleFormData.top = new FormAttachment(fieldsGroup, margin);
        ruleFormData.width = 340;
        ruleFormData.height = 65;
        ruleFormData.left = new FormAttachment(middle, 0);
        ruleFormData.right = new FormAttachment(100, 0);
        ruleGroup.setLayoutData(ruleFormData);

        
        Method = buildCombo("Method:", jobEntryName, lsMod, middle, margin, fieldsGroup, new String[]{"Pearson", "Spearman"});
        datasetName = buildCombo("Dataset Name:", Method, lsMod, middle, margin, fieldsGroup, datasets);
        OutName = buildText("Output Dataset:", datasetName, lsMod, middle, margin, fieldsGroup);
        
		String rul = "";
		for(int i=0; i<outlRules.length; i++){
			rul += "|";
			rul += outlRules[i];
		}
		outlierRules = rul.split("\\|");
		
	        Rule = buildCombo("Rule:", jobEntryName, lsMod, middle, margin, ruleGroup, outlierRules );
		    //Rule = new Combo(ruleGroup, SWT.DROP_DOWN);
		    //Rule.select(arg0)
		    //Rule.setText("Select an Outlier Rule");
			Rule.setItems(outlierRules);
			//Rule.setItems(new String[]{rul,"test"});
		
			 //Begin
	        
	        Group perGroup = new Group(compForGrp, SWT.SHADOW_NONE);
	        props.setLook(perGroup);
	        perGroup.setText("Persist");
	        perGroup.setLayout(groupLayout);
	        FormData perGroupFormat = new FormData();
	        perGroupFormat.top = new FormAttachment(ruleGroup, margin);
	        perGroupFormat.width = 340;
	        perGroupFormat.height = 80;
	        perGroupFormat.left = new FormAttachment(middle, 0);
	        perGroupFormat.right = new FormAttachment(100, 0);
	        perGroup.setLayoutData(perGroupFormat);
	        
	        composite = new Composite(perGroup, SWT.NONE);
	        composite.setLayout(new FormLayout());
	        composite.setBackground(new Color(null, 255, 255, 255));

	        final Composite composite_1 = new Composite(composite, SWT.NONE);
	        composite_1.setLayout(new GridLayout(2, false));
	        final FormData fd_composite_1 = new FormData();
	        fd_composite_1.top = new FormAttachment(0);
	        fd_composite_1.left = new FormAttachment(0, 10);
	        fd_composite_1.bottom = new FormAttachment(0, 34);
	        fd_composite_1.right = new FormAttachment(0, 390);
	        composite_1.setLayoutData(fd_composite_1);
	        composite_1.setBackground(new Color(null, 255, 255, 255));
	        
	        label = new Label(composite_1, SWT.NONE);
	        label.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false, 1, 1));
	        label.setText("Logical Name:");
	        label.setBackground(new Color(null, 255, 255, 255));

	        outputName = new Text(composite_1, SWT.BORDER);
	        outputName.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 1, 1));
	        outputName.setEnabled(false);
	        if(jobEntry.getPersistOutputChecked()!= null && jobEntry.getPersistOutputChecked().equals("true")){
	        	outputName.setEnabled(true);
	        }
	        
	        final Composite composite_2 = new Composite(composite, SWT.NONE);
	        composite_2.setLayout(new GridLayout(1, false));
	        final FormData fd_composite_2 = new FormData();
	        fd_composite_2.top = new FormAttachment(0, 36);
	        fd_composite_2.bottom = new FormAttachment(100, 0);
	        fd_composite_2.right = new FormAttachment(0, 390);
	        fd_composite_2.left = new FormAttachment(0, 10);
	        composite_2.setLayoutData(fd_composite_2);
	        composite_2.setBackground(new Color(null, 255, 255, 255));

	        chkBox = new Button(composite_2, SWT.CHECK);
	        chkBox.setText("Persist Ouput");
	        chkBox.setBackground(new Color(null, 255, 255, 255));
	        
	        chkBox.addSelectionListener(new SelectionAdapter() {
	            @Override
	            public void widgetSelected(SelectionEvent e) {
	            	Button button = (Button) e.widget;
	            	if(button.getSelection()){
	            		persist = "true";
	            		outputName.setEnabled(true);
	            	}
	            	else{
	            		persist = "false";
	            		outputName.setText("");
	            		outputName.setEnabled(false);
	            	}

	            }
	        });
	        //End 	
			
		item1.setControl(compForGrp);	
        
        TabItem item2 = new TabItem(tab, SWT.NULL);
        item2.setText("Fields Selected");
		
        ScrolledComposite sc2 = new ScrolledComposite(tab, SWT.H_SCROLL | SWT.V_SCROLL);
        Composite compForGrp2 = new Composite(sc2, SWT.NONE);
        compForGrp2.setLayout(new GridLayout(1, false));
        sc2.setContent(compForGrp2);

        // Set the minimum size
        sc2.setMinSize(300, 200);

        // Expand both horizontally and vertically
        sc2.setExpandHorizontal(true);
        sc2.setExpandVertical(true);
        
        item2.setControl(sc2);
        Button button = new Button(compForGrp2, SWT.PUSH);
        button.setText("Add Columns");
        button.setLayoutData(new GridData(GridData.FILL));
        
        final TableViewer tv = new TableViewer(compForGrp2,  SWT.CHECK | SWT.BORDER | SWT.FULL_SELECTION | SWT.H_SCROLL | SWT.V_SCROLL);

	    tv.setContentProvider(new PlayerContentProvider());
	    tv.setLabelProvider(new PlayerLabelProvider());
	    
	    final Table table = tv.getTable();
	    table.setLayoutData(new GridData(GridData.FILL_BOTH));
	    
	    final TableColumn tc0 = new TableColumn(table, SWT.LEFT);
	    tc0.setText("Select All");
	    tc0.setWidth(150);
	    tc0.setImage(RecordLabels.getImage("unchecked"));
	    tc0.addListener(SWT.Selection, new Listener() {
			@Override
			public void handleEvent(Event event) {
		        boolean checkBoxFlag = false;
		        for (int i = 0; i < table.getItemCount(); i++) {
		            if (table.getItems()[i].getChecked()) {
		                checkBoxFlag = true;
		                
		            }
		        }
		        if (checkBoxFlag) {
		            for (int m = 0; m < table.getItemCount(); m++) {
		                table.getItems()[m].setChecked(false);
		                tc0.setImage(RecordLabels.getImage("unchecked"));				                
		                table.deselectAll();
		            }
		        } else {
		            for (int m = 0; m < table.getItemCount(); m++) {
		                table.getItems()[m].setChecked(true);
		                tc0.setImage(RecordLabels.getImage("checked"));
		                table.selectAll();
		            }
		        } 	
		        tv.refresh();
		        table.redraw();
		    } 
		});
	    
	    TableColumn tc1 = new TableColumn(table, SWT.LEFT);
	    tc1.setText("Outlier(s)");
	    tc1.setWidth(150);
	    
	    if(jobEntry.getFields() != null)
            fields = jobEntry.getFields();
	    tv.setInput(fields);
	    if(fields != null && fields.size() > 0) {
            
            for (Iterator iterator = fields.iterator(); iterator.hasNext();) {
                    Player obj = (Player) iterator.next();
            }
	    }
        tv.setInput(fields);
        table.setRedraw(true);
        
        
	    Button del = new Button(compForGrp2, SWT.PUSH);
	    del.setText("Delete");
	    del.addSelectionListener(new SelectionAdapter(){
	    	public void widgetSelected(SelectionEvent event){
	    		int cnt = 0;
	    		for(int i = 0; i<table.getItemCount(); i++){
	    			if(table.getItem(i).getChecked()){
	    				
	    				fields.remove(Math.abs(cnt - i));
						cnt++;
					}
	    		}
	    		if(tc0.getImage().equals(RecordLabels.getImage("checked")))
	    			tc0.setImage(RecordLabels.getImage("unchecked")); 
	    		tv.refresh();
	    		tv.setInput(fields);
	    		
	    	}
	    });
	    
	    datasetName.addModifyListener(new ModifyListener(){
        	
            public void modifyText(ModifyEvent e){
            	fields = new ArrayList();
            	tv.refresh();
            	tv.setInput(fields);            	
            }
	    });


	    // Add a listener to change the tableviewer's input
	    button.addSelectionListener(new SelectionAdapter() {
		      public void widgetSelected(SelectionEvent event) {
		    	    final Shell shellFilter = new Shell(display);
					FormLayout layoutFilter = new FormLayout();
					layoutFilter.marginWidth = 25;
					layoutFilter.marginHeight = 25;
					shellFilter.setLayout(layoutFilter);
					shellFilter.setText("Filter Columns");
					
					Label filter = new Label(shellFilter, SWT.NONE);
					filter.setText("Filter: ");
					final Text NameFilter = new Text(shellFilter, SWT.SINGLE | SWT.BORDER);
					
					final ArrayList<String[]> field = new ArrayList<String[]>();
					final Tree tab = new Tree(shellFilter, SWT.CHECK | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
					tab.setHeaderVisible(true);
					tab.setLinesVisible(true);
					
				    final TreeColumn column1 = new TreeColumn(tab, SWT.LEFT);
				    column1.setText("Fields");
				    column1.setWidth(200);
				    column1.setImage(RecordLabels.getImage("unchecked"));
				    
				    final TreeColumn column2 = new TreeColumn(tab, SWT.LEFT);			   
				    column2.setWidth(0);
				    
				    column1.addListener(SWT.Selection, new Listener() {
						@Override
						public void handleEvent(Event event) {
					        boolean checkBoxFlag = false;
					        for (int i = 0; i < tab.getItemCount(); i++) {
					            if (tab.getItems()[i].getChecked()) {
					                checkBoxFlag = true;
					                
					            }
					        }
					        if (checkBoxFlag) {
					            for (int m = 0; m < tab.getItemCount(); m++) {
					                tab.getItems()[m].setChecked(false);
					                column1.setImage(RecordLabels.getImage("unchecked"));				                
					                tab.deselectAll();
					            }
					        } else {
					            for (int m = 0; m < tab.getItemCount(); m++) {
					            	if(!tab.getItem(m).getText(1).equals("string")){
					            		tab.getItem(m).setChecked(true);
					            		column1.setImage(RecordLabels.getImage("checked"));
					            	}
					            }
					        } 		
					        for(int m = 0; m<tab.getItemCount(); m++){
					        	if(tab.getItem(m).getChecked()){
					        		String st = tab.getItem(m).getText();
					        		
					        		int idx = 0; String type = "";
					 	   	      	 for(Iterator<String[]> it2 = field.iterator(); it2.hasNext(); ){
					 	   	     	 	 String[] s = it2.next();
					 	   	     	 	 type = s[2];
					 	   	     		 if(s[0].equalsIgnoreCase(st)){
					 	   	     				idx = field.indexOf(s);
					 	   	     				break;
					 	   	     		 }
					 	   	     	 }
					 	   	     	 field.remove(idx);
					 	   	     	 field.add(idx,new String[]{st,"true",type});
					 	   	     	 // to find index of the selected item in the original field array list
					        	}
					        	if(!tab.getItem(m).getChecked()){
					        		String st = tab.getItem(m).getText();
					        		
					        		int idx = 0; String type = "";
					 	   	      	 for(Iterator<String[]> it2 = field.iterator(); it2.hasNext(); ){
					 	   	     	 	 String[] s = it2.next();
					 	   	     	 	 type = s[2];
					 	   	     		 if(s[0].equalsIgnoreCase(st)){
					 	   	     				idx = field.indexOf(s);
					 	   	     				break;
					 	   	     		 }
					 	   	     	 }
					 	   	     	 field.remove(idx);
					 	   	     	 field.add(idx,new String[]{st,"false",type});
					 	   	     	 // to find index of the selected item in the original field array list
					        	}
					        }
			                tab.redraw();
					    } 
					});
				    
				    Button okFilter = new Button(shellFilter, SWT.PUSH);
					okFilter.setText("     OK     ");
					Button CancelFilter = new Button(shellFilter, SWT.PUSH);
					CancelFilter.setText("   Cancel   ");
				    				
					AutoPopulate ap = new AutoPopulate();
	          try{
	      		
	              String[] items = ap.fieldsRecByDataset( datasetName.getText(),jobMeta.getJobCopies());
	              //RecordList rec = ap.rawFieldsByDataset( datasetName.getText(),jobMeta.getJobCopies());
                  RecordList rec = ap.buildMyRecordList(items);
                  
                  for(int i = 0; i < rec.getRecords().size(); i++){
                      TreeItem item = new TreeItem(tab, SWT.NONE);
                      RecordBO ob = rec.getRecords().get(i);
                      item.setText(0, ob.getColumnName().toLowerCase());
                      String type = "String";
                      String width = "";
                      try{
                             type = ob.getColumnType();
                             width = ob.getColumnWidth();
                             item.setText(1,type+width);
                             if(ob.getColumnType().toLowerCase().contains("string")){
                            	 item.setBackground(0, new Color(null,211,211,211));
                             }
                      }catch (Exception e){
                             System.out.println("Frequency Cant look up column type");
                      }
                      
                      field.add(new String[]{rec.getRecords().get(i).getColumnName().toLowerCase(),"false",type+width});
                }
	              
	              
	          }catch (Exception ex){
	              System.out.println("failed to load record definitions");
	              System.out.println(ex.toString());
	              ex.printStackTrace();
	          }
	          FormData dat = new FormData();
			        dat.top = new FormAttachment(NameFilter, 0, SWT.CENTER);
			        filter.setLayoutData(dat);
			        dat = new FormData();
			        dat.left = new FormAttachment(filter, 75, SWT.LEFT);
			        dat.right = new FormAttachment(100, 0);
			        NameFilter.setLayoutData(dat);
			        
			        dat = new FormData(200,200);
			        dat.top = new FormAttachment(filter, 25);
			        dat.left = new FormAttachment(filter, 0, SWT.LEFT);
			        dat.right = new FormAttachment(100, 0);
			        tab.setLayoutData(dat);
			        
			        dat = new FormData();
			        dat.top = new FormAttachment(tab, 25);
			        dat.left = new FormAttachment(0, 45);
			        okFilter.setLayoutData(dat);
			        
			        dat = new FormData();
			        dat.top = new FormAttachment(tab, 25);
			        dat.left = new FormAttachment(okFilter, 15);
			        CancelFilter.setLayoutData(dat);
		       
			        NameFilter.addModifyListener(new ModifyListener(){
			        	
			            public void modifyText(ModifyEvent e){
			            		
			            		tab.setItemCount(0);		            		
			            		for(Iterator<String[]> it1 = field.iterator(); it1.hasNext(); ){
			            			String[] s = it1.next();
			            			if(s[0].startsWith(NameFilter.getText())){
			            				TreeItem I = new TreeItem(tab, SWT.NONE);
			            				I.setText(0,s[0]);
			            				//I.setText(1,s[2]);
			            				if(s[1].equalsIgnoreCase("true")) 
			            					I.setChecked(true);
			            				/*if(s[2].equalsIgnoreCase("string")){ 
			            					I.setChecked(false);
			            					I.setBackground(new Color(null,211,211,211));
			            				}*/
			            			}
			            		}
			            		tab.setRedraw(true);
			            		
			            }
			        });
			        
			       
			        
			        tab.addListener(SWT.Selection, new Listener() {
			    	     public void handleEvent(Event event) {
			    	    	 if(((TreeItem)event.item).getText(1).equalsIgnoreCase("string")) 
			    	    		 ((TreeItem)event.item).setChecked(false);
			    	    	 String st = ((TreeItem)event.item).getText(0);
			    	    	 String type = ((TreeItem)event.item).getText(1);
			    	    	 boolean f = ((TreeItem)event.item).getChecked();
			 	   	      	 int idx = 0; 
			 	   	      	 for(Iterator<String[]> it2 = field.iterator(); it2.hasNext(); ){
			 	   	     	 	 String[] s = it2.next();
			 	   	     		 if(s[0].equalsIgnoreCase(st)){
			 	   	     				idx = field.indexOf(s);
			 	   	     				break;
			 	   	     		 }
			 	   	     	 }
			 	   	     	 field.remove(idx);
			 	   	     	 if(f)
			 	   	     		 field.add(idx,new String[]{st,"true",type});
			 	   	     	 else
			 	   	     		 field.add(idx,new String[]{st,"false",type});
			    	   	}
			       });
			        
			        Listener okfilter = new Listener(){
						@Override
						public void handleEvent(Event arg0) {
							ArrayList<String> check = new ArrayList<String>();
							if(table.getItemCount() > 0){
								for(int i = 0; i<table.getItemCount(); i++){
									check.add(table.getItem(i).getText());
								}
							}
							
							for(Iterator<String[]> it = field.iterator(); it.hasNext();){
								String[] S = it.next();
								if(S[1].equalsIgnoreCase("True") && !check.contains(S[0])){
									Player p = new Player();
									p.setFirstName(S[0]);
									int idx = 0;
									if(outlierRules != null){
										for(int i = 0; i<outlierRules.length; i++){
											if(outlierRules[i].toLowerCase().contains(S[0].toLowerCase())){
												idx = i;
												break;
											}
										}
										p.setRule(outlierRules[idx]);
									}
									else{
										p.setRule("");
									}
									 
									fields.add(p);
									
									
								}
								
							}
							
							tv.setInput(fields);
							
							shellFilter.dispose();
							
						}
			        	
			        };
			        okFilter.addListener(SWT.Selection, okfilter);

			        Listener cancelfilter = new Listener(){

						@Override
						public void handleEvent(Event arg0) {
							shellFilter.dispose();
							
						}
			        	
			        };
			        CancelFilter.addListener(SWT.Selection, cancelfilter);

					shellFilter.pack();
			        shellFilter.open();
					while (!shellFilter.isDisposed()) {
						if (!display.readAndDispatch())
							display.sleep();
					}
		      }
		    });
	    
	    table.setHeaderVisible(true);
	    table.setLinesVisible(true);
	    table.addListener (SWT.Selection, new Listener () {
            public void handleEvent (Event event) {
            	if(event.detail == SWT.CHECK){	
            		tv.refresh();
                    table.redraw();
            	}
            }
        });
	    
	    
	    
	    final CellEditor[] editors = new CellEditor[2];
	    editors[0] = new TextCellEditor(table);
	    editors[1] = new TextCellEditor(table);
	    tv.setColumnProperties(PROP);
	    tv.setCellModifier(new PersonCellModifier(tv));
	    tv.setCellEditors(editors);
        
        
        wOK = new Button(shell, SWT.PUSH);
        wOK.setText("OK");
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText("Cancel");

        BaseStepDialog.positionBottomButtons(shell, new Button[]{wOK, wCancel}, margin, tab);
        
        
        // Add listeners
        Listener cancelListener = new Listener() {

            public void handleEvent(Event e) {
                cancel();
            }
        };
        Listener okListener = new Listener() {

            public void handleEvent(Event e) {
            	recordList = new RecordList();
            	RecordBO ob = new RecordBO();
            	ob.setColumnName("Fields");
            	ob.setColumnType("STRING");
            	ob.setDefaultValue("");
            	recordList.addRecordBO(ob);
            	ob = new RecordBO();
            	ob.setColumnName("Val");
            	ob.setColumnType("REAL");
            	ob.setDefaultValue("0");
            	recordList.addRecordBO(ob);
            	ob = new RecordBO();
            	ob.setColumnName("Recs_used");
            	ob.setColumnType("INTEGER");
            	ob.setDefaultValue("0");
            	recordList.addRecordBO(ob);
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
        
        if (jobEntry.getOutName() != null) {
            OutName.setText(jobEntry.getOutName());
        }
        
        if (jobEntry.getMethod() != null) {
            Method.setText(jobEntry.getMethod());
        }
        
        if(jobEntry.getFields() != null){
        	fields = jobEntry.getFields();
        	tv.setInput(fields);        	
        }
        
        if(jobEntry.getRule() != null){
        	Rule.setText(jobEntry.getRule());
        }

        if (jobEntry.getRecordList() != null) {
        	recordList = jobEntry.getRecordList();
        }

        if (jobEntry.getPersistOutputChecked() != null && chkBox != null) {
        	chkBox.setSelection(jobEntry.getPersistOutputChecked().equals("true")?true:false);
        }
        
        if(chkBox != null && chkBox.getSelection()){
        	for (Control control : composite_1.getChildren()) {
        		if(!control.isDisposed()){
					if (jobEntry.getOutputName() != null && outputName != null) {
			        	outputName.setText(jobEntry.getOutputName());
					}
					if (jobEntry.getLabel() != null && label != null) {
			    		label.setText(jobEntry.getLabel());
					}
        		}
        	}
		}
        if(jobEntry.getDefJobName() != null){
        	defJobName = jobEntry.getDefJobName();
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
    	
    	if(this.Method.getText().equals("")){
    		isValid = false;
    		errors += "\"Method\" is a required field!\r\n";
    	}
    	
    	if(this.datasetName.getText().equals("")){
    		isValid = false;
    		errors += "\"Dataset Name\" is a required field!\r\n";
    	}
    	
    	if(this.OutName.getText().equals("")){
    		isValid = false;
    		errors += "\"Output Name\" is a required field!\r\n";
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
        jobEntry.setMethod(this.Method.getText());
        jobEntry.setFields(fields);
        jobEntry.setRule(this.Rule.getText());
        jobEntry.setRecordList(recordList);
        jobEntry.setOutName(OutName.getText());
        
        if(chkBox.getSelection() && outputName != null){
        	jobEntry.setOutputName(outputName.getText());
        }
        if(chkBox.getSelection() && label != null){
        	jobEntry.setLabel(label.getText());
        }
        if(chkBox != null){
        	jobEntry.setPersistOutputChecked(chkBox.getSelection()?"true":"false");
        }
        if(defJobName.trim().equals("")){
        	defJobName = "Spoon-job";
        }
        jobEntry.setDefJobName(defJobName);
        
        dispose();
    }

    private void cancel() {
        jobEntry.setChanged(backupChanged);
        jobEntry = null;
        dispose();
    }

}



class Player { 
	  private String firstName;
	  
	  private String rule;

	  public String getFirstName() {
		  return firstName;
	  }

	  public void setFirstName(String firstName) {
		  this.firstName = firstName;
	  }

	  public String getRule() {
		  return rule;
	  }

	  public void setRule(String rule) {
		  this.rule = rule;
	  }
	  
	  
}



class PlayerLabelProvider implements ITableLabelProvider {

	
	
	public PlayerLabelProvider() {
	}


	public Image getColumnImage(Object arg0, int arg1) {
		return null;
	}


	public String getColumnText(Object arg0, int arg1) {
	  Player values = (Player) arg0;

	  switch(arg1){
	  case 0:
	  	  return values.getFirstName();
	  case 1:
	  	  return values.getRule();

	  }
	  return null;
	}
	
	public void addListener(ILabelProviderListener arg0) {
	  // Throw it away
	}
	
	public void dispose() {
	 
	}
	
	public boolean isLabelProperty(Object arg0, String arg1) {
	  return false;
	}
	
	public void removeListener(ILabelProviderListener arg0) {
	  // Do nothing
	}
}


class PlayerContentProvider implements IStructuredContentProvider {

	public Object[] getElements(Object arg0) {
	  
	  return ((List) arg0).toArray();
	}
	
	public void dispose() {
	  // We don't create any resources, so we don't dispose any
	}
	
	public void inputChanged(Viewer arg0, Object arg1, Object arg2) {
	  // Nothing to do
	}
}


class PersonCellModifier implements ICellModifier {
	  private Viewer viewer;

	  public PersonCellModifier(Viewer viewer) {
	    this.viewer = viewer;
	  }

	  public boolean canModify(Object element, String property) {
	    // Allow editing of all values
	    return true;
	  }
	  public Object getValue(Object element, String property) {
	    Player p = (Player) element;
	    if (ECLCorrelationDialog.NAME.equals(property))
	      return p.getFirstName();
	    if (ECLCorrelationDialog.RULE.equals(property))
		      return p.getRule();
	    else
	      return null;
	  }

	  public void modify(Object element, String property, Object value) {
	    if (element instanceof Item)
	      element = ((Item) element).getData();

	    Player p = (Player) element;
	    if (ECLCorrelationDialog.NAME.equals(property))
	      p.setFirstName((String) value);
	    if (ECLCorrelationDialog.RULE.equals(property))
		      p.setRule((String) value);
	   
	    // Force the viewer to refresh
	    viewer.refresh();
	  }
}