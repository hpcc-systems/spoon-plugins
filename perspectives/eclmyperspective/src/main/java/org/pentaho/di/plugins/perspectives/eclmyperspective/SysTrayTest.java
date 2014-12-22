package org.pentaho.di.plugins.perspectives.eclmyperspective;


import java.awt.color.*;
import java.awt.Dimension;
import java.awt.GradientPaint;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.lang.ArrayUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CLabel;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.dnd.DND;
import org.eclipse.swt.dnd.DragSource;
import org.eclipse.swt.dnd.DragSourceAdapter;
import org.eclipse.swt.dnd.DragSourceEvent;
import org.eclipse.swt.dnd.DropTarget;
import org.eclipse.swt.dnd.DropTargetAdapter;
import org.eclipse.swt.dnd.DropTargetEvent;
import org.eclipse.swt.dnd.TextTransfer;
import org.eclipse.swt.dnd.Transfer;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.ImageData;
import org.eclipse.swt.graphics.ImageLoader;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TabFolder;
import org.eclipse.swt.widgets.TabItem;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PiePlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.chart.renderer.category.StackedBarRenderer;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.general.DefaultPieDataset;
import org.jfree.data.xy.IntervalXYDataset;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.experimental.chart.swt.ChartComposite;
import org.swtchart.Chart;
import org.swtchart.IBarSeries;
import org.swtchart.IBarSeries.BarWidthStyle;
import org.swtchart.ILegend;
import org.swtchart.ILineSeries;
import org.swtchart.LineStyle;
import org.swtchart.ISeries.SeriesType;
import org.w3c.dom.Document;

import au.com.bytecode.opencsv.CSVReader;

public class SysTrayTest {
	static String[] filePath;
	static String fileName = "";	
	static ArrayList<String> map = new ArrayList<String>();
	static ArrayList<String[]> columns = new ArrayList<String[]>();
	static CTabItem ite;
	static int choice;
	static boolean numeric;
	static Display display;
	static Shell shell;
	static JFreeChart chart;
	public static void main(String[] args) throws Exception{
		 
		display = new Display();
		shell = new Shell(display);
		GridLayout gridLayout = new GridLayout();
		gridLayout.numColumns = 1;
		shell.setLayout(gridLayout);
		shell.setText("Report Builder");
		shell.setImage(new Image(display, "D:\\Users\\703119704\\Desktop\\reportbuilder.png"));
		
		Color color = shell.getBackground();
		shell.setBackground(new Color(null,255,255,255));
		
		final Composite comp2 = new Composite(shell, SWT.NONE);		
		comp2.setLayout(new GridLayout(3,false));
		comp2.setLayoutData(new GridData(GridData.FILL_BOTH));
		comp2.setBackground(new Color(null,255,255,255));

		
		
		Composite three = new Composite(comp2, SWT.NONE | SWT.BORDER);
		three.setLayout(new GridLayout(1,false));
		three.setLayoutData(new GridData(GridData.FILL_BOTH));//
		three.setBackground(new Color(null,188,188,188));
		
		Composite t1 = new Composite(three, SWT.NONE);
		t1.setLayout(new GridLayout(3,false));
		t1.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
		t1.setBackground(three.getBackground());
		
		Label tL = new Label(t1, SWT.NONE);
		tL.setText("Unsaved Report");
		tL.setFont(new Font(display,"Arial", 18, SWT.BOLD ));
		tL.setBackground(three.getBackground());
		
		Label tl2 = new Label(t1, SWT.NONE);
		tl2.setText("					");
		tl2.setBackground(three.getBackground());
		tl2.setLayoutData(new GridData(GridData.FILL));
		
		ToolBar tc = new ToolBar(t1,  SWT.FLAT | SWT.WRAP | SWT.RIGHT);
	    tc.setBackground(t1.getBackground());	    
		tc.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
		
		ToolItem itemPdf = new ToolItem(tc, SWT.PUSH);
	    itemPdf.setToolTipText("Print PDF");
	    Image icon = new Image(display, "D:\\Users\\703119704\\Downloads\\Flow-flow\\assembly\\package-res\\ui\\images\\print.png");
	    itemPdf.setImage(icon);
	    ToolItem itemSave = new ToolItem(tc, SWT.PUSH);
	    itemSave.setToolTipText("Save");
	    icon = new Image(display, "D:\\Users\\703119704\\Downloads\\Flow-flow\\assembly\\package-res\\ui\\images\\save.png");
	    itemSave.setImage(icon);
	    final ToolItem itemSaveAs = new ToolItem(tc, SWT.PUSH);
	    itemSaveAs.setToolTipText("Save As");
	    icon = new Image(display, "D:\\Users\\703119704\\Downloads\\Flow-flow\\assembly\\package-res\\ui\\images\\saveas.png");
	    itemSaveAs.setImage(icon);

				Composite three1 = new Composite(comp2, SWT.NONE | SWT.BORDER);	
				three1.setLayout(new GridLayout(1,false));
				GridData gd = new GridData();
				gd.horizontalSpan = 2;
				three1.setLayoutData(new GridData(GridData.FILL_VERTICAL));//
				three1.setBackground(new Color(null,188,188,188));
				
				Composite threeClose = new Composite(three1, SWT.NONE);
				threeClose.setBackground(three1.getBackground());
				threeClose.setLayout(new GridLayout(3,false));
				threeClose.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
				
				Label lbl1 = new Label(threeClose, SWT.NONE);
				lbl1.setText("Available Field(s) for: ");
				//lbl.setFont(new FontData());
				//lbl1.setLayoutData(new GridData(GridData.FILL_HORIZONTAL)); 
				lbl1.setBackground(three1.getBackground());
				
				Label l3 = new Label(threeClose, SWT.NONE);
				l3.setText("         ");
				l3.setBackground(three1.getBackground());
				l3.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
				
				ToolBar toolBar = new ToolBar(threeClose, SWT.FLAT | SWT.WRAP | SWT.RIGHT);
			    toolBar.setBackground(three1.getBackground());
			    toolBar.setLayoutData(new GridData(GridData.CENTER));
				
			    
			    ToolItem itemPush = new ToolItem(toolBar, SWT.PUSH);
			    itemPush.setToolTipText("Close");
			    Image icon1 = new Image(display, "D:\\Users\\703119704\\Downloads\\Flow-flow\\assembly\\package-res\\ui\\images\\close-panel.png");
			    itemPush.setImage(icon1);
			    
			    
				final Label lbl = new Label(three1, SWT.NONE);
				lbl.setText("");
				//lbl.setFont(new FontData());
				lbl.setLayoutData(new GridData(GridData.FILL_HORIZONTAL)); 
				lbl.setBackground(three1.getBackground());
				
				Composite threeFind = new Composite(three1, SWT.NONE);
				threeFind.setBackground(three1.getBackground());
				threeFind.setLayout(new GridLayout(2,false));
				threeFind.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
				
				Label L = new Label(threeFind, SWT.NONE);
				L.setText("Find: ");
				L.setBackground(three1.getBackground());
				
				
				Text txt = new Text(threeFind, SWT.NONE | SWT.BORDER);
			    txt.setText("");
				txt.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
			    
				final Tree tree = new Tree(three1, SWT.LEFT | SWT.V_SCROLL | SWT.BORDER | SWT.BEGINNING);	    		
			    tree.setLayoutData(new GridData(GridData.FILL_BOTH));
			    tree.setHeaderVisible(true);			    
			    
			    Transfer[] types = new Transfer[] { TextTransfer.getInstance() };
			    DragSource source = new DragSource(tree, DND.DROP_MOVE | DND.DROP_COPY);
			    source.setTransfer(types);
			    
			    source.addDragListener(new DragSourceAdapter() {
			        public void dragSetData(DragSourceEvent event) {
			          // Get the selected items in the drag source
			          DragSource ds = (DragSource) event.widget;
			          Tree tab = (Tree) ds.getControl();
			          System.out.println(tab); 
			          TreeItem[] selection = tree.getSelection();

			          StringBuffer buff = new StringBuffer();
			          for (int i = 0, n = selection.length; i < n; i++) { 
			            buff.append(selection[i].getText());
			          }

			           event.data = buff.toString();
			        }
			      });

			    
			    
			    
		final Composite three2 = new Composite(comp2, SWT.NONE | SWT.BORDER);
		three2.setLayout(new GridLayout(1,false)); 
		three2.setLayoutData(new GridData(GridData.FILL_VERTICAL));//
		three2.setBackground(color);
		
		
		
		CLabel L4 = new CLabel(three2, SWT.NONE);
		L4.setText("Visualization Settings and Tables");
		L4.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
	    L4.setBackground(shell.getDisplay().getSystemColor(SWT.COLOR_GRAY));
	    // Set the background gradient
	    L4.setBackground(new Color[] { shell.getDisplay().getSystemColor(SWT.COLOR_GRAY),
	        shell.getDisplay().getSystemColor(SWT.COLOR_CYAN),
	        shell.getDisplay().getSystemColor(SWT.COLOR_BLUE) }, new int[] { 100, 100 });
		
	    
		
		Group generalGroup = new Group(three2, SWT.SHADOW_NONE);        
        generalGroup.setText("General");
        //generalGroup.setSize(200,700);
        generalGroup.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
        generalGroup.setLayout(new GridLayout(2,false));
        
        
        
        Label L1 = new Label(generalGroup, SWT.NONE);
        L1.setText("Tables:");
        
        
        
        final Combo c1 = new Combo(generalGroup, SWT.BORDER | SWT.H_SCROLL);
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = (Document) dBuilder.parse("D:\\Users\\703119704\\Desktop\\ECL_Results\\W20140919-110005_results.xml");
		doc.getDocumentElement().normalize();
		String[] file = null;// = "";
		
		if(doc.getElementsByTagName("result") != null){
			int i = 0;
			file = new String[doc.getElementsByTagName("result").getLength()];			
			filePath = new String[doc.getElementsByTagName("result").getLength()];
			while(i < doc.getElementsByTagName("result").getLength()){
				file[i] =  doc.getElementsByTagName("result").item(i).getTextContent();
				filePath[i] = doc.getElementsByTagName("result").item(i).getTextContent();
				String[] S = file[i].split("\\\\");
				file[i] = S[S.length-1].split("_")[1]; 
				if(file[i].length()>15)
					file[i] = file[i].substring(0, 15) + "...\n";
				i++;
			}
			
		}		
		
		c1.setItems(file);
        c1.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
        
        c1.addModifyListener(new ModifyListener(){
        	
            public void modifyText(ModifyEvent e){
            	lbl.setText("  "+c1.getText());               	
            	int idx = c1.getSelectionIndex();
            	
            	fileName = filePath[idx];
            	System.out.println(fileName); 
            	tree.setItemCount(0);
            	try {
					readFile(fileName, true, null, tree, null,null,null,null,itemSaveAs);
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
	    });
        
        Group visualGroup = new Group(three2, SWT.SHADOW_NONE);        
        visualGroup.setText("Visualization");
        //generalGroup.setSize(200,700);
        visualGroup.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
        visualGroup.setLayout(new GridLayout(2,false));
        
        Label gr = new Label(visualGroup, SWT.NONE);
        gr.setText("Graph:");
        
        final Combo cx = new Combo(visualGroup, SWT.NONE);
        cx.setItems(new String[]{"Piechart","Linechart","LineChart_String","Barchart","Scatterchart"});
		cx.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
		
		
		Label gr1 = new Label(visualGroup, SWT.NONE);
        gr1.setText("Operation:");
        
        final Combo cx1 = new Combo(visualGroup, SWT.NONE);
        cx1.setItems(new String[]{"Sum", "Ave", "Variance", "Add", "Subtract", "Multiply", "Divide", "Mod"});
		cx1.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
		
		Group tabGroup = new Group(three2, SWT.SHADOW_NONE);        
        tabGroup.setText("Drag 'n Drop ");       
        tabGroup.setLayoutData(new GridData(GridData.FILL_BOTH));
        tabGroup.setLayout(new GridLayout(1,false));
        
        Composite axis = new Composite(tabGroup, SWT.NONE);
	    axis.setLayout(new GridLayout(2,false)); 
	    axis.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));//
	    axis.setBackground(color);
        
	    Label xaxis = new Label(axis, SWT.NONE);
	    xaxis.setText("X-Axis:");	    	   
	    
		final Tree tab1 = new Tree(tabGroup, SWT.CHECK | SWT.V_SCROLL | SWT.BORDER);	    		
	    tab1.setLayoutData(new GridData(GridData.FILL_BOTH));
	    tab1.getVerticalBar();
	    tab1.setHeaderVisible(true);
	    
	    final Combo ax = new Combo(axis, SWT.NONE);	    
	    ax.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
	    
	    
	    Composite buttons = new Composite(tabGroup, SWT.NONE);
	    buttons.setLayout(new GridLayout(2,false)); 
	    buttons.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));//
	    buttons.setBackground(color);
	    
	    
	    Button B1 = new Button(buttons, SWT.PUSH);
	    B1.setText("Delete");
	    
	    Button B2 = new Button(buttons, SWT.PUSH);
	    B2.setText("Create Graph");
	    
	    DropTarget target = new DropTarget(tab1, DND.DROP_MOVE | DND.DROP_COPY | DND.DROP_DEFAULT);
	    target.setTransfer(types);
	    target.addDropListener(new DropTargetAdapter() {
	      public void dragEnter(DropTargetEvent event) {
	        if (event.detail == DND.DROP_DEFAULT) {
	          event.detail = (event.operations & DND.DROP_COPY) != 0 ? DND.DROP_COPY : DND.DROP_NONE;
	        }

	        // Allow dropping text only
	        for (int i = 0, n = event.dataTypes.length; i < n; i++) {
	          if (TextTransfer.getInstance().isSupportedType(event.dataTypes[i])) {
	            event.currentDataType = event.dataTypes[i];
	          }
	        }
	      }

	      public void dragOver(DropTargetEvent event) {
	         event.feedback = DND.FEEDBACK_SELECT | DND.FEEDBACK_SCROLL;
	      }
	      public void drop(DropTargetEvent event) {
	        if (TextTransfer.getInstance().isSupportedType(event.currentDataType)) {
	          // Get the dropped data
	          DropTarget target = (DropTarget) event.widget;
	          //Table table = (Table) target.getControl();
	          Tree tab2 = (Tree) target.getControl();	          
	          String data = (String) event.data;
	          
	          // Check if dropped data already in Table
	          if(map.isEmpty()){
	        	  map.add(data);
	        	  
	        	  for(int i = 0; i<tree.getItemCount(); i++){
	        		if(tree.getItem(i).getText().equalsIgnoreCase(data)){
	        			columns.add(new String[]{data,Integer.toString(i)});
	        		}
	        	  }
	        	  
	        	  TreeItem item = new TreeItem(tab2, SWT.NONE);	        	  
		          item.setText(0,data);
		          tab2.redraw();
	          }
	          else{
	        	  if(!map.contains(data)){
	        		  map.add(data);
	        		  for(int i = 0; i<tree.getItemCount(); i++){
	  	        		if(tree.getItem(i).getText().equalsIgnoreCase(data)){
	  	        			columns.add(new String[]{data,Integer.toString(i)});
	  	        		}
	  	        	  }
	        		  TreeItem item = new TreeItem(tab2, SWT.NONE);
	    	          item.setText(0,data);
	    	          tab2.redraw();
	        	  }
	          }
	          // Create a new item in the table to hold the dropped data	          
	          String items[] = new String[tab2.getItemCount()];
	          for(int i = 0; i<tab2.getItemCount(); i++){
	        	  items[i] = tab2.getItem(i).getText();
	          }
	          ax.setItems(items);
	        }
	        
	        
	      }
	    });
	    
	    B1.addListener(SWT.Selection, new Listener(){

			@Override
			public void handleEvent(Event arg0) {
				int cnt = 0;//String txt;
				int len = tab1.getItemCount();
				for(int i = 0; i<len; i++){
					if(tab1.getItem(Math.abs(i-cnt)).getChecked()){
						map.remove(Math.abs(i-cnt));
						columns.remove(Math.abs(i-cnt));
						tab1.getItem(Math.abs(i-cnt)).dispose();
						cnt = cnt + 1;
					}
				}
			}
	    	
	    });
	    
	    final CTabFolder tabFolder = new CTabFolder(three, SWT.CLOSE);
	    tabFolder.setSimple(false);
	    
	    
	    ite = new CTabItem(tabFolder, SWT.CLOSE);
	    ite.setText("Table");
	    ite.setToolTipText("Table");
	    
	    final Table table = new Table(tabFolder, SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
	    table.setLinesVisible(true);
	    table.setHeaderVisible(true);	    
	    ite.setControl(table);
	    
	    tabFolder.setSelection(ite);
	    
	    
	    
	    
	    tabFolder.setLayoutData(new GridData(GridData.FILL_BOTH));
	    
	    B2.addListener(SWT.Selection, new Listener(){
			@Override
			public void handleEvent(Event arg0) {
				choice = cx.getSelectionIndex();
				ite = new CTabItem(tabFolder, SWT.CLOSE);
			    ite.setText("Graph");
			    ite.setToolTipText("Graph");
				try {
					readFile(fileName,false,table,null,tabFolder,ite,ax,tab1,itemSaveAs);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
	    });
	    itemPush.addListener(SWT.Selection, new Listener(){

			@Override
			public void handleEvent(Event arg0) {
				if(tree.getItemCount()>0)
					tree.setItemCount(0);
				if(!lbl.getText().equals(""))
					lbl.setText("");
				if(tab1.getItemCount()>0)
					tab1.setItemCount(0);
				//if(!c1.getText().equalsIgnoreCase(""))
					//c1.setText("");
				if(!cx.getText().equalsIgnoreCase(""))
					cx.setText("");
				if(!cx1.getText().equalsIgnoreCase(""))
					cx1.setText("");
			}
			
	    	
	    });
	    
        
		shell.open();
		while (!shell.isDisposed()) {
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }
	}
	

	
	
	
	
	
	public static void readFile(String filename, boolean header, Table table, Tree tree, final CTabFolder tabFolder, final CTabItem ite, Combo ax, Tree tab1, ToolItem SaveAs) throws IOException{
		if(table != null){
			System.out.println(columns.size()); 
			table.setItemCount(0);
			table.setRedraw(false);
			while ( table.getColumnCount() > 0 ) {
			    table.getColumns()[ 0 ].dispose();
			}
			table.setRedraw(true);			
		}
		if(!filename.equals("")){ 
			System.out.println(filename); 
			
			CSVReader reader = null;
			try {
				reader = new CSVReader(new FileReader(filename),',','"');
			} catch (FileNotFoundException e2) {
				e2.printStackTrace();
			}
            String [] strLineArr;
            
            // Open the file that is the first 
            // command line parameter
            FileInputStream fstream = null;
			try {
				fstream = new FileInputStream(filename);
			} catch (FileNotFoundException e2) {
				e2.printStackTrace();
			}
            // Get the object of DataInputStream
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            System.out.println(br); 
            int length = 0;
            boolean first = true;
            if(header == true){
            	try {
					while ((strLineArr = reader.readNext()) != null) {						
						for(int i = 0; i<strLineArr.length; i++){
							TreeItem treeItem0 = new TreeItem(tree, 0);
		        	    	treeItem0.setText(strLineArr[i]);
		        	    	
						}						
						break;					    
					}
				} catch (IOException e1) {
					e1.printStackTrace();
				}
            }
            
            	
            	
            
            else{
            	ArrayList<ArrayList<Double>> yaxes = new ArrayList<ArrayList<Double>>();
            	ArrayList<String> names = new ArrayList<String>();
            	for(int i = 0; i<columns.size()-1; i++){
            		yaxes.add(new ArrayList<Double>());            		
            	}
            	ArrayList<String> category = new ArrayList<String>();
            	ArrayList<Double> x_axis = new ArrayList<Double>();
            
            	XYSeries[] series = new XYSeries[columns.size()];
            	XYSeries[] scatterSeries = new XYSeries[columns.size()];
            	String[] bars = new String[columns.size()];
            	System.out.println(columns.size()+" Yehi hai"); 
            	int srcnt = 0;
            	for(Iterator<String[]> it = columns.iterator(); it.hasNext();){
            		String[] S = (String[]) it.next();
            		if(!S[0].equalsIgnoreCase(ax.getText())){
            			series[srcnt] = new XYSeries("");
            			scatterSeries[srcnt] = new XYSeries(S[0]);
            			bars[srcnt] = S[0];
            			srcnt++;
            		}
            	}
            	XYSeriesCollection data = new XYSeriesCollection();
            	XYSeriesCollection Scatterdata = new XYSeriesCollection();
            	DefaultCategoryDataset Catdataset = new DefaultCategoryDataset();
            	DefaultPieDataset piedataset = new DefaultPieDataset();
            	DefaultCategoryDataset Bardataset = new DefaultCategoryDataset();
	            //Read File Line By Line
	            try {       
	            	String[] vars = null;
	            	strLineArr = reader.readNext();	 
	            	if(first){
	            		vars = strLineArr;
  	            	}
	            	
					while ((strLineArr) != null) {
						if(!first){
							switch(choice){
							case 0: //piedataset = new DefaultPieDataset();  
								break;							
							case 1: //series = new XYSeries("first");
								break;
							case 3:// For Bar Chart
								break;
							default: System.out.println("Naa ho payega!");  
							}
							 
						}
						TableItem item = null;
			              
			              if(first){
			                  length = strLineArr.length;
			              }else{
			                   item = new TableItem (table, SWT.NONE);
			              }
			              
			              int thisLen = strLineArr.length;
			              int j = 0;
			              if(thisLen<=length){
			            	  int fir = 0;
			                  for(int i =0; i<thisLen; i++){
			              
			                      if(first){
			                    	  for(Iterator<String[]> it = columns.iterator(); it.hasNext();){
		                        		  String[] S = it.next();
		                        		  if(i == Integer.parseInt(S[1])){
		                        			  TableColumn column = new TableColumn (table, SWT.LEFT);
				                    		  column.setText(strLineArr[i]);			                          
				                    		  column.setWidth(100);
				                    		  column.pack();
				                    		  
		                        		  }
		                        	  }
			                    		
			                    	    //System.out.println(strLineArr[i]); 
			                      }else{
			                         
			                          //System.out.println("-- "+i+" -- " + strLineArr[i]);
			                    	  
			                    	  for(Iterator<String[]> it = columns.iterator(); it.hasNext();){
		                        		  String[] S = it.next();
		                        		  if(i == Integer.parseInt(S[1])){
		                        			  item.setText(j,strLineArr[i]);
		                        			  j++;
		                        		  }
		                        	  }
			                    	  
			                          if(i > 0){
			                        	  String xaxis = ax.getText();int cnt = 0;
			                        	  for(Iterator<String[]> it = columns.iterator(); it.hasNext();){
			                        		  String[] S = it.next();			                        		    
			                        		  if(xaxis.equalsIgnoreCase(S[0])){
			                        			  cnt = Integer.parseInt(S[1]);
			                        			  //System.out.println("What the hell "+ cnt);  
			                        			  break;
			                        		  }
			                        	  }
			                        	  
			                        	  for(Iterator<String[]> it = columns.iterator(); it.hasNext();){
			                        		  String[] S = it.next();			                        		  
			                        		  if(i == Integer.parseInt(S[1]) &&i!=cnt){
			                        			  
			                        			  /*yaxes.get(fir).add(Double.parseDouble(strLineArr[i]));
			                        			  if(!names.contains(S[0]))
			                        				  names.add(fir, S[0]); 
			                        			  fir++;
			                        			  if(isNumeric(strLineArr[cnt])){
			                        				  x_axis.add(Double.parseDouble(strLineArr[cnt]));
			                        				  numeric = true;
			                        			  }
			                        			  else
			                        				  category.add(strLineArr[cnt]);*/
			                        			  switch(choice){
			                        			  case 0: piedataset.setValue(strLineArr[cnt], Double.parseDouble(strLineArr[i]));
			                        			  	break;
			                        			  case 1:
			                        				  if(isNumeric(strLineArr[cnt])){
			                        					  series[fir].setKey(S[0]); 
			                        					  numeric = true;
			                        					  series[fir].add(Double.parseDouble(strLineArr[cnt]), Double.parseDouble(strLineArr[i]));//Double.parseDouble(strLineArr[cnt])
			                        					   
			                        				  }
			                        				  else
			                        					  Catdataset.addValue(Double.parseDouble(strLineArr[i]), vars[i], strLineArr[cnt]);
			                        			  	  break;
			                        			  case 3:
			                        				  	Bardataset.addValue(Double.parseDouble(strLineArr[i]), vars[i], strLineArr[cnt]);
			                        				  break;
			                        			  case 4:
			                        				  if(isNumeric(strLineArr[cnt])){
			                        					  numeric = true;
			                        					  scatterSeries[fir].add(Double.parseDouble(strLineArr[cnt]), Double.parseDouble(strLineArr[i]));//Double.parseDouble(strLineArr[cnt])
			                        				  }
			                        			  default: 
			                        			  }
			                        			  //System.out.println(vars[i] + " " +strLineArr[i]); 
			                        			  fir++;
			                        		  }
			                        		  
			                        	  }
			                        		  
			                          }
			                      }			                      
			                  }				                  
			                  //data.addSeries(series);
			                  System.out.println("firs are" +fir);
			              }
			              first = false;
			              strLineArr = reader.readNext();
			              
					}			
					
					for(int l = 0;l<columns.size()-1;l++){
						for(int i = 0; i<series[l].getItemCount(); i++){
							System.out.println(series[l].getDataItem(i).getYValue()); 
						}
						data.addSeries(series[l]);
						//scdata.addSeries(scSeries[l]);
						//bardata.addSeries(barSeries[l]);
					}
					XYDataset dataset = data;
					//XYDataset scDataset = scdata;
					//IntervalXYDataset BarDataset = bardata;
					CategoryDataset datasetcat = Catdataset;
					//CategoryDataset bardatasetcat = BarCatdataset;
					
					
					
					for(String S : names){
						System.out.println(S + "Muhahahahaha"); 
					}
					
					
					
					System.out.println("Number of fields for y axis is: "+ columns.size()); 
					final Composite graph = new Composite(tabFolder, SWT.NONE);
				    graph.setLayout(new GridLayout(1,false));
				    graph.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
				    graph.setBackground(new Color(null,255,255,255));
				    ite.setControl(graph);
				    
				    ToolBar tc = new ToolBar(graph,  SWT.FLAT | SWT.WRAP | SWT.RIGHT);
				    tc.setBackground(graph.getBackground());	    
					tc.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
					
					ToolItem itemSaveAs = new ToolItem(tc, SWT.PUSH);
				    itemSaveAs.setToolTipText("Print PDF");
				    Image icon = new Image(display, "D:\\Users\\703119704\\Downloads\\Flow-flow\\assembly\\package-res\\ui\\images\\saveas.png");//src/main/resources/plugin/
				    itemSaveAs.setImage(icon);
				    
				    
				    final Composite composite = new Composite(graph, SWT.NONE);
					composite.setLayout(new FillLayout(SWT.FILL));
					composite.setLayoutData(new GridData(GridData.FILL_BOTH));//SWT.FILL, SWT.FILL, true, true
					composite.setBackground(new Color(Display.getCurrent(),255,255,255));
					switch(choice){
					case 1:
						if(numeric){
							chart = ChartFactory.createXYLineChart(
					            "hahahaha",      // chart title
					            "X",                      // x axis label
					            "Y",                      // y axis label
					            dataset,                  // data
					            PlotOrientation.VERTICAL,
					            true,                     // include legend
					            true,                     // tooltips
					            false                     // urls
					        );}
							else{
							chart = ChartFactory.createLineChart(
						            "hahahaha",      // chart title
						            "X",             // x axis label
						            "Y",                      // y axis label
						            datasetcat,                  // data
						            PlotOrientation.VERTICAL, // orientation
						            true,                     // include legend
						            true,                     // tooltips
						            false                     // urls
						        );
							}
					        /*final XYPlot plot = chart.getXYPlot();
					        final XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
					        renderer.setSeriesLinesVisible(0, false);
					        renderer.setSeriesShapesVisible(1, false);
					        plot.setRenderer(renderer);
					        final NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
					        rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
					        //ChartPanel chartPanel = new ChartPanel(chart);											
					        */
							
							
							if(!numeric){
								final CategoryPlot plot = (CategoryPlot) chart.getPlot();
						        final NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
						        rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
						        rangeAxis.setAutoRangeIncludesZero(true);
								
								
						        
						        ChartComposite composite1 = new ChartComposite(graph, SWT.NONE,chart,true);
								composite1.setLayout(new FillLayout(SWT.FILL));
								composite1.setLayoutData(new GridData(GridData.FILL_BOTH));//SWT.FILL, SWT.FILL, true, true
								composite1.setBackground(new Color(null,255,0,0));
								composite1.setChart(chart);
								break;
	
							}
							else{
								final XYPlot plot =  chart.getXYPlot();
						        final NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
						        rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
						        rangeAxis.setAutoRangeIncludesZero(true);
								
								
																							   
							    
						        ChartComposite composite1 = new ChartComposite(graph, SWT.NONE,chart,true);
								composite1.setLayout(new FillLayout(SWT.FILL));
								composite1.setLayoutData(new GridData(GridData.FILL_BOTH));//SWT.FILL, SWT.FILL, true, true
								composite1.setBackground(new Color(null,255,0,0));
								composite1.setChart(chart);
								break;
	
							}
	
					case 3:
						
						
						IBarSeries[] BarSeries1 = new IBarSeries[columns.size() - 1];
						final Chart barchart = new Chart(composite, SWT.NONE);												
						
						barchart.getTitle().setText("Bar Chart");
						barchart.getTitle().setForeground(new Color(null,0,0,0));
						barchart.setBackground(new Color(null,255,255,255));
						barchart.setBackgroundInPlotArea(new Color(Display.getDefault(),200,200,200));
						
						ILegend legend = barchart.getLegend();
						legend.setPosition(SWT.BOTTOM);
						
						Random rand = new Random();
						for(int k = 0; k<columns.size()-1; k++){
							ArrayList<Double> temp = yaxes.get(k);
							Double[] yaxis = new Double[temp.size()];
							Double[] axis = new Double[x_axis.size()];
							String[] cat = new String[category.size()];
							yaxis = temp.toArray(yaxis);
							if(numeric){
								axis = x_axis.toArray(axis);
								
								System.out.println("xaxis:");
								for(Double S : axis){
									System.out.println(S);
								} 
								
								System.out.println("yaxis:");
								for(Double D : temp){
									System.out.println(D); 
								}
								barchart.getAxisSet().getXAxis(0).getTitle().setText("Series 1");
								barchart.getAxisSet().getXAxis(0).getTitle().setForeground(new Color(null,0,0,0));  
								barchart.getAxisSet().getYAxis(0).getTitle().setText("Series 2");
								barchart.getAxisSet().getYAxis(0).getTitle().setForeground(new Color(null,0,0,0));
								 
								
								// create scatter series
								System.out.println(names.get(k) + "WTF");
								BarSeries1[k] = (IBarSeries) barchart.getSeriesSet().createSeries(SeriesType.BAR, names.get(k)); 
								BarSeries1[k].setVisibleInLegend(true); 
								 
								BarSeries1[k].setXSeries(ArrayUtils.toPrimitive(axis)); 
								BarSeries1[k].setYSeries(ArrayUtils.toPrimitive(yaxis));
								int R = (int)(Math.random()*256);
								int G = (int)(Math.random()*256);
								int B= (int)(Math.random()*256);
								Color color = new Color(null, R, G, B); //random color, but can be bright or dull

								BarSeries1[k].setBarColor(color); 
								barchart.getAxisSet().adjustRange();
							}
							else{
								cat = category.toArray(cat);
								
								System.out.println("xaxis:");
								for(String S : cat){
									System.out.println(S);
								} 
								
								System.out.println("yaxis:");
								for(Double D : temp){
									System.out.println(D); 
								}
								barchart.getAxisSet().getXAxis(0).getTitle().setText("Series 1");
								barchart.getAxisSet().getYAxis(0).getTitle().setText("Series 2");
								barchart.getAxisSet().getYAxis(0).getTitle().setForeground(new Color(null,0,0,0));
								barchart.getAxisSet().getXAxis(0).enableCategory(true);
								barchart.getAxisSet().getXAxis(0).setCategorySeries(cat);
								// create scatter series
								BarSeries1[k] = (IBarSeries) barchart.getSeriesSet().createSeries(SeriesType.BAR, names.get(k));  
								//BarSeries1[k].setLineStyle(LineStyle.NONE);
								//scatterSeries1[k].setXSeries(cat); 
								BarSeries1[k].setYSeries(ArrayUtils.toPrimitive(yaxis));
								int R = (int)(Math.random()*256);
								int G = (int)(Math.random()*256);
								int B= (int)(Math.random()*256);
								Color color = new Color(null, R, G, B); //random color, but can be bright or dull

								BarSeries1[k].setBarColor(color); 
								barchart.getAxisSet().adjustRange();
							}
						}
						SaveAs.addSelectionListener(new SelectionListener(){

							@Override
							public void widgetDefaultSelected(SelectionEvent arg0) {
								// TODO Auto-generated method stub
								
							}

							@Override
							public void widgetSelected(SelectionEvent arg0) {

								//barchart.save("D:/Users/703119704/Desktop/fotu.jpg", SWT.IMAGE_JPEG);
								Point size = barchart.getSize();
						        //
								GC gc = new GC(barchart);
						        final Image image = new Image(display, size.x, size.y);
						        gc.copyArea(image, 0, 0);
						        gc.dispose();

						        ImageData data = image.getImageData();
						        //System.out.println(image); 
						        ImageLoader loader = new ImageLoader();
						        loader.data = new ImageData[] { data };
						        
						        loader.save("D:/Users/703119704/Desktop/fotu.jpg", SWT.IMAGE_PNG);
						        image.dispose();
							}
					    	
					    });
						
						break;
					
					case 4:
						
						ILineSeries[] scatterSeries1 = new ILineSeries[columns.size() - 1];
						Chart chart = new Chart(composite, SWT.NONE);												
						
						chart.getTitle().setText("Scatter Chart");
						
						
						
						for(int k = 0; k<columns.size()-1; k++){
							ArrayList<Double> temp = yaxes.get(k);
							Double[] yaxis = new Double[temp.size()];
							Double[] axis = new Double[x_axis.size()];
							String[] cat = new String[category.size()];
							yaxis = temp.toArray(yaxis);
							if(numeric){
								axis = x_axis.toArray(axis);
								
								System.out.println("xaxis:");
								for(Double S : axis){
									System.out.println(S);
								} 
								
								System.out.println("yaxis:");
								for(Double D : temp){
									System.out.println(D); 
								}
								chart.getAxisSet().getXAxis(0).getTitle().setText("Series 1");
								chart.getAxisSet().getYAxis(0).getTitle().setText("Series 2");
								// create scatter series
								scatterSeries1[k] = (ILineSeries) chart.getSeriesSet().createSeries(SeriesType.LINE, Integer.toString(k)); 
								scatterSeries1[k].setLineStyle(LineStyle.NONE);
								scatterSeries1[k].setXSeries(ArrayUtils.toPrimitive(axis)); 
								scatterSeries1[k].setYSeries(ArrayUtils.toPrimitive(yaxis));
								scatterSeries1[k].setSymbolColor(new Color(null,255,255*k,0)); 
								chart.getAxisSet().adjustRange();
							}
							else{
								cat = category.toArray(cat);
								
								System.out.println("xaxis:");
								for(String S : cat){
									System.out.println(S);
								} 
								
								System.out.println("yaxis:");
								for(Double D : temp){
									System.out.println(D); 
								}
								chart.getAxisSet().getXAxis(0).getTitle().setText("Series 1");
								chart.getAxisSet().getYAxis(0).getTitle().setText("Series 2");
								chart.getAxisSet().getXAxis(0).enableCategory(true);
								chart.getAxisSet().getXAxis(0).setCategorySeries(cat);
								// create scatter series
								scatterSeries1[k] = (ILineSeries) chart.getSeriesSet().createSeries(SeriesType.LINE, Integer.toString(k)); 
								scatterSeries1[k].setLineStyle(LineStyle.NONE);
								//scatterSeries1[k].setXSeries(cat); 
								scatterSeries1[k].setYSeries(ArrayUtils.toPrimitive(yaxis));
								scatterSeries1[k].setSymbolColor(new Color(null,255,255*k,0)); 
								chart.getAxisSet().adjustRange();
							}
							
							
						}
						break;
						default:
							System.out.println("Beta Tumse Naa Ho Paayega!!");
							break;
					}
					
					
					/*System.out.println(series[0].getAutoSort());   
					
					for(int l = 0;l<columns.size()-1;l++){
						data.addSeries(series[l]);
						Scatterdata.addSeries(scatterSeries[l]);
					}
					XYDataset xydataset = data;
					XYDataset scatterdataset = Scatterdata;
					CategoryDataset dataset = Catdataset;
					CategoryDataset datasetBar = Bardataset;
					System.out.println("dataset "+datasetBar.getRowCount());  
					switch(choice){
					case 1: 
						JFreeChart chart = null;
 						if(numeric){
						chart = ChartFactory.createLineChart(
				            "Line Chart Demo 6",      // chart title
				            ax.getText(),             // x axis label
				            "Y",                      // y axis label
				            dataset,                  // data
				            PlotOrientation.VERTICAL, // orientation
				            true,                     // include legend
				            true,                     // tooltips
				            false                     // urls
				        );
						chart = ChartFactory.createXYLineChart(
				            "Line Chart Demo 6",      // chart title
				            ax.getText(),             // x axis label
				            "Y",                      // y axis label
				            xydataset,                  // data
				            PlotOrientation.VERTICAL, // orientation
				            true,                     // include legend
				            true,                     // tooltips
				            false                     // urls
				        );
						}
				        
				        if(!numeric){
									final CategoryPlot plot = (CategoryPlot) chart.getPlot();
							        final NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
							        rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
							        rangeAxis.setAutoRangeIncludesZero(true);
									
									
							        final Composite graph = new Composite(tabFolder, SWT.NONE);
								    graph.setLayout(new GridLayout(1,false));
								    graph.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));	
								    ite.setControl(graph);
							        ChartComposite composite = new ChartComposite(graph, SWT.NONE,chart,true);
									composite.setLayout(new FillLayout(SWT.FILL));
									composite.setLayoutData(new GridData(GridData.FILL_BOTH));//SWT.FILL, SWT.FILL, true, true
									composite.setBackground(new Color(null,255,0,0));
									composite.setChart(chart);
									break;
		
								}
								else{
									final XYPlot plot =  chart.getXYPlot();
							        final NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
							        rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
							        rangeAxis.setAutoRangeIncludesZero(true);
									
									
							        final Composite graph = new Composite(tabFolder, SWT.NONE);
								    graph.setLayout(new GridLayout(1,false));
								    graph.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));	
								    ite.setControl(graph);
							        ChartComposite composite = new ChartComposite(graph, SWT.NONE,chart,true);
									composite.setLayout(new FillLayout(SWT.FILL));
									composite.setLayoutData(new GridData(GridData.FILL_BOTH));//SWT.FILL, SWT.FILL, true, true
									composite.setBackground(new Color(null,255,0,0));
									composite.setChart(chart);
									break;
		
								}
					case 0: JFreeChart piechart = ChartFactory.createPieChart("Programmer Population", piedataset, true, true, false);
							PiePlot plotpi = (PiePlot) piechart.getPlot();
							plotpi.setCircular(true);
							final Composite pigraph = new Composite(tabFolder, SWT.NONE);
						    pigraph.setLayout(new GridLayout(1,false));
						    pigraph.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));	
						    ite.setControl(pigraph);
					        ChartComposite picomposite = new ChartComposite(pigraph, SWT.NONE,piechart,true);
							picomposite.setLayout(new FillLayout(SWT.FILL));
							picomposite.setLayoutData(new GridData(GridData.FILL_BOTH));//SWT.FILL, SWT.FILL, true, true
							picomposite.setBackground(new Color(null,255,0,0));
							picomposite.setChart(piechart);
							break;
							
					case 3: 
						JFreeChart Barchart = ChartFactory.createStackedBarChart(
				            "Bar Chart Demo",         // chart title
				            ax.getText(),               // domain axis label
				            "Value",                  // range axis label
				            datasetBar,                  // data
				            PlotOrientation.VERTICAL, // orientation
				            true,                     // include legend
				            true,                     // tooltips?
				            false                     // URLs?
				        );
						
						final CategoryPlot plot = (CategoryPlot) Barchart.getPlot();
						
				        
				        StackedBarRenderer renderer = (StackedBarRenderer) plot.getRenderer();
				        renderer.setDrawBarOutline(false);				        				       
						
						final Composite bargraph = new Composite(tabFolder, SWT.NONE);
					    bargraph.setLayout(new GridLayout(1,false));
					    bargraph.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));	
					    ite.setControl(bargraph);
				        ChartComposite barcomposite = new ChartComposite(bargraph, SWT.NONE,Barchart,true);
						barcomposite.setLayout(new FillLayout(SWT.FILL));
						barcomposite.setLayoutData(new GridData(GridData.FILL_BOTH));//SWT.FILL, SWT.FILL, true, true
						barcomposite.setBackground(new Color(null,255,0,0));
						barcomposite.setChart(Barchart);
						break;
						
					case 4:
						JFreeChart scchart = ChartFactory.createScatterPlot(
					            "Line Chart Demo 6",      // chart title
					            ax.getText(),             // x axis label
					            "Y",                      // y axis label
					            scatterdataset,                  // data
					            PlotOrientation.VERTICAL, // orientation
					            true,                     // include legend
					            true,                     // tooltips
					            false                     // urls
					        );
						final XYPlot scplot =  scchart.getXYPlot();
				        final NumberAxis rangeAxis = (NumberAxis) scplot.getRangeAxis();
				        rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
				        rangeAxis.setAutoRangeIncludesZero(true);
						
						
				        final Composite graph = new Composite(tabFolder, SWT.NONE);
					    graph.setLayout(new GridLayout(1,false));
					    graph.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));	
					    ite.setControl(graph);
				        ChartComposite composite = new ChartComposite(graph, SWT.NONE,scchart,true);
						composite.setLayout(new FillLayout(SWT.FILL));
						composite.setLayoutData(new GridData(GridData.FILL_BOTH));//SWT.FILL, SWT.FILL, true, true
						composite.setBackground(new Color(null,255,0,0));
						composite.setChart(scchart);
						break;
						
					}*/
					
					
						
						//chart = null;
				} 
	            catch (IOException e1) {
					e1.printStackTrace();
				}
            }
	            
        }
	            
		
		/*Display display = new Display();
		Shell shell = new Shell(display);
		
		System.out.println("Working Directory = " + System.getProperty("user.dir"));
		String current = new java.io.File( "." ).getCanonicalPath();
		current = current.replaceAll("\\\\", "/");
        System.out.println("Current dir: "+current);   
		
		Image im = new Image(display,current+"/open.png");//src/main/resources/plugin/
		if(im != null)
			shell.setImage(im);
		
		shell.open();
		while (!shell.isDisposed()) {
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }*/
		
		
	}
	public static boolean isNumeric(String str)  
	  {  
	    try  
	    {  
	      double d = Double.parseDouble(str);  
	    }  
	    catch(NumberFormatException nfe)  
	    {  
	      return false;  
	    }  
	    return true;  
	  }
	
}