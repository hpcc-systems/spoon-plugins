package org.pentaho.di.plugins.perspectives.eclmyperspective;



import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.StringTokenizer;

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
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PiePlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.general.DefaultPieDataset;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.experimental.chart.swt.ChartComposite;
import org.pentaho.di.core.EngineMetaInterface;
import org.pentaho.di.core.gui.SpoonFactory;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.SpoonPerspective;
import org.pentaho.di.ui.spoon.SpoonPerspectiveListener;
import org.pentaho.ui.xul.XulOverlay;
import org.pentaho.ui.xul.impl.DefaultXulOverlay;
import org.pentaho.ui.xul.impl.XulEventHandler;
import org.swtchart.Chart;
import org.swtchart.IBarSeries;
import org.swtchart.ILegend;
import org.swtchart.ISeries.SeriesType;
import org.w3c.dom.Document;

import au.com.bytecode.opencsv.CSVReader;

public class ECLMySwtPerspective implements SpoonPerspective {

  private Composite comp = null;
  private static ECLMySwtPerspective instance = new ECLMySwtPerspective();
  
  private Display display;
  private String fileName;
  private CTabFolder folder;
  //private XYDataset dataset;
  private Shell parentShell;
  private String fileName1 = "";
  private Label lbl;
  private boolean isActive;
  private boolean numeric;
  CTabItem ite;
  private File file;
  private JFreeChart chart, piechart, scchart;// barChart; 
  private Chart barchart;
private String[] filePath; 
  
  public void setFileName(String fn){
      fileName = fn;
      lbl.setText("Data returned from HPCC " + fileName);

  }
  public String getFileName(){
      return fileName;
  }
  
  private ECLMySwtPerspective(){  
   System.out.println("create eclResults ECLResultsSwtPerspective");
    //createUI();
   //getUI();
  }

  /*private Composite createUI(){
	  
	  parentShell = ((Spoon) SpoonFactory.getInstance()).getShell();
	    
	  display = parentShell.getDisplay();

	  comp = new Composite(((Spoon) SpoonFactory.getInstance()).getShell(), SWT.BORDER);
	  comp.setLayout(new GridLayout());
	  comp.setLayoutData(new GridData(GridData.FILL_BOTH));
	  
	  //CTabItem tab2 = new CTabItem(folder, SWT.NONE);
    	//tab2.setText("jus' checkin'");
	  
		folder = new CTabFolder(comp, SWT.CLOSE);
		folder.setSimple(false);
		folder.setBorderVisible(true);
		folder.setLayoutData(new GridData(GridData.FILL_BOTH));
    	//Label lbl = new Label(folder, SWT.NONE);
    	//lbl.setText("hjgghjghjg");
		return comp;
  }
*/
  public void buildgui(final String fileName) throws Exception{
	  String file = ""; final ArrayList<String[]> columns = new ArrayList<String[]>();
	  final ArrayList<String> map = new ArrayList<String>();

	  CTabItem wuidTab = new CTabItem(folder, SWT.NONE);
		wuidTab.setText("Report");
		
		folder.setSelection(folder.indexOf(wuidTab));
		 System.out.println("BUILDTAB--------" + folder.indexOf(wuidTab));
		Composite tabHolder = new Composite(folder, SWT.NONE);
		tabHolder.setLayout(new GridLayout(1,false));
		tabHolder.setLayoutData(new GridData(GridData.FILL_BOTH));
		//tabHolder.setBackground(new Color(null,255,255,255)); 
		//Label lbl = new Label(tabHolder, SWT.NONE);
		//lbl.setText(fileName);
		//lbl.setLayoutData(new GridData(GridData.FILL));
		
		
		
		/*ToolBar toolBar = new ToolBar(tabHolder, SWT.FLAT | SWT.WRAP | SWT.RIGHT);
	    
	    ToolItem itemPush = new ToolItem(toolBar, SWT.PUSH);
	    itemPush.setToolTipText("Open");
	    Image icon1 = new Image(tabHolder.getDisplay(), "./ui/images/open.png");
	    itemPush.setImage(icon1);
	    itemPush.addListener(SWT.Selection, new Listener(){

			@Override
			public void handleEvent(Event arg0) {
				Shell sh = new Shell(display);
				DirectoryDialog dialog = new DirectoryDialog(sh); 
			    dialog.setFilterPath("c:\\"); 
				
			}
	    	
	    });
	    
	    ToolItem itemSave = new ToolItem(toolBar, SWT.PUSH);
	    itemSave.setToolTipText("Save As PDF");
	    Image icon2 = new Image(tabHolder.getDisplay(), "./ui/images/save.png");//src/main/resources/plugin/
	    itemSave.setImage(icon2);	    	    	   
	    
	    toolBar.pack();
	    toolBar.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
	    
	    Label Sep = new Label(tabHolder, SWT.SEPARATOR | SWT.HORIZONTAL);	    
		Sep.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));*/
		
		Color color = parentShell.getBackground();
		parentShell.setBackground(new Color(null,255,255,255));
		
		final Composite comp2 = new Composite(tabHolder, SWT.NONE);		
		comp2.setLayout(new GridLayout(3,false));
		comp2.setLayoutData(new GridData(GridData.FILL_BOTH));
		comp2.setBackground(new Color(null,255,255,255));
		
		
		
		
		/**/
		
		Composite three = new Composite(comp2, SWT.NONE | SWT.BORDER);
		three.setLayout(new GridLayout(1,false));
		three.setLayoutData(new GridData(GridData.FILL_BOTH));//
		three.setBackground(new Color(null,188,188,188));
		
		Composite t1 = new Composite(three, SWT.NONE);
		t1.setLayout(new GridLayout(3,false));
		t1.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
		t1.setBackground(three.getBackground());
		
		final Label tL = new Label(t1, SWT.NONE);
		tL.setText("Unsaved Report");
		tL.setFont(new Font(display,"Arial", 18, SWT.BOLD ));
		tL.setBackground(three.getBackground());
		
		Label tl2 = new Label(t1, SWT.NONE);
		tl2.setText("												");
		tl2.setBackground(three.getBackground());
		tl2.setLayoutData(new GridData(GridData.FILL));
		
		ToolBar tc = new ToolBar(t1,  SWT.FLAT | SWT.WRAP | SWT.RIGHT);
	    tc.setBackground(t1.getBackground());	    
		tc.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
		
		ToolItem itemPdf = new ToolItem(tc, SWT.PUSH);
	    itemPdf.setToolTipText("Print PDF");
	    Image icon = new Image(display, "./ui/images/print.png");//src/main/resources/plugin/
	    itemPdf.setImage(icon);
	    
		/*Composite Graph = new Composite(three, SWT.NONE | SWT.BORDER);
		Graph.setLayout(new GridLayout(1,false));
		Graph.setLayoutData(new GridData(GridData.FILL_BOTH));
		Graph.setBackground(new Color(null,255,255,255));
		*/
		
		

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
				
				ToolBar toolBar1 = new ToolBar(threeClose, SWT.FLAT | SWT.WRAP | SWT.RIGHT);
			    toolBar1.setBackground(three1.getBackground());
			    toolBar1.setLayoutData(new GridData(GridData.CENTER));
				
			    
			    
			    ToolItem itemPush1 = new ToolItem(toolBar1, SWT.PUSH);
			    itemPush1.setToolTipText("Close");
			    Image icon11 = new Image(display, "./ui/images/close-panel.png");//src/main/resources/plugin/
			    itemPush1.setImage(icon11);
			    
			    
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
			    
				final Tree tree = new Tree(three1, SWT.V_SCROLL | SWT.BORDER);	    		
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
	    L4.setBackground(parentShell.getDisplay().getSystemColor(SWT.COLOR_GRAY));
	    // Set the background gradient
	    L4.setBackground(new Color[] { parentShell.getDisplay().getSystemColor(SWT.COLOR_GRAY),
	    		parentShell.getDisplay().getSystemColor(SWT.COLOR_CYAN),
	    		parentShell.getDisplay().getSystemColor(SWT.COLOR_BLUE) }, new int[] { 100, 100 });
		
	    
		
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
		Document doc = (Document) dBuilder.parse(fileName);
		doc.getDocumentElement().normalize();
		String[] file1 = null;// = "";
		
		if(doc.getElementsByTagName("result") != null){
			int i = 0;
			file1 = new String[doc.getElementsByTagName("result").getLength()];			
			filePath = new String[doc.getElementsByTagName("result").getLength()];
			while(i < doc.getElementsByTagName("result").getLength()){
				file1[i] =  doc.getElementsByTagName("result").item(i).getTextContent();
				filePath[i] = doc.getElementsByTagName("result").item(i).getTextContent();
				String[] S = file1[i].split("\\\\");
				file1[i] = S[S.length-1].split("_")[2]; 
				if(file1[i].length()>15)
					file1[i] = file1[i].substring(0, 15) + "...\n";
				i++;
			}
			
		}		
		c1.setItems(file1);
        c1.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
        
        c1.addModifyListener(new ModifyListener(){
        	
            public void modifyText(ModifyEvent e){
            	lbl.setText("  "+c1.getText());               	
            	int idx = c1.getSelectionIndex(); 
            	
            	fileName1 = filePath[idx];
            	System.out.println(fileName1); 
            	tree.setItemCount(0);
            	CSVReader reader = null;
				try {
					reader = new CSVReader(new FileReader(fileName1),',','"');
				} catch (FileNotFoundException e2) {
					// TODO Auto-generated catch block
					e2.printStackTrace();
				}
                String [] strLineArr;
                
                // Open the file that is the first 
                // command line parameter
                FileInputStream fstream = null;
				try {
					fstream = new FileInputStream(fileName1);
				} catch (FileNotFoundException e2) {
					// TODO Auto-generated catch block
					e2.printStackTrace();
				}
                // Get the object of DataInputStream
                DataInputStream in = new DataInputStream(fstream);
                BufferedReader br = new BufferedReader(new InputStreamReader(in));
                System.out.println(br); 

                
                
                
                //Read File Line By Line
                try {
					while ((strLineArr = reader.readNext()) != null) {
						for(int i = 0; i<strLineArr.length; i++){
							TreeItem treeItem0 = new TreeItem(tree, 0);
		        	    	treeItem0.setText(strLineArr[i]);
						}						
						break;					    
					}
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
        cx.setItems(new String[]{"Piechart", "Linechart", "Barchart", "Scatterchart"});
		cx.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
		
		
		Label gr1 = new Label(visualGroup, SWT.NONE);
        gr1.setText("X-Axis:");
        
        final Combo cx1 = new Combo(visualGroup, SWT.NONE);
        //cx1.setItems(new String[]{"Sum", "Ave", "Variance", "Add", "Subtract", "Multiply", "Divide", "Mod"});
		cx1.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
		
		Label gr2 = new Label(visualGroup, SWT.NONE);
        gr2.setText("Chart Name");
        
        final Text cx2 = new Text(visualGroup, SWT.BORDER);        
		cx2.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
		
		
		Group tabGroup = new Group(three2, SWT.SHADOW_NONE);        
        tabGroup.setText("Drag 'n Drop ");
        //generalGroup.setSize(200,700);
        tabGroup.setLayoutData(new GridData(GridData.FILL_BOTH));
        tabGroup.setLayout(new GridLayout(1,false));
        
		final Tree tab1 = new Tree(tabGroup, SWT.CHECK | SWT.V_SCROLL | SWT.BORDER);	    		
	    tab1.setLayoutData(new GridData(GridData.FILL_BOTH));
	    tab1.getVerticalBar();
	    tab1.setHeaderVisible(true);
	    
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
	          cx1.setItems(items);
	        }
	      }
	    });
	    
	    B1.addListener(SWT.Selection, new Listener(){

			@Override
			public void handleEvent(Event arg0) {
				int cnt = 0;
				for(int i = 0; i<tab1.getItemCount(); i++){
					if(tab1.getItem(Math.abs(i-cnt)).getChecked()){
						map.remove(Math.abs(i-cnt));
						columns.remove(Math.abs(i-cnt));
						tab1.getItem(Math.abs(i-cnt)).dispose(); 
						cnt = cnt + 1;
					}
				}
				String items[] = new String[tab1.getItemCount()];
		          for(int i = 0; i<tab1.getItemCount(); i++){
		        	  items[i] = tab1.getItem(i).getText();
		          }
		          cx1.setItems(items);
			}
	    	
	    });
	    
	    final CTabFolder tabFolder = new CTabFolder(three, SWT.CLOSE);
	    tabFolder.setSimple(false);
	    tabFolder.setBackground(color);
	    
	    ite = new CTabItem(tabFolder, SWT.NONE);
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
				ite = new CTabItem(tabFolder, SWT.NONE);
			    ite.setText("GraphTab");
			    ite.setToolTipText("GraphTab");
			    final Composite graphmain = new Composite(tabFolder, SWT.NONE);
			    graphmain.setLayout(new GridLayout(1,false));
			    graphmain.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));	    
			    ite.setControl(graphmain);
			    
				int choice = cx.getSelectionIndex();
				if(table != null){
					System.out.println(columns.size()); 
					table.setItemCount(0);
					table.setRedraw(false);
					while ( table.getColumnCount() > 0 ) {
					    table.getColumns()[ 0 ].dispose();
					}
					table.setRedraw(true);			
				}
				if(!fileName1.equals("")){
					System.out.println(fileName1); 
					
					CSVReader reader = null;
					try {
						reader = new CSVReader(new FileReader(fileName1),',','"');
					} catch (FileNotFoundException e2) {
						e2.printStackTrace();
					}
		            String [] strLineArr;
		            
		            // Open the file that is the first 
		            // command line parameter
		            FileInputStream fstream = null;
					try {
						fstream = new FileInputStream(fileName);
					} catch (FileNotFoundException e2) {
						e2.printStackTrace();
					}
		            // Get the object of DataInputStream
		            DataInputStream in = new DataInputStream(fstream);
		            BufferedReader br = new BufferedReader(new InputStreamReader(in));
		            System.out.println(br); 
		            int length = 0;
		            boolean first = true;
		            boolean header = false;
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
		            	XYSeries[] scSeries = new XYSeries[columns.size()];
		            	XYSeries[] barSeries = new XYSeries[columns.size()];
		            	int srcnt = 0;
		            	for(Iterator<String[]> it = columns.iterator(); it.hasNext();){
		            		String[] S = (String[]) it.next();
		            		if(!S[0].equalsIgnoreCase(cx1.getText())){
		            			series[srcnt] = new XYSeries("");
		            			scSeries[srcnt] = new XYSeries("");
		            			barSeries[srcnt] = new XYSeries(S[0]);
		            			srcnt++;		            			
		            		}
		            	}
		            	XYSeriesCollection data = new XYSeriesCollection();
		            	XYSeriesCollection scdata = new XYSeriesCollection();
		            	XYSeriesCollection bardata = new XYSeriesCollection();
		            	DefaultCategoryDataset Catdataset = new DefaultCategoryDataset();
		            	//DefaultCategoryDataset BarCatdataset = new DefaultCategoryDataset();
		            	DefaultPieDataset piedataset = new DefaultPieDataset();
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
									case 2:
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
					                        	  String xaxis = cx1.getText();int cnt = 0;
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
					                        		  if(i == Integer.parseInt(S[1]) && i != cnt){
					                        			  switch(choice){
					                        			  case 0: piedataset.setValue(strLineArr[cnt], Double.parseDouble(strLineArr[i]));
					                        			  	break;
					                        			  case 1:
					                        				  if(isNumeric(strLineArr[cnt])){
					                        					  series[fir].setKey(S[0]);
					                        					  numeric = true;
						                        				  series[fir].add(Double.parseDouble(strLineArr[cnt]), Double.parseDouble(strLineArr[i]));
					                        				  }
					                        				  else
					                        					  Catdataset.addValue(Double.parseDouble(strLineArr[i]), vars[i], strLineArr[cnt]);
					                        				  
					                        			  	  break;
					                        			  	  
					                        			  case 2:
					                        				  yaxes.get(fir).add(Double.parseDouble(strLineArr[i]));
					                        				  if(!names.contains(S[0]))
						                        				  names.add(fir, S[0]); 
						                        			  //fir++;
						                        			  if(isNumeric(strLineArr[cnt])){
						                        				  x_axis.add(Double.parseDouble(strLineArr[cnt]));
						                        				  numeric = true;
						                        			  }
						                        			  else
						                        				  category.add(strLineArr[cnt]);
					                        				  /*if(isNumeric(strLineArr[cnt])){
					                        					  numeric = true;
						                        				  barSeries[fir].add(Double.parseDouble(strLineArr[cnt]), Double.parseDouble(strLineArr[i]));
					                        				  }
					                        				  else
					                        					  BarCatdataset.addValue(Double.parseDouble(strLineArr[i]), vars[i], strLineArr[cnt]);*/
					                        				  break;
					                        			  case 3:
					                        				  if(isNumeric(strLineArr[cnt])){
					                        					  scSeries[fir].setKey(S[0]);
					                        					  numeric = true;
						                        				  scSeries[fir].add(Double.parseDouble(strLineArr[cnt]), Double.parseDouble(strLineArr[i]));
					                        				  }
					                        				  break;
					                        			  default: 
					                        			  }
					                        			  System.out.println(strLineArr[i]);
					                        			  fir++;
					                        		  }
					                        		 
					                        	  }
					                        		  
					                          }
					                      }			                      
					                  } // the i loop ends here				                  
					                  
					              }
					              first = false;
					              strLineArr = reader.readNext();
					              
							}					
							for(int l = 0;l<columns.size()-1;l++){
								data.addSeries(series[l]);
								scdata.addSeries(scSeries[l]);
								bardata.addSeries(barSeries[l]);
							}
							XYDataset dataset = data;
							XYDataset scDataset = scdata;
							//IntervalXYDataset BarDataset = bardata;
							CategoryDataset datasetcat = Catdataset;
							//CategoryDataset bardatasetcat = BarCatdataset;
							
							System.out.println("dataset "+dataset);  
							
							String chartName = "MyChart";
							if(cx2.getText().equals(""))
								chartName = "MyChart";
							else
								chartName = cx2.getText();
							
							switch(choice){
							
							case 1:
								
								//JFreeChart chart;
								if(numeric){
								chart = ChartFactory.createXYLineChart(
						            chartName,      // chart title
						            cx1.getText(),                      // x axis label
						            "Y",                      // y axis label
						            dataset,                  // data
						            PlotOrientation.VERTICAL,
						            true,                     // include legend
						            true,                     // tooltips
						            false                     // urls
						        );}
								else{
								chart = ChartFactory.createLineChart(
							            chartName,      // chart title
							            cx1.getText(),             // x axis label
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
									
									
							        final Composite graph = new Composite(tabFolder, SWT.NONE);
								    graph.setLayout(new GridLayout(1,false));
								    graph.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));	
								    ite.setControl(graph);
								    
								    ToolBar tc = new ToolBar(graph,  SWT.FLAT | SWT.WRAP | SWT.RIGHT);
								    tc.setBackground(graph.getBackground());	    
									tc.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
									
									ToolItem itemSave = new ToolItem(tc, SWT.PUSH);
								    itemSave.setToolTipText("Save");
								    Image icon = new Image(display, "./ui/images/save.png");//src/main/resources/plugin/
								    itemSave.setImage(icon);								    
									ToolItem itemSaveAs = new ToolItem(tc, SWT.PUSH);
								    itemSaveAs.setToolTipText("Save As");
								    icon = new Image(display, "./ui/images/saveas.png");//src/main/resources/plugin/
								    itemSaveAs.setImage(icon);
								    
								    saveas(itemSaveAs, chart, null, ite);
								    save(itemSave, chart, null, ite);
								    
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
								    ToolBar tc = new ToolBar(graph,  SWT.FLAT | SWT.WRAP | SWT.RIGHT);
								    tc.setBackground(graph.getBackground());	    
									tc.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
									
									
								    ToolItem itemSave = new ToolItem(tc, SWT.PUSH);
								    itemSave.setToolTipText("Save");
								    Image icon = new Image(display, "./ui/images/save.png");//src/main/resources/plugin/
								    itemSave.setImage(icon);								    
									ToolItem itemSaveAs = new ToolItem(tc, SWT.PUSH);
								    itemSaveAs.setToolTipText("Save As");
								    icon = new Image(display, "./ui/images/saveas.png");//src/main/resources/plugin/
								    itemSaveAs.setImage(icon);
								    				  
								    saveas(itemSaveAs, chart, null, ite);
								    save(itemSave, chart, null, ite);
								    
							        ChartComposite composite = new ChartComposite(graph, SWT.NONE,chart,true);
									composite.setLayout(new FillLayout(SWT.FILL));
									composite.setLayoutData(new GridData(GridData.FILL_BOTH));//SWT.FILL, SWT.FILL, true, true
									composite.setBackground(new Color(null,255,0,0));
									composite.setChart(chart);
									break;
		
								}
							case 0: piechart = ChartFactory.createPieChart(chartName, piedataset, true, true, false);
									PiePlot plotpi = (PiePlot) piechart.getPlot();
									plotpi.setCircular(true);
									final Composite pigraph = new Composite(tabFolder, SWT.NONE);
								    pigraph.setLayout(new GridLayout(1,false));
								    pigraph.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));	
								    ite.setControl(pigraph);
								    ToolBar tc = new ToolBar(pigraph,  SWT.FLAT | SWT.WRAP | SWT.RIGHT);
								    tc.setBackground(pigraph.getBackground());	    
									tc.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
									
								    
								    ToolItem itemSave = new ToolItem(tc, SWT.PUSH);
								    itemSave.setToolTipText("Save");
								    Image icon = new Image(display, "./ui/images/save.png");//src/main/resources/plugin/
								    itemSave.setImage(icon);								    
									ToolItem itemSaveAs = new ToolItem(tc, SWT.PUSH);
								    itemSaveAs.setToolTipText("Save As");
								    icon = new Image(display, "./ui/images/saveas.png");//src/main/resources/plugin/
								    itemSaveAs.setImage(icon);
								    
								    saveas(itemSaveAs, piechart, null, ite);
								    save(itemSave, piechart, null, ite);
								    
							        ChartComposite picomposite = new ChartComposite(pigraph, SWT.NONE,piechart,true);
									picomposite.setLayout(new FillLayout(SWT.FILL));
									picomposite.setLayoutData(new GridData(GridData.FILL_BOTH));//SWT.FILL, SWT.FILL, true, true
									picomposite.setBackground(new Color(null,255,0,0));
									picomposite.setChart(piechart);
									
									
									
									break;
									
							case 3: {
								scchart = ChartFactory.createScatterPlot(
							            chartName,      // chart title
							            cx1.getText(),                      // x axis label
							            "Y",                      // y axis label
							            scDataset,                  // data
							            PlotOrientation.VERTICAL,
							            true,                     // include legend
							            true,                     // tooltips
							            false                     // urls
							        );
								
								final XYPlot plot =  scchart.getXYPlot();
						        final NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
						        rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
						        rangeAxis.setAutoRangeIncludesZero(true);
								
								
						        final Composite graph = new Composite(tabFolder, SWT.NONE);
							    graph.setLayout(new GridLayout(1,false));
							    graph.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));	
							    ite.setControl(graph);
							    
							    tc = new ToolBar(graph,  SWT.FLAT | SWT.WRAP | SWT.RIGHT);
							    tc.setBackground(graph.getBackground());	    
								tc.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
								
							    
							    itemSave = new ToolItem(tc, SWT.PUSH);
							    itemSave.setToolTipText("Save");
							    icon = new Image(display, "./ui/images/save.png");//src/main/resources/plugin/
							    itemSave.setImage(icon);								    
								itemSaveAs = new ToolItem(tc, SWT.PUSH);
							    itemSaveAs.setToolTipText("Save As");
							    icon = new Image(display, "./ui/images/saveas.png");//src/main/resources/plugin/
							    itemSaveAs.setImage(icon);
							    
							    saveas(itemSaveAs, scchart, null, ite);
							    save(itemSave, scchart, null, ite);
							    
						        ChartComposite composite = new ChartComposite(graph, SWT.NONE,scchart,true);
								composite.setLayout(new FillLayout(SWT.FILL));
								composite.setLayoutData(new GridData(GridData.FILL_BOTH));//SWT.FILL, SWT.FILL, true, true
								composite.setBackground(new Color(null,255,0,0));
								composite.setChart(scchart);
								break;
							}
							case 2:
								
								final Composite bargraph = new Composite(tabFolder, SWT.NONE);
							    bargraph.setLayout(new GridLayout(1,false));
							    bargraph.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));	
							    ite.setControl(bargraph);
							    						
							    
							    
							    tc = new ToolBar(bargraph,  SWT.FLAT | SWT.WRAP | SWT.RIGHT);
							    tc.setBackground(bargraph.getBackground());	    
								tc.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
								
							    itemSave = new ToolItem(tc, SWT.PUSH);
							    itemSave.setToolTipText("Save");
							    icon = new Image(display, "./ui/images/save.png");//src/main/resources/plugin/
							    itemSave.setImage(icon);								    
								itemSaveAs = new ToolItem(tc, SWT.PUSH);
							    itemSaveAs.setToolTipText("Save As");
							    icon = new Image(display, "./ui/images/saveas.png");//src/main/resources/plugin/
							    itemSaveAs.setImage(icon);
							    
							    Composite barcomposite = new Composite(bargraph, SWT.NONE);
								barcomposite.setLayout(new FillLayout(SWT.FILL));
								barcomposite.setLayoutData(new GridData(GridData.FILL_BOTH));//SWT.FILL, SWT.FILL, true, true
								barcomposite.setBackground(new Color(null,255,255,255));
								IBarSeries[] BarSeries1 = new IBarSeries[columns.size() - 1];
								barchart = new Chart(barcomposite, SWT.NONE);												
								
								String barchartName = "MyChart";
								if(cx2.getText().equals(""))
									barchartName = "MyChart";
								else
									barchartName = cx2.getText();
								barchart.getTitle().setText(barchartName);
								barchart.getTitle().setForeground(new Color(null,0,0,0));
								barchart.setBackground(new Color(null,255,255,255));
								barchart.setBackgroundInPlotArea(new Color(null,205,205,205));
								
								ILegend legend = barchart.getLegend();
								legend.setPosition(SWT.BOTTOM);
								
								//Random rand = new Random();
								
								for(int k = 0; k<columns.size()-1; k++){
									ArrayList<Double> temp = yaxes.get(k);
									Double[] yaxis = new Double[temp.size()];
									Double[] axis = new Double[x_axis.size()];
									String[] cat = new String[category.size()];
									yaxis = temp.toArray(yaxis);
									if(numeric){
										axis = x_axis.toArray(axis);																			
										barchart.getAxisSet().getXAxis(0).getTitle().setText(cx1.getText());
										barchart.getAxisSet().getXAxis(0).getTitle().setForeground(new Color(null,0,0,0));
										barchart.getAxisSet().getYAxis(0).getTitle().setText("Y");
										barchart.getAxisSet().getYAxis(0).getTitle().setForeground(new Color(null,0,0,0));
										
										BarSeries1[k] = (IBarSeries) barchart.getSeriesSet().createSeries(SeriesType.BAR, names.get(k)); 
										
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
										barchart.getAxisSet().getXAxis(0).getTitle().setText(cx1.getText());
										barchart.getAxisSet().getXAxis(0).getTitle().setForeground(new Color(null,0,0,0));
										barchart.getAxisSet().getYAxis(0).getTitle().setText("Y");
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
								saveas(itemSaveAs, null, barchart, ite);
								save(itemSave, null, barchart, ite);
								break;
							}
								
							//chart = null;
						} 
			            catch (IOException e1) {
							e1.printStackTrace();
						}
		            }
				}
				
			}
	    	
	    });
	    itemPush1.addListener(SWT.Selection, new Listener(){

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
	    	    		
	    itemPdf.addSelectionListener(new SelectionListener(){

			@Override
			public void widgetDefaultSelected(SelectionEvent arg0) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void widgetSelected(SelectionEvent arg0) {
				
			}
	    	
	    });
	    
	    wuidTab.setControl(tabHolder);
  }
  
  public void save(ToolItem itemSave, final JFreeChart chart, final Chart barchart, final CTabItem ite){
	  itemSave.addSelectionListener(new SelectionListener(){

		@Override
		public void widgetDefaultSelected(SelectionEvent arg0) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void widgetSelected(SelectionEvent arg0) {
			if(file != null){
				file.delete();
				try {
					file.createNewFile();
					if(chart != null){	
						if(file.getName().endsWith("png"))
							ChartUtilities.saveChartAsPNG(file, chart, 800, 600);
						else if(file.getName().endsWith("jpg") || file.getName().endsWith("jpeg"))
							ChartUtilities.saveChartAsJPEG(file, chart, 800, 600);
					}
					else if(barchart != null){
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
				        if(file.getName().endsWith("png"))
				        	loader.save(file.getAbsolutePath(), SWT.IMAGE_PNG);
				        else if(file.getName().endsWith("jpg") || file.getName().endsWith("jpeg"))
				        	loader.save(file.getAbsolutePath(), SWT.IMAGE_JPEG);
				        image.dispose();
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			
			
			}
			else{
				try {
					FileDialog fd = new FileDialog(parentShell, SWT.SAVE);
					fd.setText("Save As");
					fd.setFilterPath(".");
					String[] filterExt = { "*.png", "*.jpeg", ".jpg", "*.*" };
					fd.setFilterExtensions(filterExt);
					String selected = fd.open();
					if(selected != null){
						
						file = new File(selected);
						if(file.getName() != null)
							ite.setText(file.getName());
						else
							ite.setText("GraphTab");
						if (file.exists()) {
							file.delete();
						}
						
						file.createNewFile();
						if(chart != null){	
							if(file.getName().endsWith("png"))
								ChartUtilities.saveChartAsPNG(file, chart, 800, 600);
							else if(file.getName().endsWith("jpg") || file.getName().endsWith("jpeg"))
								ChartUtilities.saveChartAsJPEG(file, chart, 800, 600);
						}
						else if(barchart != null){
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
					        if(file.getName().endsWith("png"))
					        	loader.save(file.getAbsolutePath(), SWT.IMAGE_PNG);
					        else if(file.getName().endsWith("jpg") || file.getName().endsWith("jpeg"))
					        	loader.save(file.getAbsolutePath(), SWT.IMAGE_JPEG);
					        image.dispose();
						}
					}
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		  
	  });
	  
	  
  }
  
  public void saveas(ToolItem itemSaveAs, final JFreeChart chart, final Chart barchart, final CTabItem ite){
	  itemSaveAs.addSelectionListener(new SelectionListener(){

			@Override
			public void widgetDefaultSelected(SelectionEvent arg0) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void widgetSelected(SelectionEvent arg0) {
				try {
					FileDialog fd = new FileDialog(parentShell, SWT.SAVE);
					fd.setText("Save As");
					fd.setFilterPath(".");
					String[] filterExt = { "*.png", "*.jpeg", ".jpg", "*.*" };
					fd.setFilterExtensions(filterExt);
					String selected = fd.open();
					if(selected != null){
						
						file = new File(selected);
						if(file.getName() != null)
							ite.setText(file.getName());
						else
							ite.setText("GraphTab");
						if (file.exists()) {
							file.delete();
						}
						
						file.createNewFile();
						if(chart != null){	
							if(file.getName().endsWith("png"))
								ChartUtilities.saveChartAsPNG(file, chart, 800, 600);
							else if(file.getName().endsWith("jpg") || file.getName().endsWith("jpeg"))
								ChartUtilities.saveChartAsJPEG(file, chart, 800, 600);
							
						}	
						else if(barchart != null){
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
					        if(file.getName().endsWith("png"))
					        	loader.save(file.getAbsolutePath(), SWT.IMAGE_PNG);
					        else if(file.getName().endsWith("jpg") || file.getName().endsWith("jpeg"))
					        	loader.save(file.getAbsolutePath(), SWT.IMAGE_JPEG);
					        image.dispose();
						}
					}
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
	    	
	    });	  
  }
  
  public boolean isNumeric(String str)  
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
  
  public static ECLMySwtPerspective getInstance(){
       // System.out.println("ECLResultsSwtPerspective");
    return instance;
  }

  public void setActive(boolean b) {
      //the function is called 3 times when leaving this perspective this odd
      //logic is here so it doesn't reload the data when leaving this perspective
	  //folder.setSelection(folder.getItemCount()-1);
	  if(b){
		  if(!this.isActive){
			  if(System.getProperties().getProperty("reportFile") != null && !System.getProperties().getProperty("reportFile").equals("")){
				  String xmlFile = System.getProperties().getProperty("reportFile");
				  ArrayList<String> resultFiles = parseData("reportFile");
				  for(int i =0; i< resultFiles.size(); i++){
					  try {
						buildgui(resultFiles.get(i));
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				  }
				  System.getProperties().setProperty("reportFile","");      	
			  }	
			  int len = folder.getItemCount();
			  folder.setSelection(len-1);
			  this.isActive = true;
		  }
		  else{
			  this.isActive = false;
		 }
       
   }else{
        System.out.println("create eclReporting setActive -- deactivate");
   }
	  
  }
  
  public ArrayList parseData(String propName){
	  ArrayList<String> files = new ArrayList();
	  String saltData = "";
	  	 try{
	  		 if(System.getProperty(propName) != null){
	  			 saltData = System.getProperty(propName);
	  		 }
	  		StringTokenizer fileTokens = new StringTokenizer(saltData,",");
		     	while (fileTokens.hasMoreElements()) {
		     		String file = fileTokens.nextToken();
		     		
		     		if(file != null && !file.equals("null") && !file.equals("")){
		     			System.out.println("Built tab from list" + file);
		     			
		     			files.add(file);
		     		}
		     	}
		     	 saltData = "";
	  	 }catch (Exception e){
	  		 System.out.println("Failed to open files ");
	  	 }
	  	 //just incase it was a signle node
	  	 if(saltData != null && !saltData.equals("null") && !saltData.equals("")){
	  		System.out.println("Built tab from single " + saltData);
	  		files.add(saltData);
	  	 }
	  	//System.setProperty(propName,"");
	  	return files;
  }
  public List<XulOverlay> getOverlays() {
    return Collections.singletonList((XulOverlay) new DefaultXulOverlay("org/pentaho/di/plugins/perspectives/eclmyperspective/res/spoon_perspective_overlay.xul"));
  }

  public List<XulEventHandler> getEventHandlers() {
    return Collections.singletonList((XulEventHandler) new ECLMyPerspectiveHandler());
  }

  public void addPerspectiveListener(SpoonPerspectiveListener spoonPerspectiveListener) {
       System.out.println("addPerspectiveListner");
  }

  public String getId() {
    //System.out.println("getId");
    return "eclmyperspective";
  }

  
  // Whatever you pass out will be reparented. Don't construct the UI in this method as it may be called more than once.
  public Composite getUI() {
      System.out.println("getUI");
	  if(comp == null){
		  parentShell = ((Spoon) SpoonFactory.getInstance()).getShell();
		    
		  display = parentShell.getDisplay();

		  comp = new Composite(parentShell, SWT.BORDER);
		  comp.setLayout(new GridLayout());
		  GridData gd = new GridData(SWT.FILL,SWT.FILL,true,true);
		  comp.setLayoutData(gd);

		  //comp.setLayoutData(new GridData(GridData.FILL_BOTH));
		  
		  //CTabItem tab2 = new CTabItem(folder, SWT.NONE);
	    	//tab2.setText("jus' checkin'");
		  
		  lbl = new Label(comp, SWT.CENTER | SWT.TOP);
		  	      
	       GridData ldata = new GridData(SWT.CENTER, SWT.TOP, true, false);
	       lbl.setLayoutData(ldata);
	       lbl.setText("Create Your Reports Here"); 
	       Button fileButton = new Button(comp, SWT.PUSH | SWT.SINGLE | SWT.TOP);
		    
	       fileButton.setLayoutData(new GridData(SWT.LEFT, SWT.TOP, true, false));
	       fileButton.setText("OPEN FILE");
	     
	       //Listener for the file open button (fileButton)
	       Listener fileOpenListener = new Listener() {
	
	           public void handleEvent(Event e) {
	               String newFile = buildFileDialog();
	               if(newFile != ""){
	                   fileName = newFile;
	                   //TODO: create new tab for file
	                   //openFile(fileName);
	                   try {
						buildgui(fileName);
					} catch (Exception e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
	                   
	                   //int len = folder.getChildren().length;
	                   int len = folder.getItemCount();
	                   System.out.println("Number of tabs: " + len);
	                   folder.setSelection(len-1);
	               }
	           }
	       };
	        
	     fileButton.addListener(SWT.Selection, fileOpenListener);
	       
			folder = new CTabFolder(comp, SWT.CLOSE);
			folder.setSimple(false);
			folder.setBorderVisible(true);
			folder.setLayoutData(new GridData(GridData.FILL_BOTH));
	    	//Label lbl = new Label(folder, SWT.NONE);
	    	//lbl.setText("hjgghjghjg");
		  
	  }
	  
    return comp;
  }
  
  private String buildFileDialog() {
	    
      //file field
      FileDialog fd = new FileDialog(parentShell, SWT.OPEN);

      fd.setText("Open");
      fd.setFilterPath("C:/");
      String[] filterExt = { "*.xml","*.csv", "*.*" };
      fd.setFilterExtensions(filterExt);
      String selected = fd.open();
      if(!(fd.getFileName()).equalsIgnoreCase("")){
          return fd.getFilterPath() + System.getProperty("file.separator") + fd.getFileName();
      }else{
          return "";
      }
      
  }
  
  public String getDisplayName(Locale locale) {
       //System.out.println("getDisplayName");
    return "ECL Reporting";
  }

  public InputStream getPerspectiveIcon() {
    ClassLoader loader = getClass().getClassLoader();
    return loader.getResourceAsStream("org/pentaho/di/plugins/perspectives/eclmyperspective/res/blueprint.png");
  }

  /**
   * This perspective is not Document based, therefore there is no EngineMeta to save/open.
   * @return
   */
  public EngineMetaInterface getActiveMeta() {
       System.out.println("getActiveMeta");
    return null;
  }
  
 
  
  
  
  
}
