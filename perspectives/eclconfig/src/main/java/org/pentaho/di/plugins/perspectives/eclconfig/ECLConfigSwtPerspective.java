package org.pentaho.di.plugins.perspectives.eclconfig;

import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Label;
import org.pentaho.di.core.EngineMetaInterface;
import org.pentaho.di.core.gui.SpoonFactory;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.SpoonPerspective;
import org.pentaho.di.ui.spoon.SpoonPerspectiveListener;
import org.pentaho.ui.xul.XulOverlay;
import org.pentaho.ui.xul.impl.DefaultXulOverlay;
import org.pentaho.ui.xul.impl.XulEventHandler;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

//import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.StringTokenizer;
import org.eclipse.swt.widgets.Composite;

import java.io.*;
import java.lang.reflect.Method;

import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Shell;


import org.eclipse.swt.*;
import org.eclipse.swt.layout.*;
import org.eclipse.swt.widgets.*;

import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.SashForm;




public class ECLConfigSwtPerspective implements SpoonPerspective {

  private Composite comp;
  private static ECLConfigSwtPerspective instance = new ECLConfigSwtPerspective();
  
  private String dataIn;
  private String fileName;
  private int fileCount;

  

  private int middle = 100;
  private int margin = 25;

  private Label lbl;
  private Shell parentShell;
  private Table table;
  
  private CTabFolder folder;
  
  private boolean isActive;
  
  private String wuid = "";
  private String jobname = "";
  private String serverAddress = "";

  public void setFileName(String fn){
      fileName = fn;
      lbl.setText("Data returned from HPCC " + fileName);

  }
  public String getFileName(){
      return fileName;
  }
  
  private ECLConfigSwtPerspective(){  
    System.out.println("create eclConfig ECLConfigSwtPerspective");
    createUI();
  }

  private void createUI(){
   
     
  }
  

  public static ECLConfigSwtPerspective getInstance(){
        System.out.println("ECLConfigSwtPerspective");
    return instance;
  }

  public void setActive(boolean b) {
     
  }
  
  
  
  public List<XulOverlay> getOverlays() {
    return Collections.singletonList((XulOverlay) new DefaultXulOverlay("org/pentaho/di/plugins/perspectives/eclconfig/res/spoon_perspective_overlay.xul"));
  }

  public List<XulEventHandler> getEventHandlers() {
    return Collections.singletonList((XulEventHandler) new ECLConfigPerspectiveHandler());
  }

  public void addPerspectiveListener(SpoonPerspectiveListener spoonPerspectiveListener) {
       System.out.println("addPerspectiveListner");
  }

  public String getId() {
    //System.out.println("getId");
    return "spoonECLConfig";
  }

  
  // Whatever you pass out will be reparented. Don't construct the UI in this method as it may be called more than once.
  public Composite getUI() {
       //System.out.println("getUI");
    return comp;
  }

  public String getDisplayName(Locale locale) {
       //System.out.println("getDisplayName");
    return "ECL Config";
  }

  public InputStream getPerspectiveIcon() {
    ClassLoader loader = getClass().getClassLoader();
    return null;
	//return loader.getResourceAsStream("org/pentaho/di/plugins/perspectives/eclconfig/res/blueprint.png");
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
