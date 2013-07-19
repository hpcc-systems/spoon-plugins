package org.pentaho.di.plugins.perspectives.saltresults;

//import org.pentaho.di.plugins.examples.texteditor.EditorPerspective;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.components.XulMessageBox;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;

/**
 * Event handler respond to UI events. This one responds to clicks on the menu-items added through the Plugin
 * and Perspective Overlays
 * 
 * User: nbaker
 * Date: 1/6/11
 */
public class SaltResultsPerspectiveHandler extends AbstractXulEventHandler {

  public void saltHello() throws XulException {
    XulMessageBox msg = (XulMessageBox) document.createElement("messagebox");
    msg.setTitle("Salt Results");
    msg.setMessage("Salt Results. This was provided by a plugin!");
    msg.open();
  }

  public void saltHelloToPerspective() throws XulException {
    XulMessageBox msg = (XulMessageBox) document.createElement("messagebox");
    msg.setTitle("Salt Results");
    msg.setMessage("Hello! This action is only available when the perspective is active.");
    msg.open();
  }


  public String getName(){
    return "spoonSaltResults";
  }
}
