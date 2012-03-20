
import org.hpccsystems.ecldirect.WsEcl;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author chalaax
 */
public class WsEclTest {
    public static void main(String[] args) {
        WsEcl wsEcl = new WsEcl("192.168.59.129", "thor", "personandcontact.1");
        wsEcl.execute();
    }
}
