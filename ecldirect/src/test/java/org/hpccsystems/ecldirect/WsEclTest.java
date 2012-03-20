package org.hpccsystems.ecldirect;

import org.junit.Test;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author chalaax
 */
public class WsEclTest {
    
    @Test
    public void must_work() {
        WsEcl wsEcl = new WsEcl("192.168.59.129", "thor", "personandcontact.1");
        wsEcl.execute();
    }
}
