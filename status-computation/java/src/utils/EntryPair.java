/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package utils;

/**
 *
 * @param <A> 
 * @param <B> 
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class EntryPair<A,B> {
    
    public A First;
    public B Second;
    
    public EntryPair(A a, B b) {
        this.First = a;
        this.Second = b;
    }
}
