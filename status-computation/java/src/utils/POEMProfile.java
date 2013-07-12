/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package utils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class POEMProfile {
    private String name = null;
    private Set<String> metrics = null;
    
    public POEMProfile(String n, List<String> pM) {
        this.name = n;
        this.metrics = new HashSet<String>(pM);
    }
    
    public POEMProfile(String n, String m) {
        this.name = n;
        this.metrics = new HashSet<String>();
        this.metrics.add(m);
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the metrics
     */
    public Set<String> getMetrics() {
        return metrics;
    }

    /**
     * @param metrics the metrics to set
     */
    public void setMetrics(Set<String> metrics) {
        this.metrics = metrics;
    }
    
    /**
     * @param metrics the metrics to set
     */
    public void setMetrics(List<String> metrics) {
        this.metrics = new HashSet<String>(metrics);
    }
    
    /**
     * @param metric the metrics to set
     */
    public void appendMetrics(String metric) {
        if (this.metrics == null) {
            this.metrics = new HashSet<String>();
        }
        
        this.metrics.add(metric);
    }
}
