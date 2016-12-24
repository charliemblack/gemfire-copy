package io.pivotal.gemfire.demo;

import java.io.Serializable;

/**
 * Created by Charlie Black on 12/23/16.
 */
public class Action implements Serializable{

    private Object key;
    private Object value;
    private boolean isPut = true;
    private boolean isPDXInstance = false;

    public Action(Object key, Object value, boolean isPut) {
        this.key = key;
        this.value = value;
        this.isPut = isPut;
    }

    public Action(Object key, Object value) {
        this.key = key;
        this.value = value;
    }

    public Action(Object key) {
        this.key = key;
        this.isPut = false;
    }

    public Object getKey() {
        return key;
    }

    public void setKey(Object key) {
        this.key = key;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public boolean isPut() {
        return isPut;
    }

    public void setPut(boolean put) {
        isPut = put;
    }

    public boolean isPDXInstance() {
        return isPDXInstance;
    }

    public void setPDXInstance(boolean PDXInstance) {
        isPDXInstance = PDXInstance;
    }
}
