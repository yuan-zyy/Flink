package com.demo;

import java.util.Map;

public class OpConfig {
    public String id;
    public String className;
    public int parallelism;
    public String chainingStrategy; // DEFAULT, START_NEW, DISABLE
    public Map<String, Object> params;
}