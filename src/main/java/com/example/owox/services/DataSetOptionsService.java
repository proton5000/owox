package com.example.owox.services;

public interface DataSetOptionsService {

    void taskOne(String projectId, String datasetId, String prefix, int count) throws Exception;

    void taskTwo(String projectId, String datasetId) throws Exception;

}
