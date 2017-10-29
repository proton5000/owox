package com.example.owox.services.impl;

import com.google.cloud.bigquery.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.Calendar;
import java.util.concurrent.TimeUnit;

public class DataSetOptionsTest {
    @Test
    public void taskOne() throws Exception {
        DataSetOptions dataSetOptions = new DataSetOptions();
        dataSetOptions.taskOne("coursera-162521", "proton5000", "tmp_test", 1);

        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

        DatasetId dataset = DatasetId.of("coursera-162521", "proton5000");
        Dataset myDataset = bigquery.getDataset(dataset);

        Table table = bigquery.getTable(myDataset.getDatasetId().getDataset(), "tmp_test0");
        System.out.println(table);
        Assert.assertEquals("tmp_test0", table.getTableId().getTable());
        bigquery.delete(table.getTableId());
    }

    @Test
    public void taskTwo() throws Exception {
        DataSetOptions dataSetOptions = new DataSetOptions();
        dataSetOptions.taskOne("coursera-162521", "proton5000", "tmp555", 1);
        dataSetOptions.taskTwo("coursera-162521", "proton5000");

        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

        DatasetId dataset = DatasetId.of("coursera-162521", "proton5000");
        Dataset myDataset = bigquery.getDataset(dataset);

        Table table = bigquery.getTable(myDataset.getDatasetId().getDataset(), "tmp5550");

        Calendar calendar = Calendar.getInstance();

        Assert.assertTrue(TimeUnit.DAYS.convert(table.getExpirationTime() - calendar.getTimeInMillis(), TimeUnit.MILLISECONDS) >= 13);

        bigquery.delete(table.getTableId());

    }

}