package com.example.owox.services.impl;

import com.example.owox.services.DataSetOptionsService;
import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.*;
import org.springframework.stereotype.Service;

import java.util.Calendar;
import java.util.Date;

@Service
public class DataSetOptions implements DataSetOptionsService {

    @Override
    public void taskOne(String projectId, String datasetId, String prefix, int count) throws Exception {

        // Instantiates a client
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

        System.out.println("List of datasets:");
        for (Dataset dataset : bigquery.listDatasets().iterateAll()) {
            System.out.printf("%s%n", dataset.getDatasetId().getDataset());
        }
        System.out.println("------------------------------------");
        System.out.println("Create table with fields");

        //get my dataset
        DatasetId dataset = DatasetId.of(projectId, datasetId);
        Dataset myDataset = bigquery.getDataset(dataset);

        //create tables in my dataset
        for (int i = 0; i < count; i++) {

            TableId tableId = TableId.of(myDataset.getDatasetId().getDataset(), prefix + i);
            // Table field definition
            Field field0 = Field.of("hitId", LegacySQLTypeName.STRING);
            Field field1 = Field.of("userId", LegacySQLTypeName.STRING);
            // Table schema definition
            Schema schema = Schema.of(field0, field1);
            TableDefinition tableDefinition = StandardTableDefinition.of(schema);
            TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

            Table table = bigquery.create(tableInfo);

            System.out.println("table = " + table.getTableId().getTable());
            System.out.println("fields = " + table.getDefinition().getSchema().getFields());

        }

    }

    @Override
    public void taskTwo(String projectId, String datasetId) throws Exception {

        // Instantiates a client
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

        System.out.println("List of datasets:");
        for (Dataset dataset : bigquery.listDatasets().iterateAll()) {
            System.out.printf("%s%n", dataset.getDatasetId().getDataset());
        }
        System.out.println("------------------------------------");
        System.out.println("Update expiration time in tables");

        //get my dataset
        DatasetId dataset = DatasetId.of(projectId, datasetId);
        Dataset myDataset = bigquery.getDataset(dataset);

        //get tables from my dataset
        Page<Table> tables = bigquery.listTables(myDataset.getDatasetId().getDataset(), BigQuery.TableListOption.pageSize(100));

        //get date (current date + 14 days)
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_MONTH, 14);

        //set new expiration time in my tables
        for (Table table : tables.iterateAll()) {

            if ( table.getTableId().getTable().substring(0, 3).equals("tmp") ) {

                TableInfo tableInfo = table.toBuilder().setExpirationTime(calendar.getTimeInMillis()).build();
                Table newTable = bigquery.update(tableInfo);

                System.out.println("current time = " + Calendar.getInstance().getTime());
                System.out.println("expiration time = " + new Date(newTable.getExpirationTime()));
            }
        }
    }
}
