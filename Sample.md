
### Sample output

```json
Creating a linked service lakehouseLinkedService...
{
  "id": "/HIDDEN/providers/Microsoft.DataFactory/factories/datafactory-2025-09-19-1423/linkedservices/lakehouseLinkedService",
  "name": "lakehouseLinkedService",
  "type": "Microsoft.DataFactory/factories/linkedservices",
  "properties": {
    "type": "Lakehouse",
    "typeProperties": {
      "workspaceId": "HIDDEN",
      "artifactId": "HIDDEN",
      "servicePrincipalId": "HIDDEN",
      "tenant": "HIDDEN",
      "servicePrincipalCredentialType": "ServicePrincipalKey",
      "encryptedCredential": "<encryptedCredential>"
    }
  },
  "etag": "530618b9-0000-0100-0000-68cd676c0000"
}
Creating dataset source lakehouseDataSetSource...
{
  "id": "/HIDDEN/providers/Microsoft.DataFactory/factories/datafactory-2025-09-19-1423/datasets/lakehouseDataSetSource",
  "name": "lakehouseDataSetSource",
  "type": "Microsoft.DataFactory/factories/datasets",
  "properties": {
    "type": "LakehouseTable",
    "structure": {
      "type": "Expression",
      "value": "@dataset().myStructure"
    },
    "linkedServiceName": {
      "type": "LinkedServiceReference",
      "referenceName": "lakehouseLinkedService"
    },
    "parameters": {
      "myTable": {
        "type": "String"
      },
      "mySchema": {
        "type": "String"
      },
      "myStructure": {
        "type": "Object"
      }
    },
    "typeProperties": {
      "schema": {
        "type": "Expression",
        "value": "@dataset().mySchema"
      },
      "table": {
        "type": "Expression",
        "value": "@dataset().myTable"
      }
    }
  },
  "etag": "53064fb9-0000-0100-0000-68cd676d0000"
}
Creating dataset sink lakehouseDataSetSink...
{
  "id": "/HIDDEN/providers/Microsoft.DataFactory/factories/datafactory-2025-09-19-1423/datasets/lakehouseDataSetSink",
  "name": "lakehouseDataSetSink",
  "type": "Microsoft.DataFactory/factories/datasets",
  "properties": {
    "type": "LakehouseTable",
    "linkedServiceName": {
      "type": "LinkedServiceReference",
      "referenceName": "lakehouseLinkedService"
    },
    "typeProperties": {
      "schema": "MySchema",
      "table": "public_holidaysout"
    }
  },
  "etag": "530668b9-0000-0100-0000-68cd676d0000"
}
Creating Data Flow lakehouseDataFlow...
{
  "id": "/HIDDEN/providers/Microsoft.DataFactory/factories/datafactory-2025-09-19-1423/dataflows/lakehouseDataFlow",
  "name": "lakehouseDataFlow",
  "type": "Microsoft.DataFactory/factories/dataflows",
  "properties": {
    "type": "MappingDataFlow",
    "typeProperties": {
      "sources": [
        {
          "name": "source1",
          "dataset": {
            "type": "DatasetReference",
            "referenceName": "lakehouseDataSetSource"
          }
        }
      ],
      "sinks": [
        {
          "name": "sink1",
          "dataset": {
            "type": "DatasetReference",
            "referenceName": "lakehouseDataSetSink"
          }
        }
      ],
      "script": "\r\nsource(output(\r\n        countryOrRegion as string,\r\n\t\tholidayName as string,\r\n        normalizeHolidayName as string,\r\n\t\tcountryRegionCode as string\r\n    ),\r\n    allowSchemaDrift: true,\r\n\tvalidateSchema: false) ~> source1\r\nsource1 sink(allowSchemaDrift: true,\r\n\tvalidateSchema: false,\r\n\tinput(\r\n        countryOrRegion as string,\r\n\t\tholidayName as string,\r\n        normalizeHolidayName as string,\r\n\t\tcountryRegionCode as string\r\n    ),\r\n\tdeletable:false,\r\n\tinsertable:true,\r\n\tupdateable:false,\r\n\tupsertable:false,\r\n\toptimizedWrite: false,\r\n\tmergeSchema: false,\r\n\tautoCompact: false,\r\n\tskipDuplicateMapInputs: true,\r\n\tskipDuplicateMapOutputs: true) ~> sink1"
    }
  },
  "etag": "5306eeb9-0000-0100-0000-68cd676e0000"
}
Creating pipelinelakehousePipeline...
{
  "id": "/HIDDEN/providers/Microsoft.DataFactory/factories/datafactory-2025-09-19-1423/pipelines/lakehousePipeline",
  "name": "lakehousePipeline",
  "type": "Microsoft.DataFactory/factories/pipelines",
  "properties": {
    "activities": [
      {
        "name": "lakehouseDataFlowActivity",
        "type": "ExecuteDataFlow",
        "typeProperties": {
          "dataFlow": {
            "type": "DataFlowReference",
            "referenceName": "lakehouseDataFlow",
            "datasetParameters": {
              "source1": {
                "mySchema": "MySchema",
                "myTable": "public_holidays",
                "myStructure": [
                  {
                    "name": "countryOrRegion",
                    "type": "String"
                  },
                  {
                    "name": "holidayName",
                    "type": "String"
                  },
                  {
                    "name": "normalizeHolidayName",
                    "type": "String"
                  },
                  {
                    "name": "countryRegionCode",
                    "type": "String"
                  }
                ]
              }
            }
          }
        }
      }
    ]
  },
  "etag": "53060bba-0000-0100-0000-68cd676e0000"
}
Creating pipeline run...
Pipeline run ID: 95899a83-e7d9-4e61-ba04-fbc7167f9e8d
Checking pipeline run status...
19/09/2025 14:23:45 Status: Queued
19/09/2025 14:23:56 Status: InProgress
19/09/2025 14:24:06 Status: InProgress
19/09/2025 14:24:16 Status: InProgress
19/09/2025 14:24:27 Status: InProgress
19/09/2025 14:24:37 Status: InProgress
19/09/2025 14:24:47 Status: InProgress
19/09/2025 14:24:58 Status: InProgress
19/09/2025 14:25:08 Status: InProgress
19/09/2025 14:25:18 Status: InProgress
19/09/2025 14:25:29 Status: InProgress
19/09/2025 14:25:39 Status: InProgress
19/09/2025 14:25:50 Status: InProgress
19/09/2025 14:26:00 Status: InProgress
19/09/2025 14:26:11 Status: InProgress
19/09/2025 14:26:21 Status: InProgress
19/09/2025 14:26:32 Status: InProgress
19/09/2025 14:26:42 Status: InProgress
19/09/2025 14:26:52 Status: InProgress
19/09/2025 14:27:03 Status: InProgress
19/09/2025 14:27:13 Status: InProgress
19/09/2025 14:27:24 Status: InProgress
19/09/2025 14:27:34 Status: InProgress
19/09/2025 14:27:44 Status: InProgress
19/09/2025 14:27:55 Status: InProgress
19/09/2025 14:28:05 Status: InProgress
19/09/2025 14:28:15 Status: InProgress
19/09/2025 14:28:26 Status: InProgress
19/09/2025 14:28:36 Status: InProgress
19/09/2025 14:28:47 Status: InProgress
19/09/2025 14:28:57 Status: InProgress
19/09/2025 14:29:08 Status: InProgress
19/09/2025 14:29:18 Status: InProgress
19/09/2025 14:29:28 Status: InProgress
19/09/2025 14:29:39 Status: InProgress
19/09/2025 14:29:49 Status: InProgress
19/09/2025 14:30:00 Status: InProgress
19/09/2025 14:30:10 Status: InProgress
19/09/2025 14:30:21 Status: InProgress
19/09/2025 14:30:31 Status: InProgress
19/09/2025 14:30:41 Status: InProgress
19/09/2025 14:30:52 Status: InProgress
19/09/2025 14:31:02 Status: InProgress
19/09/2025 14:31:13 Status: InProgress
19/09/2025 14:31:23 Status: InProgress
19/09/2025 14:31:33 Status: InProgress
19/09/2025 14:31:44 Status: InProgress
19/09/2025 14:31:54 Status: InProgress
19/09/2025 14:32:04 Status: InProgress
19/09/2025 14:32:15 Status: InProgress
19/09/2025 14:32:25 Status: InProgress
19/09/2025 14:32:36 Status: InProgress
19/09/2025 14:32:46 Status: InProgress
19/09/2025 14:32:57 Status: InProgress
19/09/2025 14:33:07 Status: InProgress
19/09/2025 14:33:18 Status: Failed
Failed Duration Ms: 571963
Checking copy activity run details...
Failed Duration Ms: 561398
{
        "runStatus": {
          "idleTimeBeforeCurrentJob": 0,
          "sparkVersion": "3.4",
          "computeAcquisitionDuration": 539004,
          "version": "20250827.1",
          "profile": {},
          "metrics": {
            "sink1": {
              "format": "delta",
              "stages": [
                {
                  "recordsWritten": 0,
                  "lastUpdateTime": "",
                  "bytesWritten": 0,
                  "recordsRead": 0,
                  "bytesRead": 0,
                  "streams": {
                    "sink1": {
                      "count": 0,
                      "cached": false,
                      "totalPartitions": 0,
                      "partitionStatus": "Success",
                      "partitionCounts": [],
                      "type": "sink"
                    },
                    "source1": {
                      "count": 0,
                      "cached": false,
                      "totalPartitions": 0,
                      "partitionStatus": "Success",
                      "partitionCounts": [],
                      "type": "source"
                    }
                  },
                  "target": "sink1",
                  "time": 0,
                  "progressState": ""
                }
              ],
              "sinkPostProcessingTime": 0,
              "store": "lakehouse",
              "rowsWritten": 0,
              "details": {},
              "progressState": {},
              "sources": {
                "source1": {
                  "rowsRead": 0,
                  "store": "lakehouse",
                  "details": {
                    "pathResolutionDuration": [
                      3916
                    ],
                    "fileSystemInitDuration": [
                      4446
                    ]
                  },
                  "format": "delta"
                }
              },
              "sinkProcessingTime": 0
            }
          },
          "clusterComputeId": "9273232d-6c68-4b40-903e-43f385afe0a7",
          "dsl": "\nsource() ~> source1\n\nsource1 sink() ~> sink1",
          "integrationRuntimeName": "General-8-1-0dd0d971-3a01-429a-afa0-dd9fbff7a0bb",
          "sparkRunId": "0"
        },
        "effectiveIntegrationRuntime": "AutoResolveIntegrationRuntime (East US)",
        "billingReference": {
          "activityType": "executedataflow",
          "billableDuration": [
            {
              "meterType": "Data Flow",
              "duration": 1.2191694651111111,
              "unit": "coreHour",
              "sessionType": "JobCluster"
            }
          ]
        },
        "reportLineageToPurview": {
          "status": "NotReported"
        }
      }
{
        "message": "{\"StatusCode\":\"DF-Executor-InvalidPath\",\"Message\":\"Job failed due to reason: at Source 'source1': Path /HIDDEN/Tables/public_holidays/ does not resolve to any file(s). Please make sure the file/folder exists and is not hidden. At the same time, please ensure special character is not included in file/folder name, for example, name starting with _\",\"Details\":\"\"}",
        "failureType": "UserError",
        "target": "lakehouseDataFlowActivity"
      }
```
