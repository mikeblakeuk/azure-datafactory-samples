// Based on https://learn.microsoft.com/en-us/azure/data-factory/quickstart-create-data-factory-dot-net

var line = $"{Environment.NewLine}---------------------------------------------{Environment.NewLine}";

var host = new HostBuilder()
    .ConfigureAppConfiguration((_, builder) => builder.AddJsonFile("appsettings.json", optional: true))
    .ConfigureServices((context, services) => services.Configure<DataFactoryLakehouseOptions>(context.Configuration.GetSection(DataFactoryLakehouseOptions.Key)))
    .Build();

var options = host.Services.GetRequiredService<IOptions<DataFactoryLakehouseOptions>>().Value;

// Data Factory names
var dataFactoryLinkedServiceName = "lakehouseLinkedService";
var dataFactorySourceName = "lakehouseDataSetSource";
var dataFactorySinkName = "lakehouseDataSetSink";
var dataFactoryDataFlowName = "lakehouseDataFlow";
var dataFactoryPipelineName = "lakehousePipeline";
var dataFactoryDataFlowPipelineActivityName = "lakehouseDataFlowActivity";

var sourceName = "source1";
var sinkName = "sink1";

// Credentials for this application to connect to Azure
var tokenCredentials = new DefaultAzureCredential();
var armClient = new ArmClient(tokenCredentials, options.Azure.SubscriptionId);
var dataFactoryName = $"{options.Azure.DataFactoryNamePrefix}{DateTime.UtcNow:yyyy-MM-dd-HHmm}" ;

Console.WriteLine("Getting an existing resource group " + options.Azure.ResourceGroupName + "...");
var resourceGroupResource = armClient.GetResourceGroupResource(ResourceGroupResource.CreateResourceIdentifier(options.Azure.SubscriptionId, options.Azure.ResourceGroupName));
var operation = await resourceGroupResource.GetDataFactories().CreateOrUpdateAsync(WaitUntil.Completed, dataFactoryName, new DataFactoryData(location: options.Azure.DataFactoryLocation), "*");
var dataFactoryResource = operation.Value;

// Create a Lakehouse linked service
Console.WriteLine("Creating a linked service " + dataFactoryLinkedServiceName + "...");
var linkedServiceData = new DataFactoryLinkedServiceData(
    new LakeHouseLinkedService
    {
        Tenant = options.Fabric.FabricTenantId,
        WorkspaceId = options.Fabric.FabricWorkspaceId,
        ArtifactId = options.Fabric.FabricLakehouseId,
        ServicePrincipalId = options.DataFactory.ClientId,
        ServicePrincipalCredential = new DataFactorySecretString(options.DataFactory.ClientSecret),
        ServicePrincipalCredentialType = "ServicePrincipalKey"
    });

var linkedServiceOperation = await dataFactoryResource.GetDataFactoryLinkedServices().CreateOrUpdateAsync(WaitUntil.Completed, dataFactoryLinkedServiceName, linkedServiceData, "*");
Console.WriteLine(linkedServiceOperation.WaitForCompletionResponse().Content + line);

// Create an LakeHouseTableDataset Source
Console.WriteLine("Creating dataset source " + dataFactorySourceName + "...");
var linkedServiceReference = new DataFactoryLinkedServiceReference(DataFactoryLinkedServiceReferenceKind.LinkedServiceReference, dataFactoryLinkedServiceName);
var sourceDataset = new LakeHouseTableDataset(linkedServiceReference)
{
    Table = DataFactoryElement<string>.FromExpression($"@dataset().{DatasetParameters.Table}"),
    // https://github.com/Azure/azure-sdk-for-net/issues/48545
    SchemaTypePropertiesSchema = DataFactoryElement<string>.FromExpression($"@dataset().{DatasetParameters.Schema}"),
    Structure = DataFactoryElement<IList<DatasetDataElement>>.FromExpression($"@dataset().{DatasetParameters.Structure}"),
    Parameters =
    {
        { DatasetParameters.Table, new EntityParameterSpecification(EntityParameterType.String) },
        { DatasetParameters.Schema, new EntityParameterSpecification(EntityParameterType.String) },
        { DatasetParameters.Structure, new EntityParameterSpecification(EntityParameterType.Object) }
    }
};
var datasetData = new DataFactoryDatasetData(sourceDataset);
var datasetOperation = dataFactoryResource.GetDataFactoryDatasets().CreateOrUpdate(WaitUntil.Completed, dataFactorySourceName, datasetData);
Console.WriteLine(datasetOperation.WaitForCompletionResponse().Content + line);

// Create an LakeHouseTableDataset Sink
Console.WriteLine("Creating dataset sink " + dataFactorySinkName + "...");
var sinkReference = new DataFactoryDatasetData(
    new LakeHouseTableDataset(
        new DataFactoryLinkedServiceReference(DataFactoryLinkedServiceReferenceKind.LinkedServiceReference, dataFactoryLinkedServiceName))
    {
        SchemaTypePropertiesSchema = options.Fabric.FabricLakehouseSchemaName,
        Table = options.Fabric.FabricLakehouseTableName + "out"
    });

var sinkOperation = await dataFactoryResource.GetDataFactoryDatasets().CreateOrUpdateAsync(WaitUntil.Completed, dataFactorySinkName, sinkReference, ifMatch: "*");
Console.WriteLine(sinkOperation.WaitForCompletionResponse().Content + line);

// Create Data Flow
Console.WriteLine("Creating Data Flow " + dataFactoryDataFlowName + "...");
var dataFactoryDataFlowData = new DataFactoryDataFlowData(
    new DataFactoryMappingDataFlowProperties
    {
        Sources =
        {
            new DataFlowSource(sourceName)
            {
                Dataset = new DatasetReference(DatasetReferenceType.DatasetReference, dataFactorySourceName)
            }
        },
        Sinks =
        {
            new DataFlowSink(sinkName)
            {
                Dataset = new DatasetReference(DatasetReferenceType.DatasetReference, dataFactorySinkName)
            }
        },
        Script = $@"
source(output(
        countryOrRegion as string,
		holidayName as string,
        normalizeHolidayName as string,
		countryRegionCode as string
    ),
    allowSchemaDrift: true,
	validateSchema: false) ~> {sourceName}
{sourceName} sink(allowSchemaDrift: true,
	validateSchema: false,
	input(
        countryOrRegion as string,
		holidayName as string,
        normalizeHolidayName as string,
		countryRegionCode as string
    ),
	deletable:false,
	insertable:true,
	updateable:false,
	upsertable:false,
	optimizedWrite: false,
	mergeSchema: false,
	autoCompact: false,
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> {sinkName}"
    }
);

var dataFactoryDataFlowOperation = await dataFactoryResource.GetDataFactoryDataFlows().CreateOrUpdateAsync(WaitUntil.Completed, dataFactoryDataFlowName, dataFactoryDataFlowData, "*");
Console.WriteLine(dataFactoryDataFlowOperation.WaitForCompletionResponse().Content + line);

// Create a pipeline
Console.WriteLine(line + "Creating pipeline" + dataFactoryPipelineName + "...");
var structure = new List<DatasetSchemaDataElement>
{
    new() { SchemaColumnName = "countryOrRegion", SchemaColumnType = "String" },
    new() { SchemaColumnName = "holidayName", SchemaColumnType = "String" },
    new() { SchemaColumnName = "normalizeHolidayName", SchemaColumnType = "String" },
    new() { SchemaColumnName = "countryRegionCode", SchemaColumnType = "String" }
};

var datasetParameters = new Dictionary<string, Dictionary<string, object>>
{
    {
        sourceName, new Dictionary<string, object>
        {
            { DatasetParameters.Schema, DataFactoryElement<string>.FromLiteral(options.Fabric.FabricLakehouseSchemaName) },
            { DatasetParameters.Table, DataFactoryElement<string>.FromLiteral(options.Fabric.FabricLakehouseTableName) },
            { DatasetParameters.Structure, DataFactoryElement<IList<DatasetSchemaDataElement>>.FromLiteral(structure.ToList()) }
        }
    }
};

var pipelineData = new DataFactoryPipelineData
{
    Activities =
    {
        new ExecuteDataFlowActivity(dataFactoryDataFlowPipelineActivityName, new DataFlowReference(DataFlowReferenceType.DataFlowReference, dataFactoryDataFlowName)
        {
            DatasetParameters = BinaryData.FromObjectAsJson(datasetParameters)
        })
    }
};

var pipelineOperation = dataFactoryResource.GetDataFactoryPipelines().CreateOrUpdate(WaitUntil.Completed, dataFactoryPipelineName, pipelineData);
Console.WriteLine(pipelineOperation.WaitForCompletionResponse().Content + line);

// Create a pipeline run
Console.WriteLine("Creating pipeline run...");
var pipelineResource = dataFactoryResource.GetDataFactoryPipeline(dataFactoryPipelineName);
var runResponse = pipelineResource.Value.CreateRun();
var runId = runResponse.Value.RunId.ToString();

Console.WriteLine("Pipeline run ID: " + runId);
var urlToFactory = pipelineResource.Value.Id.ToString().Replace("/pipelines/lakehousePipeline", string.Empty);
Console.WriteLine("Web UI Link: https://adf.azure.com/en/monitoring/pipelineruns?factory=" + urlToFactory);
Console.WriteLine(line);

// Monitor the pipeline run
Console.WriteLine("Checking pipeline run status...");
DataFactoryPipelineRunInfo pipelineRun;
while (true)
{
    pipelineRun = dataFactoryResource.GetPipelineRun(runId);
    Console.WriteLine($"{DateTime.UtcNow} Status: " + pipelineRun.Status);
    if (pipelineRun.Status is "InProgress" or "Queued")
        Thread.Sleep(TimeSpan.FromSeconds(10));
    else
        break;
}
Console.WriteLine(pipelineRun.Status + " Duration Ms: " + pipelineRun.DurationInMs + line);

// Check the copy activity run details
Console.WriteLine("Checking copy activity run details...");
var queryResponse = dataFactoryResource.GetActivityRun(pipelineRun.RunId.ToString(),
    new RunFilterContent(DateTime.UtcNow.AddMinutes(-10), DateTime.UtcNow.AddMinutes(10)));
var enumerator = queryResponse.GetEnumerator();
using IDisposable enumerator1 = enumerator;
enumerator.MoveNext();

if (enumerator.Current != null)
{
    Console.WriteLine(enumerator.Current.Status + " Duration Ms: " + enumerator.Current.DurationInMs);
    Console.WriteLine(enumerator.Current.Output);
    Console.WriteLine(enumerator.Current.Error);
}
