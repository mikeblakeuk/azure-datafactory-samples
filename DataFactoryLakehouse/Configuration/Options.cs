namespace DataFactoryLakehouse.Configuration;

public class DataFactoryLakehouseOptions
{
    public const string Key = "DataFactoryLakehouse";

    public AzureOptions Azure { get; set; } = new();
    public FabricOptions Fabric { get; set; } = new();
    public DataFactoryOptions DataFactory { get; set; } = new();
}

public class AzureOptions
{
    public string SubscriptionId { get; set; } = string.Empty;
    public string ResourceGroupName { get; set; } = string.Empty;
    public string DataFactoryNamePrefix { get; set; } = string.Empty;
    public string DataFactoryLocation { get; set; } = string.Empty;
}

public class FabricOptions
{
    public string FabricTenantId { get; set; } = string.Empty;
    public string FabricWorkspaceId { get; set; } = string.Empty;
    public string FabricLakehouseId { get; set; } = string.Empty;
    public string FabricLakehouseSchemaName { get; set; } = string.Empty;
    public string FabricLakehouseTableName { get; set; } = string.Empty;

    public string[] CountryCodes { get; set; } = [];
}

public class DataFactoryOptions
{
    public string ClientId { get; set; } = string.Empty;
    public string ClientSecret { get; set; } = string.Empty;
}
