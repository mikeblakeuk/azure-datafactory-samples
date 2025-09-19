# Goal
Read from Fabric Lakehouse that has schema within Azure Data Factory using paramters on the source.

Using net9.0 with `Azure.ResourceManager.DataFactory` SDK NuGet to create the DataFactory and run it.

Based on https://learn.microsoft.com/en-us/azure/data-factory/quickstart-create-data-factory-dot-net 

⚠️Please read first.

## Setup 
You will need dotnet SDK 9
Open the C# sln and restore the dotnet packages.

### Azure
You first need Azure with a resource group and be logined in to Azure CLI/VS.

Update the config with the Azure Subscription guid and the name of the resource group
```json
   "SubscriptionId": "AAAA-SUBSCRIPTION-GUID",
   "ResourceGroupName": "RESOURCE-GROUP",
```

Create a Service Principle with Entra. This will be used for Data Factory to connect Fabric
Update the Config with the Clinet Id
```json
  "ClientId": "CLIENTID",
  "ClientSecret": "SECRET"
```
### Fabric
Create a new Fabric Workspace (ideally assigned to a capacity).

<img width="481" height="142" alt="image" src="https://github.com/user-attachments/assets/1a69be50-e227-4336-a173-5eb09f9466d9" />

Add the Service Princple as a Contributer to the Workspace

<img width="303" height="253" alt="image" src="https://github.com/user-attachments/assets/c4139046-6c27-46f0-a469-c3b38c92d7af" />

Create a new Lakehouse with Schemas enabled.

<img width="312" height="184" alt="image" src="https://github.com/user-attachments/assets/0a144bc2-51c8-4527-9ac7-621da46309f5" />

Add a schema within the Lakehouse

<img width="308" height="151" alt="image" src="https://github.com/user-attachments/assets/a06e8379-2d0f-4828-b971-4f5c05f0ea52" />

Add Samples data

<img width="200" height="209" alt="image" src="https://github.com/user-attachments/assets/0e3187d4-9055-4891-a857-2f77c15b7efa" />

Load the public holiday sample dataset from the Files to the Schema you just created

<img width="544" height="244" alt="image" src="https://github.com/user-attachments/assets/604299e6-a1af-45d6-8896-3ae7bfc23cb1" />

Update the config to the Lakehouse and SchemaName
```json
  "FabricTenantId": "AAAA-FabricTenantId",
  "FabricWorkspaceId": "AAAA-FabricWorkspaceId",
  "FabricLakehouseId": "AAAA-FabricLakehouseId",
  "FabricLakehouseSchemaName": "YourSchema",
```

Run the Program.cs

<img width="447" height="133" alt="image" src="https://github.com/user-attachments/assets/76475d7e-f7e7-44ca-a7bb-45c90b822e26" />

