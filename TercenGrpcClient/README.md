
```shell
# git submodule add https://github.com/tercen/tercen_grpc_api.git TercenGrpcClient/tercen_grpc_api
cd TercenGrpcClient/tercen_grpc_api
git pull origin main
```

```shell
Install-Package Grpc.Net.Client
Install-Package Google.Protobuf
Install-Package Grpc.Tools

dotnet sln add ../TercenGrpcClient/TercenGrpcClient.csproj
dotnet add ./TercenGrpcClient.Test/TercenGrpcClient.Test.csproj reference ./TercenGrpcClient/TercenGrpcClient.csproj  
```
 
```protobuf
option csharp_namespace = "TercenGrpcClient";
```

Edit the TercenGrpcClient.csproj project file:

Add an item group with a <Protobuf> element that refers to the tercen.proto file:
```xml
<ItemGroup>
  <Protobuf Include="Protos\tercen.proto" GrpcServices="Client" />
</ItemGroup>
```


```shell
dotnet test --no-restore --verbosity normal
```
