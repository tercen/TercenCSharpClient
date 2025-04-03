# TercenCSharpClient

A C# client library for interacting with the Tercen platform.

## Overview

`TercenCSharpClient` is a .NET library designed to simplify communication with the Tercen API (or platform). This client provides a convenient way to integrate Tercen functionality into your C# applications, enabling developers to perform tasks such as data retrieval, manipulation, or other operations supported by Tercen.

*Note: This is a placeholder description based on the repository name. For accurate details, please refer to the official documentation or update this section.*
  
## Installation

Since no releases or packages are published yet, you can clone the repository and build the library manually:

```bash
git clone --recurse-submodules https://github.com/tercen/TercenCSharpClient.git
cd TercenCSharpClient
dotnet restore
dotnet build --configuration Release --no-restore
```

*Future releases may be available via NuGet—stay tuned for updates!*


## Introduction to Tercen Studio for Local Development

Tercen Studio, located at `https://github.com/tercen/tercen_studio`, is a development environment for the
Tercen platform. Tercen is a tool for data analysis and workflow management. Tercen Studio uses Docker to run locally
and works with the Tercen gRPC API, found at `https://github.com/tercen/tercen_grpc_api.git`. The API, defined
in `.proto` files, offers services like `FileService`, `TaskService`, and `WorkflowService` to manage files,
tasks, and workflows. You can set up Tercen Studio with Docker Compose and access it at `http://127.0.0.1:5402`.
It provides a straightforward way to develop and test Tercen projects on your computer.

Once Tercen Studio is installed and running, run the following

```bash
# export TERCEN_URI="http://127.0.0.1:50051"
# export TERCEN_USERNAME=admin
# export TERCEN_PASSWORD=admin
# export TERCEN_TENANT="tercen:"

dotnet test --no-restore --verbosity normal
```


 