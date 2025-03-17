using System.Text;
using System.Text.Json;
using Grpc.Core;

namespace TercenGrpcClient.Test;

using Grpc.Net.Client;
using System.Net.Http;

using Tercen;

// Custom handler to add authentication token
public class AuthenticatedHttpClientHandler(string token) : DelegatingHandler
{
    protected override HttpResponseMessage Send(
        HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        request.Headers.Add("Authorization", token);
        return base.Send(request, cancellationToken);
    }

    protected override async Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        request.Headers.Add("Authorization", token);
        return await base.SendAsync(request, cancellationToken);
    }
}

public class TercenExceptionInner
{
    public string kind { get; set; }
    public int statusCode { get; set; }
    public string error { get; set; }
    public string reason { get; set; }

    public TercenExceptionInner(string kind, int statusCode, string error, string reason)
    {
        this.kind = kind;
        this.statusCode = statusCode;
        this.error = error;
        this.reason = reason;
    }
}

public class TercenException : Exception
{
    private TercenExceptionInner _inner;

    TercenException(TercenExceptionInner inner)
    {
        _inner = inner;
    }

    public TercenException(RpcException grpcException)
    {
        try
        {
            var tercenExceptionInner = JsonSerializer.Deserialize<TercenExceptionInner>(grpcException.Status.Detail);
            _inner = tercenExceptionInner ??
                     new TercenExceptionInner("ServiceError", 0, "unknown", grpcException.Message);
        }
        catch (JsonException ex)
        {
            _inner = new TercenExceptionInner("ServiceError", 0, "unknown", grpcException.Message);
        }
    }

    public int GetStatusCode()
    {
        return _inner.statusCode;
    }

    public string GetError()
    {
        return _inner.error;
    }

    public string GetReason()
    {
        return _inner.reason;
    }

    public bool IsNotFound()
    {
        return _inner.statusCode == 404;
    }
}

public class TercenFactory
{
    private string _token;
    private string _address;
    private GrpcChannel _channel;

    public static async System.Threading.Tasks.Task<TercenFactory> Create(string address, string tenant,
        string username, string password)
    {
        using var channel = GrpcChannel.ForAddress(address);

        var client = new UserService.UserServiceClient(channel);

        var response = await client.connect2Async(new ReqConnect2
        {
            Domain = tenant,
            UsernameOrEmail = username,
            Password = password
        });

        var userSession = response.Result;

        return new TercenFactory(CreateAuthenticatedChannel(address, userSession.Token.Token_),
            address, userSession.Token.Token_);
    }

    private TercenFactory(GrpcChannel channel, string address, string token)
    {
        _token = token;
        _address = address;
        _channel = channel;
    }

    private static GrpcChannel CreateAuthenticatedChannel(string address, string token)
    {
        var httpHandler = new AuthenticatedHttpClientHandler(token)
        {
            InnerHandler = new HttpClientHandler()
        };

        var channel = GrpcChannel.ForAddress(address, new GrpcChannelOptions
        {
            HttpHandler = httpHandler
        });

        return channel;
    }

    public UserService.UserServiceClient UserService()
    {
        return new UserService.UserServiceClient(_channel);
    }

    public TeamService.TeamServiceClient TeamService()
    {
        return new TeamService.TeamServiceClient(_channel);
    }

    public ProjectService.ProjectServiceClient ProjectService()
    {
        return new ProjectService.ProjectServiceClient(_channel);
    }

    public FileService.FileServiceClient FileService()
    {
        return new FileService.FileServiceClient(_channel);
    }
}

[TestClass]
public sealed class Test1
{
    private TercenFactory _factory;

    [TestInitialize]
    public async System.Threading.Tasks.Task SetupAsync()
    {
        string? uri = Environment.GetEnvironmentVariable("TERCEN_URI");
        if (string.IsNullOrEmpty(uri))
        {
            uri = "http://127.0.0.1:50051";
        }
        _factory = await TercenFactory.Create(
            uri,
            "",
            "admin",
            "admin");
    }

    [TestMethod]
    public async System.Threading.Tasks.Task TestUserGet()
    {
        var getResponse = await _factory.UserService().getAsync(new GetRequest { Id = "test" });
        Assert.AreEqual("test", getResponse.User.Name);
        Assert.AreEqual("test@tercen.com", getResponse.User.Email);
    }

    private async Task<Team> GetOrCreateTeam(string teamName)
    {
        try
        {
            var teamResponse = await _factory.TeamService().getAsync(new GetRequest { Id = teamName });
            return teamResponse.Team;
        }
        catch (RpcException e)
        {
            if (!new TercenException(e).IsNotFound())
            {
                throw;
            }
        }

        var team = new ETeam
        {
            Team = new Team
            {
                Name = teamName,
            }
        };

        team = await _factory.TeamService().createAsync(team);

        return team.Team;
    }

    [TestMethod]
    public async System.Threading.Tasks.Task TestTeamCreateUpdateDelete()
    {
        const string teamName = "test_csharp_team";

        var team = await GetOrCreateTeam(teamName);
        
        var getResponse = await _factory.TeamService().getAsync(new GetRequest { Id = team.Id});
        Assert.AreEqual(teamName, getResponse.Team.Name);
        Assert.AreEqual("admin", getResponse.Team.Acl.Owner);

        Assert.AreEqual(teamName, team.Id);
        Assert.IsNotNull(team.Rev);
        Assert.AreEqual(teamName, team.Name);
        Assert.AreEqual("admin", team.Acl.Owner);

        await _factory.TeamService().deleteAsync(new DeleteRequest { Id = team.Id, Rev = team.Rev });
    }

    [TestMethod]
    public async System.Threading.Tasks.Task TestProjectCreateUpdateDelete()
    {
        const string teamName = "test_csharp_team";
        const string projectName = "test_csharp_project";

        var team = await GetOrCreateTeam(teamName);

        var project = new EProject()
        {
            Project = new Project()
            {
                Name = projectName,
                Acl = new Acl()
                {
                    Owner = team.Id,
                }
            }
        };

        project = await _factory.ProjectService().createAsync(project);
        Assert.AreEqual(projectName, project.Project.Name);
        Assert.AreEqual(team.Id, project.Project.Acl.Owner);
        Assert.AreEqual("", project.Project.Description);

        project.Project.Description = "my project";

        var updateResponse = await _factory.ProjectService().updateAsync(project);

        Assert.AreNotEqual(project.Project.Rev, updateResponse.Rev);

        var updatedProject = await _factory.ProjectService().getAsync(new GetRequest { Id = project.Project.Id });

        Assert.AreEqual(updateResponse.Rev, updatedProject.Project.Rev);
        Assert.AreEqual(project.Project.Description, updatedProject.Project.Description);

        await _factory.ProjectService()
            .deleteAsync(new DeleteRequest { Id = project.Project.Id, Rev = project.Project.Rev });
        await _factory.TeamService().deleteAsync(new DeleteRequest { Id = team.Id, Rev = team.Rev });
    }

    [TestMethod]
    public async System.Threading.Tasks.Task TestSearchProject()
    {
        const string teamName = "test_csharp_team";
        const string projectName1 = "test_csharp_project_1";
        const string projectName2 = "test_csharp_project_2";

        var team = await GetOrCreateTeam(teamName);
        await _factory.TeamService().deleteAsync(new DeleteRequest { Id = team.Id, Rev = team.Rev });
        team = await GetOrCreateTeam(teamName);

        await _factory.ProjectService().createAsync(new EProject()
        {
            Project = new Project()
            {
                Name = projectName1,
                IsPublic = true,
                Acl = new Acl()
                {
                    Owner = team.Id,
                }
            }
        });
        await _factory.ProjectService().createAsync(new EProject()
        {
            Project = new Project()
            {
                Name = projectName2,
                IsPublic = true,
                Acl = new Acl()
                {
                    Owner = team.Id,
                }
            }
        });

        var request = new KeyRangeRequest { Name = "Project/findByTeamAndIsPublicAndLastModifiedDate" };

        request.StartKeys.Add(new IndexKeyValue { IndexField = "acl.owner", StringValue = team.Id });
        request.StartKeys.Add(new IndexKeyValue { IndexField = "isPublic", BoolValue = true });
        request.StartKeys.Add(new IndexKeyValue { IndexField = "lastModifiedDate.value", StringValue = "2100" });

        request.EndKeys.Add(new IndexKeyValue { IndexField = "acl.owner", StringValue = team.Id });
        request.EndKeys.Add(new IndexKeyValue { IndexField = "isPublic", BoolValue = true });
        request.EndKeys.Add(new IndexKeyValue { IndexField = "lastModifiedDate.value", StringValue = "" });

        request.Limit = 1;

        var response = await _factory.ProjectService().findKeyRangeAsync(request);

        Assert.AreEqual(1, response.List.Count);

        Assert.AreEqual(projectName2, response.List[0].Project.Name);

        request.Limit = 200;

        response = await _factory.ProjectService().findKeyRangeAsync(request);

        Assert.AreEqual(2, response.List.Count);

        Assert.AreEqual(projectName2, response.List[0].Project.Name);
        Assert.AreEqual(projectName1, response.List[1].Project.Name);

        await _factory.ProjectService()
            .deleteAsync(new DeleteRequest { Id = response.List[1].Project.Id, Rev = response.List[1].Project.Rev });

        response = await _factory.ProjectService().findKeyRangeAsync(request);

        Assert.AreEqual(1, response.List.Count);

        Assert.AreEqual(projectName2, response.List[0].Project.Name);

        await _factory.TeamService().deleteAsync(new DeleteRequest { Id = team.Id, Rev = team.Rev });
    }

    [TestMethod]
    public async System.Threading.Tasks.Task TestUploadFile()
    {
        const string teamName = "test_csharp_team";
        const string projectName1 = "test_csharp_project_1";

        // Create a temporary file
        var tempFilePath = Path.GetTempFileName();
        var fileName = Path.GetFileName(tempFilePath);

        // const string fileContent = "This is a test file content.";

        var fileContent = new StringBuilder();
        for (var i = 0; i < 10000; i++)
        {
            fileContent.AppendLine("This is a test file content.");
        }


        var team = await GetOrCreateTeam(teamName);

        try
        {
            await File.WriteAllTextAsync(tempFilePath, fileContent.ToString());

            const int chunkSize = 1024 * 64; // 64 KB chunks

            var project = await _factory.ProjectService().createAsync(new EProject()
            {
                Project = new Project()
                {
                    Name = projectName1,
                    IsPublic = true,
                    Acl = new Acl()
                    {
                        Owner = team.Id,
                    }
                }
            });

            var fileDocument = new FileDocument
            {
                Name = fileName,
                ProjectId = project.Project.Id,
                Acl = new Acl()
                {
                    Owner = team.Id,
                }
            };

            using var uploadCall = _factory.FileService().upload();
            await using var fileStream = new FileStream(tempFilePath, FileMode.Open, FileAccess.Read);
            var buffer = new byte[chunkSize];
            int bytesRead;
            var firstChunk = true;

            // Read file in chunks and send to server
            while ((bytesRead = await fileStream.ReadAsync(buffer, 0, buffer.Length)) > 0)
            {
                var chunk = new ReqUpload
                {
                    Bytes = Google.Protobuf.ByteString.CopyFrom(buffer, 0, bytesRead)
                };

                // Send fileDocument with the first chunk
                if (firstChunk)
                {
                    chunk.File = new EFileDocument
                    {
                        Filedocument = fileDocument
                    };
                    firstChunk = false;
                }

                await uploadCall.RequestStream.WriteAsync(chunk);
            }

            // Signal that the stream is complete
            await uploadCall.RequestStream.CompleteAsync();

            var resp = await uploadCall.ResponseAsync;

            Assert.IsNotNull(resp.Result.Filedocument.Id);
            Assert.IsTrue(resp.Result.Filedocument.Id.Length > 0);

            var reqDownload = new ReqDownload
            {
                FileDocumentId = resp.Result.Filedocument.Id,
            };

            using var downloadCall = _factory.FileService().download(reqDownload);
            var sb = new StringBuilder();

            await foreach (var chunk in downloadCall.ResponseStream.ReadAllAsync())
            {
                sb.Append(Encoding.UTF8.GetString(chunk.Result.ToByteArray()));
            }

            Assert.AreEqual(fileContent.ToString(), sb.ToString());

            await _factory.FileService().deleteAsync(new DeleteRequest
                { Id = resp.Result.Filedocument.Id, Rev = resp.Result.Filedocument.Rev });
        }
        finally
        {
            if (File.Exists(tempFilePath))
            {
                File.Delete(tempFilePath);
            }

            await _factory.TeamService().deleteAsync(new DeleteRequest { Id = team.Id, Rev = team.Rev });
        }
    }
}