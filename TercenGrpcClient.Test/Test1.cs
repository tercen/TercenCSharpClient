using TercenGrpcClient.extensions;

namespace TercenGrpcClient.Test;

using System.Text;
using Grpc.Core;
using Tercen;
using TercenGrpcClient.client;

[DoNotParallelize]
[TestClass]
public sealed class Test1
{
    private TercenFactory _factory;
    private string? uri;
    private string? tenant;
    private string? username;
    private string? password;

    [TestInitialize]
    public async System.Threading.Tasks.Task SetupAsync()
    {
        uri = Environment.GetEnvironmentVariable("TERCEN_URI");
        tenant = Environment.GetEnvironmentVariable("TERCEN_TENANT");
        username = Environment.GetEnvironmentVariable("TERCEN_USERNAME");
        password = Environment.GetEnvironmentVariable("TERCEN_PASSWORD");

        if (string.IsNullOrEmpty(uri))
        {
            uri = "http://127.0.0.1:50051";
        }

        if (string.IsNullOrEmpty(username))
        {
            username = "admin";
        }

        if (string.IsNullOrEmpty(password))
        {
            password = "admin";
        }

        if (string.IsNullOrEmpty(tenant))
        {
            tenant = "";
        }

        _factory = await TercenFactory.Create(
            uri,
            tenant,
            username,
            password);
    }

    [TestMethod]
    public async System.Threading.Tasks.Task TestUserGet()
    {
        var getResponse = await _factory.UserService().getAsync(new GetRequest { Id = "test" });
        Assert.AreEqual("test", getResponse.User.Name);
        Assert.AreEqual("test@tercen.com", getResponse.User.Email);
    }


    [TestMethod]
    public async System.Threading.Tasks.Task TestTeamCreateUpdateDelete()
    {
        const string teamName = "test_csharp_team";

        var team = await _factory.TeamService().GetOrCreateTeam(teamName);

        var getResponse = await _factory.TeamService().getAsync(new GetRequest { Id = team.Id });
        Assert.AreEqual(teamName, getResponse.Team.Name);
        Assert.AreEqual(username, getResponse.Team.Acl.Owner);

        Assert.AreEqual(teamName, team.Id);
        Assert.IsNotNull(team.Rev);
        Assert.AreEqual(teamName, team.Name);
        Assert.AreEqual(username, team.Acl.Owner);

        await _factory.TeamService().deleteAsync(new DeleteRequest { Id = team.Id, Rev = team.Rev });
    }

    [TestMethod]
    public async System.Threading.Tasks.Task TestProjectCreateUpdateDelete()
    {
        const string teamName = "test_csharp_team";
        const string projectName = "test_csharp_project";

        var team = await _factory.TeamService().GetOrCreateTeam(teamName);

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

        var team = await _factory.TeamService().GetOrCreateTeam(teamName);
        await _factory.TeamService().deleteAsync(new DeleteRequest { Id = team.Id, Rev = team.Rev });
        team = await _factory.TeamService().GetOrCreateTeam(teamName);

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


        var projects = await _factory.DocumentService().FindProjectsByOwner(team.Id);

        Assert.AreEqual(2, projects.Count);

        Assert.AreEqual(projectName2, projects[0].Name);
        Assert.AreEqual(projectName1, projects[1].Name);

        await _factory.ProjectService()
            .deleteAsync(new DeleteRequest { Id = projects[1].Id, Rev = projects[1].Rev });

        projects = await _factory.DocumentService().FindProjectsByOwner(team.Id);

        Assert.AreEqual(1, projects.Count);

        Assert.AreEqual(projectName2, projects[0].Name);

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
        
        // file 2MB
        var fileContent = new StringBuilder();
        const int nRows = 1000 * 1000;
        for (var i = 0; i < nRows; i++)
        {
            fileContent.AppendLine("A");
        }
        
        var team = await _factory.TeamService().GetOrCreateTeam(teamName);

        try
        {
            await File.WriteAllTextAsync(tempFilePath, fileContent.ToString());

            const int chunkSize = 1024 * 1024; // 64 KB chunks

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