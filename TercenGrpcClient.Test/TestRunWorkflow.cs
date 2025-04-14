namespace TercenGrpcClient.Test;

using Grpc.Core;
using Tercen;

[DoNotParallelize]
[TestClass]
public sealed class TestRunWorkflow
{
    private TercenFactory _factory = null!;
    private string? _uri;
    private string? _tenant;
    private string? _username;
    private string? _password;
    private string? _gitPat;
    private const string TestRunWorkflowTeamId = "grpc_test_run_workflow_team";
    private const string TestLibraryTeamId = "grpc_test_ibrary_team";
    private const string TemplateProjectName = "grpc_test_workflow_template@1.0.1";
    private const string TemplateUri = "https://github.com/tercen/grpc_test_workflow_template";
    private const string TemplateVersion = "1.0.1";
    
    [TestInitialize]
    public async System.Threading.Tasks.Task SetupAsync()
    {
        _uri = Environment.GetEnvironmentVariable("TERCEN_URI");
        _tenant = Environment.GetEnvironmentVariable("TERCEN_TENANT");
        _username = Environment.GetEnvironmentVariable("TERCEN_USERNAME");
        _password = Environment.GetEnvironmentVariable("TERCEN_PASSWORD");
        _gitPat = Environment.GetEnvironmentVariable("GIT_TOKEN");

        if (string.IsNullOrEmpty(_uri))
        {
            _uri = "http://127.0.0.1:50051";
        }

        if (string.IsNullOrEmpty(_username))
        {
            _username = "admin";
        }

        if (string.IsNullOrEmpty(_password))
        {
            _password = "admin";
        }

        if (string.IsNullOrEmpty(_tenant))
        {
            _tenant = "";
        }

        if (string.IsNullOrEmpty(_gitPat))
        {
            _gitPat = "";
        }

        _factory = await TercenFactory.Create(
            _uri,
            _tenant,
            _username,
            _password);

        var teamLibrary = await GetOrCreateTestLibraryTeam(TestLibraryTeamId);
        var project = await GetOrCreateProject(TemplateProjectName, teamLibrary.Id);
        await InstallWorkflowTemplate(project, TemplateUri, TemplateVersion);
        
        var team = await GetOrCreateTeam(TestRunWorkflowTeamId);

    }

    [TestCleanup]
    public async System.Threading.Tasks.Task TeardownAsync()
    {
        var teamLibrary = await GetOrCreateTestLibraryTeam(TestLibraryTeamId);
        await _factory.TeamService().deleteAsync(new DeleteRequest { Id = teamLibrary.Id, Rev = teamLibrary.Rev });
        var team = await GetOrCreateTestLibraryTeam(TestRunWorkflowTeamId);
        await _factory.TeamService().deleteAsync(new DeleteRequest { Id = team.Id, Rev = team.Rev });
    }

    private async Task<bool> HasWorkflowTemplate(string templateGitUri, string version)
    {
        var response = await _factory.DocumentService()
            .getLibraryAsync(new ReqGetLibrary { DocTypes = { "Workflow" }, TeamIds = { TestLibraryTeamId } });

        var templates = response.List
            .Where(doc => doc.Projectdocument is not null)
            .Where(doc =>
                doc.Projectdocument.Url.Uri.Equals(templateGitUri) &&
                doc.Projectdocument.Version.Equals(version)).ToList();

        return templates.Count > 0;
    }

    private async Task<ProjectDocument> GetWorkflowTemplate(string templateGitUri, string version)
    {
        var response = await _factory.DocumentService()
            .getLibraryAsync(new ReqGetLibrary { DocTypes = { "Workflow" } });

        var templates = response.List.Where(doc => doc.Projectdocument != null).Where(doc =>
            doc.Projectdocument.Url.Uri == templateGitUri &&
            doc.Projectdocument.Version == version).ToList();

        return templates.First().Projectdocument;
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

    private async Task<Team> GetOrCreateTestLibraryTeam(string teamName)
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
                Meta =
                {
                    new[]
                    {
                        new Pair
                        {
                            Key = "is.library",
                            Value = "true"
                        }
                    }
                }
            }
        };

        team = await _factory.TeamService().createAsync(team);

        return team.Team;
    }

    private async Task<EProject> GetOrCreateProject(string projectName, string teamId)
    {
        var request = new KeyRangeRequest { Name = "Project/findByTeamAndIsPublicAndLastModifiedDate" };

        request.StartKeys.Add(new IndexKeyValue { IndexField = "acl.owner", StringValue = teamId });
        request.StartKeys.Add(new IndexKeyValue { IndexField = "isPublic", BoolValue = true });
        request.StartKeys.Add(new IndexKeyValue { IndexField = "lastModifiedDate.value", StringValue = "2100" });

        request.EndKeys.Add(new IndexKeyValue { IndexField = "acl.owner", StringValue = teamId });
        request.EndKeys.Add(new IndexKeyValue { IndexField = "isPublic", BoolValue = true });
        request.EndKeys.Add(new IndexKeyValue { IndexField = "lastModifiedDate.value", StringValue = "" });

        request.Limit = 1;

        var response = await _factory.ProjectService().findKeyRangeAsync(request);
        if (response.List.Count > 0)
        {
            return response.List[0];
        }

        var project = new EProject()
        {
            Project = new Project()
            {
                Name = projectName,
                IsPublic = true,
                Acl = new Acl()
                {
                    Owner = teamId,
                }
            }
        };
        project = await _factory.ProjectService().createAsync(project);
        return project;
    }


    private async Task<ProjectDocument> InstallWorkflowTemplate(EProject project, string templateGitUri, string version)
    {
        var task = new ETask
        {
            Gitprojecttask = new GitProjectTask
            {
                Owner = project.Project.Acl.Owner,
                State = new EState
                {
                    Initstate = new InitState()
                },
                Meta =
                {
                    new[]
                    {
                        new Pair
                        {
                            Key = "PROJECT_ID",
                            Value = project.Project.Id
                        },
                        new Pair
                        {
                            Key = "PROJECT_REV",
                            Value = project.Project.Rev
                        },
                        new Pair
                        {
                            Key = "GIT_ACTION",
                            Value = "reset/pull"
                        },
                        new Pair
                        {
                            Key = "GIT_PAT",
                            Value = _gitPat
                        },
                        new Pair
                        {
                            Key = "GIT_URL",
                            Value = templateGitUri
                        },
                        new Pair
                        {
                            Key = "GIT_BRANCH",
                            Value = "main"
                        },
                        new Pair
                        {
                            Key = "GIT_MESSAGE",
                            Value = ""
                        },
                        new Pair
                        {
                            Key = "GIT_TAG",
                            Value = version
                        }
                    }
                }
            }
        };
        var result = await _factory.TaskService().createAsync(task);

        await _factory.TaskService().runTaskAsync(new ReqRunTask
        {
            TaskId = result.Gitprojecttask.Id
        });

        await _factory.TaskService().waitDoneAsync(new ReqWaitDone()
        {
            TaskId = result.Gitprojecttask.Id
        });

        Assert.IsTrue(await HasWorkflowTemplate(templateGitUri, version));
        return await GetWorkflowTemplate(templateGitUri, version);
    }


    private async Task<FileDocument> UploadFile(string filePath, FileDocument fileDocument)
    {
        const int chunkSize = 1024 * 64;
        using var uploadCall = _factory.FileService().upload();
        await using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read);
        var buffer = new byte[chunkSize];
        int bytesRead;
        var firstChunk = true;

        // Read file in chunks and send to server
        while ((bytesRead = await fileStream.ReadAsync(buffer)) > 0)
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

        return resp.Result.Filedocument;
    }

    [TestMethod]
    public async System.Threading.Tasks.Task TestRunTemplate()
    {
        var team = await GetOrCreateTeam(TestRunWorkflowTeamId);
        var myProject = await GetOrCreateProject("test_project", team.Id);
        
        var workflowTemplate = await GetWorkflowTemplate(TemplateUri, TemplateVersion);
        
        var response = await _factory.ProjectDocumentService()
            .cloneProjectDocumentAsync(new ReqCloneProjectDocument
                { ProjectId = myProject.Project.Id, DocumentId = workflowTemplate.Id });

        var workflow = response.Result.Workflow;

        var fileDocument = new FileDocument
        {
            Name = "crabs-long.csv",
            ProjectId = myProject.Project.Id,
            Acl = new Acl()
            {
                Owner = myProject.Project.Acl.Owner,
            }
        };

        var filePath = Path.Combine("Resources", "crabs-long.csv");
        fileDocument = await UploadFile(filePath, fileDocument);

        var tableStep = workflow.Steps.First(step => step.Tablestep != null).Tablestep;

        tableStep.Model.Relation = CreateFileDocumentRelation(fileDocument);
        tableStep.State.TaskState.Donestate = new DoneState();

        await _factory.WorkflowService().updateAsync(new EWorkflow { Workflow = workflow });

        var task = await _factory.TaskService().createAsync(new ETask
        {
            Runworkflowtask = new RunWorkflowTask
            {
                State = new EState
                {
                    Initstate = new InitState()
                },
                Owner = myProject.Project.Acl.Owner,
                ProjectId = myProject.Project.Id,
                WorkflowId = workflow.Id,
                ChannelId = Guid.NewGuid().ToString()
            }
        });

        await _factory.TaskService().runTaskAsync(new ReqRunTask
        {
            TaskId = task.Runworkflowtask.Id
        });

        await _factory.TaskService().waitDoneAsync(new ReqWaitDone
        {
            TaskId = task.Runworkflowtask.Id
        });

        task = await _factory.TaskService().getAsync(new GetRequest { Id = task.Runworkflowtask.Id });

        Assert.IsTrue(task.Runworkflowtask.State.Donestate is not null);

        workflow = (await _factory.WorkflowService()
                .getAsync(new GetRequest { Id = workflow.Id }))
            .Workflow;

        var plotDataStep = workflow.Steps.First(step => step.Datastep is { Name: "Plot" });

        var simpleRelationIds = new List<string>();

        VisitSimpleRelations(plotDataStep.Datastep.ComputedRelation, CollectSimpleRelations);

        Assert.IsTrue(simpleRelationIds.Count == 1);

        const string fileName = "Tercen_Plot.png";
        var outputPath = Path.Combine(Environment.CurrentDirectory, fileName);
        // Console.WriteLine(outputPath);
        await using var fileStream = File.Create(outputPath);

        using var call = _factory.TableSchemaService()
            .getFileMimetypeStream(new ReqGetFileMimetypeStream
                { TableId = simpleRelationIds.First(), Filename = fileName });

        await foreach (var chunk in call.ResponseStream.ReadAllAsync())
        {
            await fileStream.WriteAsync(chunk.Result.ToByteArray());
        }

        void CollectSimpleRelations(ERelation relation)
        {
            if (relation.Simplerelation != null)
            {
                simpleRelationIds.Add(relation.Simplerelation.Id);
            }
        }
    }

    private static ERelation CreateFileDocumentRelation(FileDocument fileDocument)
    {
        var tbl = new Table
        {
            NRows = 1,
            Columns =
            {
                new[]
                {
                    new Column
                    {
                        Name = "documentId",
                        Type = "string",
                        CValues = new ECValues
                        {
                            Strvalues = new StrValues
                            {
                                Values = { new[] { Guid.NewGuid().ToString() } }
                            }
                        }
                    },
                    new Column
                    {
                        Name = ".documentId",
                        Type = "string",
                        CValues = new ECValues
                        {
                            Strvalues = new StrValues
                            {
                                Values = { new[] { fileDocument.Id } }
                            }
                        }
                    },
                }
            }
        };

        var fileDocumentRelation = new ERelation
        {
            Inmemoryrelation = new InMemoryRelation
            {
                InMemoryTable = tbl,
            }
        };
        return fileDocumentRelation;
    }

    private static void VisitSimpleRelations(ERelation relation, RelationCallbackDelegate callback)
    {
        switch (relation.ObjectCase)
        {
            case ERelation.ObjectOneofCase.Compositerelation:
                VisitSimpleRelations(relation.Compositerelation.MainRelation, callback);
                foreach (var joinOperator in relation.Compositerelation.JoinOperators)
                {
                    VisitSimpleRelations(joinOperator.RightRelation, callback);
                }

                break;
            case ERelation.ObjectOneofCase.None:
                break;
            case ERelation.ObjectOneofCase.Distinctrelation:
                VisitSimpleRelations(relation.Distinctrelation.Relation, callback);
                break;
            case ERelation.ObjectOneofCase.Gatherrelation:
                break;
            case ERelation.ObjectOneofCase.Groupbyrelation:
                VisitSimpleRelations(relation.Groupbyrelation.Relation, callback);
                break;
            case ERelation.ObjectOneofCase.Inmemoryrelation:
                break;
            case ERelation.ObjectOneofCase.Pairwiserelation:
                break;
            case ERelation.ObjectOneofCase.Rangerelation:
                VisitSimpleRelations(relation.Rangerelation.Relation, callback);
                break;
            case ERelation.ObjectOneofCase.Referencerelation:
                break;
            case ERelation.ObjectOneofCase.Relation:
                break;
            case ERelation.ObjectOneofCase.Renamerelation:
                VisitSimpleRelations(relation.Renamerelation.Relation, callback);
                break;
            case ERelation.ObjectOneofCase.Selectpairwiserelation:
                break;
            case ERelation.ObjectOneofCase.Simplerelation:
                callback(relation);
                break;
            case ERelation.ObjectOneofCase.Tablerelation:
                break;
            case ERelation.ObjectOneofCase.Unionrelation:
                break;
            case ERelation.ObjectOneofCase.Whererelation:
                VisitSimpleRelations(relation.Whererelation.Relation, callback);
                break;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }
}

public delegate void RelationCallbackDelegate(ERelation relation);