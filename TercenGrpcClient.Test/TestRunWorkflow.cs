namespace TercenGrpcClient.Test;

using Grpc.Core;
using Tercen;
using TercenGrpcClient.client;
using TercenGrpcClient.extensions;

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
    
    private const string TemplateProjectName = "grpc_test_workflow_template@1.1.1";
    private const string TemplateUri = "https://github.com/tercen/grpc_test_workflow_template";
    private const string TemplateVersion = "1.1.1";

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

        var teamLibrary = await _factory.TeamService().GetOrCreateTestLibraryTeam(TestLibraryTeamId);
        var project = await _factory.GetOrCreateProject(TemplateProjectName, teamLibrary.Id);
        await _factory.TaskService().InstallWorkflowTemplate(project, TemplateUri, TemplateVersion,_gitPat);
        var team = await _factory.TeamService().GetOrCreateTeam(TestRunWorkflowTeamId);
    }

    [TestCleanup]
    public async System.Threading.Tasks.Task TeardownAsync()
    {
        // var teamLibrary = await _factory.TeamService().GetOrCreateTestLibraryTeam(TestLibraryTeamId);
        // await _factory.TeamService().deleteAsync(new DeleteRequest { Id = teamLibrary.Id, Rev = teamLibrary.Rev });
        var team = await _factory.TeamService().GetOrCreateTestLibraryTeam(TestRunWorkflowTeamId);
        await _factory.TeamService().deleteAsync(new DeleteRequest { Id = team.Id, Rev = team.Rev });
    }
  
    [TestMethod]
    public async System.Threading.Tasks.Task TestRunTemplate()
    {
        var team = await _factory.TeamService().GetOrCreateTeam(TestRunWorkflowTeamId);
        var myProject = await _factory.GetOrCreateProject("test_project", team.Id);

        var workflowTemplate = await _factory.DocumentService().GetWorkflowTemplate(TemplateUri, TemplateVersion);
        
        Assert.IsNotNull(workflowTemplate);

        var response = await _factory.ProjectDocumentService()
            .cloneProjectDocumentAsync(new ReqCloneProjectDocument
                { ProjectId = myProject.Id, DocumentId = workflowTemplate.Id });

        var workflow = response.Result.Workflow;

        var crabsFileDocument = new FileDocument
        {
            Name = "crabs-long.csv",
            ProjectId = myProject.Id,
            Acl = new Acl()
            {
                Owner = myProject.Acl.Owner,
            }
        };

        crabsFileDocument = await _factory.FileService()
            .UploadFile(Path.Combine("Resources", "crabs.csv"), crabsFileDocument);

        var crabsTableStep = workflow.Steps.First(step => step.Tablestep is { Name: "crabs.csv" }).Tablestep;

        crabsTableStep.Model.Relation = ERelationExtension.CreateFileDocumentRelation([crabsFileDocument]);
        crabsTableStep.State.TaskState.Donestate = new DoneState();
        
        var genderFileDocument = new FileDocument
        {
            Name = "crabs-gender.csv",
            ProjectId = myProject.Id,
            Acl = new Acl()
            {
                Owner = myProject.Acl.Owner,
            }
        };

        genderFileDocument = await _factory.FileService()
            .UploadFile(Path.Combine("Resources", "crabs-gender.csv"), genderFileDocument);

        var genderTableStep = workflow.Steps.First(step => step.Tablestep is { Name: "crabs-gender.csv" }).Tablestep;

        genderTableStep.Model.Relation = ERelationExtension.CreateFileDocumentRelation([genderFileDocument]);
        genderTableStep.State.TaskState.Donestate = new DoneState();

        await _factory.WorkflowService().updateAsync(new EWorkflow { Workflow = workflow });

        var task = await _factory.TaskService().createAsync(new ETask
        {
            Runworkflowtask = new RunWorkflowTask
            {
                State = new EState
                {
                    Initstate = new InitState()
                },
                Owner = workflow.Acl.Owner,
                ProjectId = workflow.ProjectId,
                WorkflowId = workflow.Id,
                ChannelId = Guid.NewGuid().ToString()
            }
        });


        using var listenCall = _factory.EventService()
            .listenTaskChannel(new ReqListenTaskChannel()
                { TaskId = task.Id(), Start = true });

        await foreach (var evt in listenCall.ResponseStream.ReadAllAsync())
        {
            Console.WriteLine(evt.Result);

            switch (evt.Result.ObjectCase)
            {
                case ETaskEvent.ObjectOneofCase.Tasklogevent:
                case ETaskEvent.ObjectOneofCase.Taskprogressevent:
                case ETaskEvent.ObjectOneofCase.Taskstateevent:
                    break;

                case ETaskEvent.ObjectOneofCase.Taskevent:
                case ETaskEvent.ObjectOneofCase.Taskdataevent:
                case ETaskEvent.ObjectOneofCase.None:
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        // await _factory.TaskService().runTaskAsync(new ReqRunTask
        // {
        //     TaskId = task.Runworkflowtask.Id
        // });
        //
        // await _factory.TaskService().waitDoneAsync(new ReqWaitDone
        // {
        //     TaskId = task.Runworkflowtask.Id
        // });


        task = await _factory.TaskService().getAsync(new GetRequest { Id = task.Runworkflowtask.Id });
        task.Runworkflowtask.State.ThrowIfNotDone();

        workflow = (await _factory.WorkflowService()
                .getAsync(new GetRequest { Id = workflow.Id }))
            .Workflow;

        var plotDataStep = workflow.Steps.First(step => step.Datastep is { Name: "Plot" });

        var simpleRelationIds = plotDataStep.Datastep.ComputedRelation.GetSimpleRelationIds().ToList();

        Assert.IsTrue(simpleRelationIds.Count == 1);

        var schemaId = simpleRelationIds.First();
        var schema = await _factory.TableSchemaService().getAsync(new GetRequest { Id = schemaId });

        var filenameColumn = schema.Columns().First(c => c.Name() == "filename");
        var mimetypeColumn = schema.Columns().First(c => c.Name() == "mimetype");

        var dataFrame = await _factory.TableSchemaService().Select(new ReqStreamTable
        {
            TableId = schemaId,
            Cnames = { filenameColumn.Name(), mimetypeColumn.Name() },
            Limit = schema.NRows(),
            Offset = 0
        });

        // Console.WriteLine(dataFrame);

        Assert.IsTrue(dataFrame.Rows.Count > 0);

        var fileName = dataFrame["filename"][0] as string;

        Assert.IsNotNull(fileName);

        var outputPath = Path.Combine(Environment.CurrentDirectory, fileName);
        // Console.WriteLine(outputPath);
        await using var fileStream = File.Create(outputPath);

        using var call = _factory.TableSchemaService()
            .getFileMimetypeStream(new ReqGetFileMimetypeStream
                { TableId = schemaId, Filename = fileName });

        await foreach (var chunk in call.ResponseStream.ReadAllAsync())
        {
            await fileStream.WriteAsync(chunk.Result.ToByteArray());
        }
    }
}