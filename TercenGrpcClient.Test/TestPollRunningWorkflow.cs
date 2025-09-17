using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using Google.Protobuf;

namespace TercenGrpcClient.Test;

using Grpc.Core;
using Tercen;
using TercenGrpcClient.client;
using TercenGrpcClient.extensions;

[DoNotParallelize]
[TestClass]
public sealed class TestPollRunningWorkflow
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
        await _factory.TaskService().InstallWorkflowTemplate(project, TemplateUri, TemplateVersion, _gitPat);
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
        var myFolder = await _factory.FolderService().GetOrCreateFolder(myProject.Id, "myExperiment/myPack");

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
            FolderId = myFolder.Id,
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
            FolderId = myFolder.Id,
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
        var exportDS = workflow.Steps.First(step => step.Datastep is { Name: "Export Table" });

        // set operator property
        var plotDS = workflow.Steps.First(step => step.Datastep is { Name: "Plot" });
        var propertyValue = plotDS.Datastep.Model.OperatorSettings.OperatorRef.PropertyValues.First(prop =>
            prop.Name == "plot.width");
        propertyValue.Value = "1200";
        
        propertyValue = plotDS.Datastep.Model.OperatorSettings.OperatorRef.PropertyValues.First(prop =>
            prop.Name == "plot.height");
        propertyValue.Value = "600";


        // move the workflow in myFolder
        workflow.FolderId = myFolder.Id;
        
        // save the workflow modifications
        var revResponse = await _factory.WorkflowService().updateAsync(new EWorkflow { Workflow = workflow });
        // update revision number
        workflow.Rev = revResponse.Rev;

        // If stepsToReset and stepsToRun are both empty, all steps will be run.
        // Reset plot step to make sure the step will be rerun with the plot settings
        var stepsToReset = new List<string>{ plotDS.Id() };
        var stepsToRun = new List<string>{ plotDS.Id(), exportDS.Id() };
        
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
                ChannelId = Guid.NewGuid().ToString(),
                Meta = { new Pair { Key = "channel.persistent", Value = "true" } },
                StepsToReset = { stepsToReset },
                StepsToRun = { stepsToRun }
            }
        });

        var startDate = DateTime.UtcNow;
        var maxDate = startDate.AddMinutes(5);

        await _factory.TaskService().runTaskAsync(new ReqRunTask
        {
            TaskId = task.Runworkflowtask.Id
        });
        
        // start pooling the EventService 
        var isComplete = false;

        while (!isComplete)
        {
            if (maxDate < DateTime.UtcNow)
            {
                throw new Exception("Timeout expired");
            }

            Thread.Sleep(1000);

            var endDate = startDate.AddYears(1);

            var events = await _factory.EventService()
                .FindPersistentChannelEventByChannelAndDate(task.Runworkflowtask.ChannelId,
                    startDate.ToIso8601String(true),
                    endDate.ToIso8601String(true));

            var firstEvent = events.FirstOrDefault();
            if (firstEvent != null)
            {
                var newStartDate = DateTime.Parse(firstEvent.Date.Value).ToUniversalTime();
                Assert.IsTrue(newStartDate.Subtract(startDate).TotalMicroseconds >= 0);
                startDate = newStartDate.Subtract(startDate).TotalMicroseconds == 0
                    ? newStartDate.AddMicroseconds(1)
                    : newStartDate;
            }
            
            // Progress events generate by the RunWorkflowTask
            var progressEvents = events
                .Where(evt => evt.Event.ObjectCase == EEvent.ObjectOneofCase.Taskprogressevent)
                .Select(evt => evt.Event.Taskprogressevent)
                .Where(evt => evt.TaskId == task.Runworkflowtask.Id)
                .ToList();

            foreach (var evt in progressEvents)
            {
                Console.WriteLine(evt);
            }
            
            // Tasks that have been completed
            var finalTaskStateEvents = events
                .Where(evt => evt.Event.ObjectCase == EEvent.ObjectOneofCase.Taskstateevent)
                .Select(evt => evt.Event.Taskstateevent)
                .Where(evt => evt.State.IsFinalState())
                .ToList();

            // Tasks that have failed
            var failedTaskStateEvents = finalTaskStateEvents
                .Where(evt => !evt.State.IsDone())
                .ToList();

            foreach (var evt in failedTaskStateEvents)
            {
                //Console.WriteLine(formatter.Format(evt));
                evt.State.ThrowIfNotDone();
            }

            // Tasks that have succeeded
            var doneTaskStateEvents = finalTaskStateEvents
                .Where(evt => evt.State.IsDone())
                .ToList();

            var plotDataStep = workflow.Steps.First(step => step.Datastep is { Name: "Plot" });

            // For the same stepId multiple tasks can be run, CubeQueryTask, RunComputationTask ...
            var plotEvts = doneTaskStateEvents
                .Where(evt => evt.StepId() == plotDataStep.Id());

            foreach (var plotEvt in plotEvts)
            {
                var childTask = await _factory.TaskService()
                    .getAsync(new GetRequest { Id = plotEvt.TaskId });

                if (childTask.ObjectCase != ETask.ObjectOneofCase.Runcomputationtask) continue;

                var simpleRelationIds =
                    childTask.Runcomputationtask.ComputedRelation.GetSimpleRelationIds().ToList();
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

            isComplete = finalTaskStateEvents
                .Any(evt => evt.TaskId == task.Runworkflowtask.Id);
        }

        task = await _factory.TaskService().getAsync(new GetRequest { Id = task.Runworkflowtask.Id });
        task.Runworkflowtask.State.ThrowIfNotDone();
    }
}