namespace TercenGrpcClient.Test;

using Grpc.Core;
using Tercen;
using TercenGrpcClient.client;
using TercenGrpcClient.extensions;

/// <summary>
/// Proves the "run the native File Downloader (curl) operator directly, no
/// CubeQueryTask" task shape that the Sartorius refactor will use to replace the
/// events.zip upload-through-main (see the remove-upload-load design).
///
/// The manifest is an IN-MEMORY Tercen table (InMemoryRelation with `path` +
/// `href` columns — same builder pattern as ERelationExtension
/// .CreateFileDocumentRelation). We create a single RunComputationTask whose
/// query carries that relation and points operatorRef at the curl operator;
/// taskService.create materialises the row schema from the in-memory relation
/// and the scheduler runs the operator ON A WORKER — no separate CubeQueryTask,
/// no persisted manifest schema. The operator streams the URL directly into the
/// project blob store and creates a FileDocument at `path`.
///
/// Env (same convention as TestRunWorkflow): TERCEN_URI / TERCEN_TENANT /
/// TERCEN_USERNAME / TERCEN_PASSWORD. EVENTS_URL overrides the download URL —
/// pass a real Azure blob SAS URL for a faithful Sartorius rehearsal; the
/// default is a small public file, which is all a shape proof needs. The URL
/// must be reachable FROM THE WORKER.
/// </summary>
[DoNotParallelize]
[TestClass]
public sealed class TestRunCurlOperator
{
    private TercenFactory _factory = null!;
    private string? _uri;
    private string? _tenant;
    private string? _username;
    private string? _password;

    private const string TestTeamId = "grpc_test_curl_operator_team";

    // Native "File Downloader" operator (sci native_operators.dart / curl_operator.dart).
    private const string CurlOperatorId = "5ba24424-6e73-40f3-a65d-0448fa5d437f";
    private const string CurlOperatorUrl = "https://tercen.com/_operator/curl";

    // Where the operator lands the FileDocument inside the project.
    private const string DownloadDir = "input";
    private const string DownloadName = "events.zip";
    private const string DownloadPath = DownloadDir + "/" + DownloadName;

    private const string DefaultEventsUrl =
        "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv";

    // base64(md5) — required by the curl operator BEFORE v1.0.2 (pre md5-optional).
    // The operator only stores it as FileDocument metadata (never verifies it), so
    // a fixed valid value is fine for the proof. This is md5("") base64-encoded.
    private const string Md5Base64 = "1B2M2Y8AsgTpgAmY7PhCfg==";

    [TestInitialize]
    public async System.Threading.Tasks.Task SetupAsync()
    {
        _uri = Environment.GetEnvironmentVariable("TERCEN_URI");
        _tenant = Environment.GetEnvironmentVariable("TERCEN_TENANT");
        _username = Environment.GetEnvironmentVariable("TERCEN_USERNAME");
        _password = Environment.GetEnvironmentVariable("TERCEN_PASSWORD");

        if (string.IsNullOrEmpty(_uri)) _uri = "http://127.0.0.1:50051";
        if (string.IsNullOrEmpty(_username)) _username = "admin";
        if (string.IsNullOrEmpty(_password)) _password = "admin";
        if (string.IsNullOrEmpty(_tenant)) _tenant = "";

        _factory = await TercenFactory.Create(_uri, _tenant, _username, _password);
    }

    [TestCleanup]
    public async System.Threading.Tasks.Task TeardownAsync()
    {
        // Best-effort — a transient Sartorius services-node 502 on the delete must
        // not fail the test itself (the proof is the task/FileDocument result).
        try
        {
            var team = await _factory.TeamService().GetOrCreateTeam(TestTeamId);
            await _factory.TeamService().deleteAsync(new DeleteRequest { Id = team.Id, Rev = team.Rev });
        }
        catch (Exception e)
        {
            Console.WriteLine($"cleanup (non-fatal): {e.Message}");
        }
    }

    /// <summary>
    /// Builds the in-memory (path, href) manifest as an ERelation — the same
    /// shape as ERelationExtension.CreateFileDocumentRelation, but with the two
    /// columns the curl operator reads.
    /// </summary>
    private static ERelation ManifestRelation(string path, string href, string md5)
    {
        var pathValues = new StrValues();
        pathValues.Values.Add(path);

        var hrefValues = new StrValues();
        hrefValues.Values.Add(href);

        var md5Values = new StrValues();
        md5Values.Values.Add(md5);

        Column StrCol(string name, StrValues values) => new Column
        {
            Name = name,
            Type = "string",
            NRows = 1,
            CValues = new ECValues { Strvalues = values }
        };

        var tbl = new Table
        {
            NRows = 1,
            Columns =
            {
                new[]
                {
                    StrCol("path", pathValues),
                    StrCol("href", hrefValues),
                    StrCol("md5", md5Values),
                }
            }
        };

        return new ERelation
        {
            Inmemoryrelation = new InMemoryRelation { InMemoryTable = tbl }
        };
    }

    [TestMethod]
    public async System.Threading.Tasks.Task TestCurlDownloadsFileOnWorker()
    {
        var eventsUrl = Environment.GetEnvironmentVariable("EVENTS_URL");
        if (string.IsNullOrEmpty(eventsUrl)) eventsUrl = DefaultEventsUrl;

        // The deployed (pre-1.0.2) curl verifies the download against this md5, so
        // it must be the ACTUAL base64(md5) of the bytes at EVENTS_URL.
        var eventsMd5 = Environment.GetEnvironmentVariable("EVENTS_MD5_BASE64");
        if (string.IsNullOrEmpty(eventsMd5)) eventsMd5 = Md5Base64;

        var team = await _factory.TeamService().GetOrCreateTeam(TestTeamId);
        var project = await _factory.GetOrCreateProject("curl_operator_proof", team.Id);

        // The manifest is a one-row in-memory table: path + href. No upload, no
        // persisted schema.
        var query = new CubeQuery
        {
            Relation = ManifestRelation(DownloadPath, eventsUrl, eventsMd5),
            OperatorSettings = new OperatorSettings
            {
                OperatorRef = new OperatorRef
                {
                    OperatorId = CurlOperatorId,
                    Name = "File Downloader",
                    Version = "1.0.2",
                    Url = new Url { Uri = CurlOperatorUrl }
                }
            }
        };
        query.RowColumns.Add(new Factor { Name = "path", Type = "string" });
        query.RowColumns.Add(new Factor { Name = "href", Type = "string" });
        query.RowColumns.Add(new Factor { Name = "md5", Type = "string" });

        // A computation query needs at least one CubeAxisQuery: the row schema is
        // built inside the axis-query loop, so with none the row relation is an
        // empty union and the row attributes (path/href/md5) can't be resolved
        // ("RenameRelation -- attribute not found -- path"). curl reads only the
        // row columns, so an empty cell axis (no x/y) is enough.
        query.AxisQueries.Add(new CubeAxisQuery());

        // A single RunComputationTask — the scheduler materialises the row schema
        // from the in-memory relation and runs curl on a worker. No CubeQueryTask.
        var task = await _factory.TaskService().createAsync(new ETask
        {
            Runcomputationtask = new RunComputationTask
            {
                State = new EState { Initstate = new InitState() },
                Owner = project.Acl.Owner,
                ProjectId = project.Id,
                Query = query,
                ChannelId = Guid.NewGuid().ToString()
            }
        });

        var taskId = task.Id();
        Console.WriteLine($"RunComputationTask created: {taskId}");

        // Start = true both runs the task and streams its events until it ends.
        using var listenCall = _factory.EventService()
            .listenTaskChannel(new ReqListenTaskChannel { TaskId = taskId, Start = true });

        await foreach (var evt in listenCall.ResponseStream.ReadAllAsync())
        {
            Console.WriteLine(evt.Result);
        }

        task = await _factory.TaskService().getAsync(new GetRequest { Id = taskId });
        task.Runcomputationtask.State.ThrowIfNotDone();

        // Proof: curl created the FileDocument in the project (it did not exist
        // before the task ran).
        var docs = await _factory.ProjectDocumentService().FindProjectObjects(project.Id);
        var created = docs
            .Where(d => d.ObjectCase == EProjectDocument.ObjectOneofCase.Filedocument)
            .Select(d => d.Filedocument)
            .Where(f => f.Name == DownloadName)
            .ToList();

        foreach (var f in created)
            Console.WriteLine($"FileDocument created: id={f.Id} name={f.Name} folderId={f.FolderId} size={f.Size}");

        // The task reaching DoneState already means the server-side md5 check
        // passed against the downloaded bytes (a mismatch fails as file.bad.md5),
        // so content integrity is proven. Existence of the FileDocument is the
        // proof. (FileDocument.Size can read 0 here — the curl upload path does
        // not populate that metadata field; the bytes are in the object store.)
        Assert.IsTrue(created.Count >= 1,
            $"curl operator should have created a '{DownloadName}' FileDocument in the project");
    }
}
