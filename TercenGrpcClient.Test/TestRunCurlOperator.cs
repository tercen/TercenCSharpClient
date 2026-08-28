namespace TercenGrpcClient.Test;

using Tercen;
using TercenGrpcClient;
using TercenGrpcClient.client;
using TercenGrpcClient.extensions;

/// <summary>
/// Exercises the <see cref="CurlOperatorExtension.DownloadFiles"/> utility against
/// a live Tercen instance: it runs the native "File Downloader" (curl) operator as
/// a single RunComputationTask that downloads a blob (by SAS URL) on a worker and
/// materializes it as a FileDocument — no bytes through main, no caller-supplied
/// md5 (Azure blob integrity is server-verified via CRC64).
///
/// Replicates the Ingenix layout (project / &lt;analysisId&gt; / events.zip) and
/// asserts the returned FileDocument has the right name and sits in a project-root
/// folder named by the analysis id.
///
/// Env (same convention as TestRunWorkflow): TERCEN_URI / TERCEN_TENANT /
/// TERCEN_USERNAME / TERCEN_PASSWORD. EVENTS_URL is the blob URL to download
/// (must be reachable FROM THE WORKER); pass a real Azure blob SAS URL for a
/// faithful Sartorius rehearsal. EVENTS_MD5_BASE64 is optional (only needed for a
/// pre-1.0.3 operator, which requires + verifies it).
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
    private const string DownloadName = "events.zip";

    private const string DefaultEventsUrl =
        "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv";

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
        // not fail the test itself (the proof is the download result).
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

    [TestMethod]
    public async System.Threading.Tasks.Task TestCurlDownloadsFileOnWorker()
    {
        var eventsUrl = Environment.GetEnvironmentVariable("EVENTS_URL");
        if (string.IsNullOrEmpty(eventsUrl)) eventsUrl = DefaultEventsUrl;

        // Optional: only a pre-1.0.3 operator needs an md5. Default: none.
        var eventsMd5 = Environment.GetEnvironmentVariable("EVENTS_MD5_BASE64");

        var team = await _factory.TeamService().GetOrCreateTeam(TestTeamId);
        var project = await _factory.GetOrCreateProject("curl_operator_proof", team.Id);

        // Simulate an Ingenix analysis id: the file lands in a top-level folder
        // named by it (project / <analysisId> / events.zip), matching Ingenix.
        // Fresh each run so curl actually downloads (it dedups by path).
        var analysisId = Guid.NewGuid().ToString();
        var downloadPath = $"{analysisId}/{DownloadName}";

        // The whole download, through the utility: one worker task, no bytes via main.
        var downloaded = await _factory.DownloadFiles(
            project.Id,
            new[] { new CurlFile(downloadPath, eventsUrl, string.IsNullOrEmpty(eventsMd5) ? null : eventsMd5) });

        Assert.AreEqual(1, downloaded.Count);
        var fileDoc = downloaded[0];
        Console.WriteLine($"FileDocument: id={fileDoc.Id} name={fileDoc.Name} folderId={fileDoc.FolderId} size={fileDoc.Size}");

        // Correct name.
        Assert.AreEqual(DownloadName, fileDoc.Name,
            "downloaded FileDocument should be named events.zip");

        // Correct folder: a project-root folder named by the analysis id (Ingenix layout).
        var folders = (await _factory.ProjectDocumentService().FindProjectObjects(project.Id))
            .Where(d => d.ObjectCase == EProjectDocument.ObjectOneofCase.Folderdocument)
            .Select(d => d.Folderdocument)
            .ToList();

        var folder = folders.FirstOrDefault(f => f.Id == fileDoc.FolderId);
        Console.WriteLine(folder == null
            ? "folder NOT found"
            : $"folder: id={folder.Id} name={folder.Name} parent='{folder.FolderId}'");

        Assert.IsNotNull(folder, "the file should be inside a folder");
        Assert.AreEqual(analysisId, folder!.Name,
            "the file should be in a folder named by the analysis id");
        Assert.IsTrue(folder.FolderId.Length == 0,
            "the analysis folder should be at the project root (no parent), as Ingenix creates it");
    }
}
