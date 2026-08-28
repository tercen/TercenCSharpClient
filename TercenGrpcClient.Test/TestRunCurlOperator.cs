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
    public async System.Threading.Tasks.Task TestCurlDownloadsFilesOnWorker()
    {
        // One OR MORE download URLs. EVENTS_URLS is a ';'-separated list — exercises
        // the batched multi-file path (N manifest rows -> one worker task). Falls
        // back to EVENTS_URL, then a small public default. Pass real Azure blob SAS
        // URLs for a faithful run.
        var urlsEnv = Environment.GetEnvironmentVariable("EVENTS_URLS");
        var single = Environment.GetEnvironmentVariable("EVENTS_URL");
        var urls = !string.IsNullOrEmpty(urlsEnv)
            ? urlsEnv.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            : new[] { string.IsNullOrEmpty(single) ? DefaultEventsUrl : single };

        // Optional md5 (pre-1.0.3 only), applied to every file. Default: none.
        var eventsMd5 = Environment.GetEnvironmentVariable("EVENTS_MD5_BASE64");
        var md5 = string.IsNullOrEmpty(eventsMd5) ? null : eventsMd5;

        var team = await _factory.TeamService().GetOrCreateTeam(TestTeamId);
        var project = await _factory.GetOrCreateProject("curl_operator_proof", team.Id);

        // Ingenix layout: files land in a project-root folder named by the analysis
        // id. Fresh id each run so curl actually downloads (it dedups by path).
        var analysisId = Guid.NewGuid().ToString();

        // Name each file like the real inputs; any extras get a generated name.
        var names = new[] { "events.zip", "gather_step.csv", "markers.csv" };
        var curlFiles = urls
            .Select((url, i) => new CurlFile(
                $"{analysisId}/{(i < names.Length ? names[i] : $"file{i}.dat")}", url, md5))
            .ToList();

        Console.WriteLine($"Downloading {curlFiles.Count} file(s) in one curl task under folder {analysisId}");

        // The whole batch: one worker task, no bytes through main.
        var downloaded = await _factory.DownloadFiles(project.Id, curlFiles);

        Assert.AreEqual(curlFiles.Count, downloaded.Count, "one FileDocument per requested file");

        // All files land in the same project-root folder named by the analysis id.
        var folders = (await _factory.ProjectDocumentService().FindProjectObjects(project.Id))
            .Where(d => d.ObjectCase == EProjectDocument.ObjectOneofCase.Folderdocument)
            .Select(d => d.Folderdocument)
            .ToList();

        var folder = folders.FirstOrDefault(f => f.Name == analysisId);
        Console.WriteLine(folder == null
            ? $"folder '{analysisId}' NOT found"
            : $"folder: id={folder.Id} name={folder.Name} parent='{folder.FolderId}'");

        Assert.IsNotNull(folder, $"expected a project-root folder named by the analysis id '{analysisId}'");
        Assert.IsTrue(folder!.FolderId.Length == 0, "the analysis folder should be at the project root");

        // Each requested file came back, in order, with the right name and folder.
        for (var i = 0; i < curlFiles.Count; i++)
        {
            var expectedName = curlFiles[i].Path.Split('/').Last();
            var fileDoc = downloaded[i];
            Console.WriteLine($"[{i}] FileDocument: id={fileDoc.Id} name={fileDoc.Name} folderId={fileDoc.FolderId} size={fileDoc.Size}");

            Assert.AreEqual(expectedName, fileDoc.Name, $"file {i} should be named {expectedName}");
            Assert.AreEqual(folder.Id, fileDoc.FolderId, $"file {i} should be in the analysis folder");
        }
    }
}
