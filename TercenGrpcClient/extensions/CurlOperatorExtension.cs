using Grpc.Core;
using Tercen;
using TercenGrpcClient;
using TercenGrpcClient.client;
using Task = System.Threading.Tasks.Task;

namespace TercenGrpcClient.extensions;

/// <summary>
/// One file to fetch with the native "File Downloader" (curl) operator.
/// </summary>
/// <param name="Path">
/// Relative project path where the file lands. The dirname becomes a folder
/// (created by the operator if missing); the basename becomes the FileDocument
/// name. e.g. "&lt;analysisId&gt;/events.zip".
/// </param>
/// <param name="Url">Download URL, e.g. an Azure blob SAS URL.</param>
/// <param name="Md5">
/// Optional base64(md5). NOT required for Azure blob URLs — the operator verifies
/// those with a server-computed CRC64 during a ranged download. Supply it only for
/// callers/instances that require an explicit hash.
/// </param>
public sealed record CurlFile(string Path, string Url, string? Md5 = null);

/// <summary>
/// Runs the native "File Downloader" (curl) operator to download files onto a
/// worker and materialize them as FileDocuments in a project — without streaming
/// the bytes through the Tercen main service.
/// </summary>
public static class CurlOperatorExtension
{
    // Native "File Downloader" operator (sci native_operators.dart).
    private const string CurlOperatorId = "5ba24424-6e73-40f3-a65d-0448fa5d437f";
    private const string CurlOperatorUrl = "https://tercen.com/_operator/curl";

    /// <summary>
    /// Downloads <paramref name="files"/> on a worker via the curl operator — a
    /// single RunComputationTask, no CubeQueryTask — and returns the created
    /// FileDocuments, one per input, in the same order.
    ///
    /// The operator streams each URL directly into the project's object store and
    /// creates a FileDocument at the given path, building any parent folders. For
    /// Azure blob URLs (*.blob.core.windows.net) the download is fetched in ranges
    /// with server-verified CRC64 integrity, so no md5 is needed. The operator
    /// dedups by path: a file already present at a path is not re-downloaded.
    /// </summary>
    /// <param name="factory">Connected Tercen factory.</param>
    /// <param name="projectId">Project the files (and download task) belong to.</param>
    /// <param name="files">Files to download (path + url [+ md5]).</param>
    /// <param name="owner">
    /// ACL owner (team id) for the task. If null, the project's own ACL owner is used.
    /// </param>
    /// <param name="parallel">Number of concurrent downloads (operator default 1).</param>
    public static async Task<IReadOnlyList<FileDocument>> DownloadFiles(
        this TercenFactory factory,
        string projectId,
        IReadOnlyList<CurlFile> files,
        string? owner = null,
        int parallel = 1)
    {
        if (files.Count == 0)
        {
            return new List<FileDocument>();
        }

        owner ??= (await factory.ProjectService()
            .getAsync(new GetRequest { Id = projectId })).Project.Acl.Owner;

        var task = await factory.TaskService().createAsync(new ETask
        {
            Runcomputationtask = new RunComputationTask
            {
                State = new EState { Initstate = new InitState() },
                Owner = owner,
                ProjectId = projectId,
                Query = BuildQuery(files, parallel),
                ChannelId = Guid.NewGuid().ToString()
            }
        });

        var taskId = task.Id();

        // Start = true both runs the task and streams its events until it ends.
        using (var listenCall = factory.EventService()
                   .listenTaskChannel(new ReqListenTaskChannel { TaskId = taskId, Start = true }))
        {
            await foreach (var _ in listenCall.ResponseStream.ReadAllAsync())
            {
                // Drain the event stream; state is checked on the reloaded task below.
            }
        }

        task = await factory.TaskService().getAsync(new GetRequest { Id = taskId });
        task.Runcomputationtask.State.ThrowIfNotDone();

        // curl doesn't return the created documents — resolve each by its path.
        var result = new List<FileDocument>(files.Count);
        foreach (var file in files)
        {
            result.Add(await factory.ResolveFileDocument(projectId, file.Path));
        }

        return result;
    }

    /// <summary>Downloads a single file; see <see cref="DownloadFiles"/>.</summary>
    public static async Task<FileDocument> DownloadFile(
        this TercenFactory factory,
        string projectId,
        string path,
        string url,
        string? md5 = null,
        string? owner = null,
        int parallel = 1)
    {
        var docs = await factory.DownloadFiles(
            projectId, new[] { new CurlFile(path, url, md5) }, owner, parallel);
        return docs[0];
    }

    private static CubeQuery BuildQuery(IReadOnlyList<CurlFile> files, int parallel)
    {
        var hasMd5 = files.Any(f => !string.IsNullOrEmpty(f.Md5));

        Column StrCol(string name, IEnumerable<string> values)
        {
            var strValues = new StrValues();
            strValues.Values.AddRange(values);
            return new Column
            {
                Name = name,
                Type = "string",
                NRows = files.Count,
                CValues = new ECValues { Strvalues = strValues }
            };
        }

        // In-memory manifest: one row per file. No upload, no persisted schema.
        var tbl = new Table { NRows = files.Count };
        tbl.Columns.Add(StrCol("path", files.Select(f => f.Path)));
        tbl.Columns.Add(StrCol("href", files.Select(f => f.Url)));
        if (hasMd5)
        {
            tbl.Columns.Add(StrCol("md5", files.Select(f => f.Md5 ?? "")));
        }

        var query = new CubeQuery
        {
            Relation = new ERelation
            {
                Inmemoryrelation = new InMemoryRelation { InMemoryTable = tbl }
            },
            OperatorSettings = new OperatorSettings
            {
                OperatorRef = new OperatorRef
                {
                    OperatorId = CurlOperatorId,
                    Name = "File Downloader",
                    Url = new Url { Uri = CurlOperatorUrl }
                }
            }
        };

        query.RowColumns.Add(new Factor { Name = "path", Type = "string" });
        query.RowColumns.Add(new Factor { Name = "href", Type = "string" });
        if (hasMd5)
        {
            query.RowColumns.Add(new Factor { Name = "md5", Type = "string" });
        }

        if (parallel > 1)
        {
            query.OperatorSettings.OperatorRef.PropertyValues.Add(
                new PropertyValue { Name = "Parallel", Value = parallel.ToString() });
        }

        // A computation query needs at least one CubeAxisQuery: the row schema is
        // built inside the axis-query loop, so with none the row relation would be
        // an empty union and the row factors couldn't be resolved. curl reads only
        // the row columns, so an empty cell axis (no x/y) is enough.
        query.AxisQueries.Add(new CubeAxisQuery());

        return query;
    }

    private static async Task<FileDocument> ResolveFileDocument(
        this TercenFactory factory, string projectId, string path)
    {
        var slash = path.LastIndexOf('/');
        var dir = slash < 0 ? "" : path.Substring(0, slash);
        var name = slash < 0 ? path : path.Substring(slash + 1);

        // Root files have no folder id; otherwise resolve the (now-existing) folder.
        var folderId = dir.Length == 0
            ? ""
            : (await factory.FolderService().GetOrCreateFolder(projectId, dir)).Id;

        var docs = await factory.ProjectDocumentService()
            .FindProjectObjectsByFolderAndName(projectId, folderId, name);

        var fileDoc = docs
            .Where(d => d.ObjectCase == EProjectDocument.ObjectOneofCase.Filedocument)
            .Select(d => d.Filedocument)
            .FirstOrDefault();

        if (fileDoc == null)
        {
            throw new Exception($"curl download produced no FileDocument at '{path}'");
        }

        return fileDoc;
    }
}
