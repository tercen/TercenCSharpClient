using Apache.Arrow;
using Apache.Arrow.Ipc;
using Grpc.Core;
using Microsoft.Data.Analysis;
using Tercen;
using TercenGrpcClient;
using TercenGrpcClient.client;
using Column = Tercen.Column;
using Table = Tercen.Table;
using Task = System.Threading.Tasks.Task;

namespace TercenGrpcClient.extensions;

public delegate void RelationCallbackDelegate(ERelation relation);

public static class ERelationExtension
{
    public static IEnumerable<string> GetSimpleRelationIds(this ERelation relation)
    {
        var simpleRelationIds = new List<string>();

        relation.VisitSimpleRelations(CollectSimpleRelations);

        return simpleRelationIds;

        void CollectSimpleRelations(ERelation rel)
        {
            if (rel.Simplerelation != null)
            {
                simpleRelationIds.Add(rel.Simplerelation.Id);
            }
        }
    }

    private static void VisitSimpleRelations(this ERelation relation, RelationCallbackDelegate callback)
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

    public static ERelation CreateFileDocumentRelation(IEnumerable<FileDocument> fileDocuments)
    {
        var aliasDocumentIds = new StrValues();
        var documentIds = new StrValues();

        var enumerable = fileDocuments as FileDocument[] ?? fileDocuments.ToArray();
        foreach (var fileDocument in enumerable)
        {
            aliasDocumentIds.Values.Add(Guid.NewGuid().ToString());
            documentIds.Values.Add(fileDocument.Id);
        }

        var tbl = new Table
        {
            NRows = enumerable.Count(),
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
                            Strvalues = aliasDocumentIds
                        }
                    },
                    new Column
                    {
                        Name = ".documentId",
                        Type = "string",
                        CValues = new ECValues
                        {
                            Strvalues = documentIds
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
}

public static class FileServiceExtension
{
    public static async Task<FileDocument> UploadFile(this FileService.FileServiceClient fileService, string filePath,
        FileDocument fileDocument)
    {
        const int chunkSize = 1024 * 1024;
        using var uploadCall = fileService.upload();
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
}

public static class TercenFactoryExtension
{
    public static async Task<Project> GetOrCreateProject(this TercenFactory factory, string projectName, string teamId)
    {
        return await factory.DocumentService().FindProjectByOwnerAndName(teamId, projectName) ?? (await factory
            .ProjectService().createAsync(new EProject()
            {
                Project = new Project()
                {
                    Name = projectName,
                    IsPublic = false,
                    Acl = new Acl()
                    {
                        Owner = teamId,
                    }
                }
            })).Project;
    }
}

public static class TableSchemaServiceExtension
{
    public static async Task<DataFrame> Select(this TableSchemaService.TableSchemaServiceClient tableSchemaService,
        ReqStreamTable reqStreamTable)
    {
        var list = new List<DataFrame>();
        await foreach (var dataFrame in tableSchemaService.Stream(reqStreamTable))
        {
            list.Add(dataFrame);
        }

        var df = ConcatenateVertically(list.ToArray());

        return df;
    }


    public static async IAsyncEnumerable<DataFrame> Stream(
        this TableSchemaService.TableSchemaServiceClient tableSchemaService,
        ReqStreamTable reqStreamTable)
    {
        reqStreamTable.BinaryFormat = "arrow";

        using var call = tableSchemaService.streamTable(reqStreamTable);

        await foreach (var chunk in call.ResponseStream.ReadAllAsync())
        {
            var df = await DataFrameFromBytes(chunk.Result.ToByteArray());
            yield return df;
        }
    }

    private static async IAsyncEnumerable<Dictionary<string, object>> StreamArrowRowsAsync(byte[] arrowBytes)
    {
        using var stream = new MemoryStream(arrowBytes);
        using var reader = new ArrowStreamReader(stream);
        var schema = reader.Schema;

        RecordBatch recordBatch;

        while ((recordBatch = await reader.ReadNextRecordBatchAsync()) != null)
        {
            var len = recordBatch.Length;

            for (var row = 0; row < len; row++)
            {
                var rowData = new Dictionary<string, object>();
                for (var col = 0; col < recordBatch.ColumnCount; col++)
                {
                    var fieldName = schema.FieldsList[col].Name;
                    var column = recordBatch.Column(col);
                    object value = column switch
                    {
                        Int32Array intArray => intArray.GetValue(row),
                        StringArray stringArray => stringArray.GetString(row),
                        DoubleArray doubleArray => doubleArray.GetValue(row),
                        BooleanArray boolArray => boolArray.GetValue(row),
                        _ => null
                    };

                    rowData[fieldName] = value;
                }

                yield return rowData;
            }
        }
    }

    public static async Task<DataFrame> DataFrameFromBytes(byte[] arrowBytes)
    {
        var rows = new List<Dictionary<string, object>>();
        var columnTypes = new Dictionary<string, Type>();
        bool schemaInitialized = false;

        // Collect rows and infer schema
        await foreach (var row in StreamArrowRowsAsync(arrowBytes))
        {
            if (!schemaInitialized)
            {
                foreach (var kvp in row)
                {
                    columnTypes[kvp.Key] = kvp.Value?.GetType() ?? typeof(object);
                }

                schemaInitialized = true;
            }

            rows.Add(row);
        }

        // Create DataFrame columns
        var columns = new List<DataFrameColumn>();
        foreach (var columnName in columnTypes.Keys)
        {
            var values = rows.Select(row => row[columnName]).ToList();
            DataFrameColumn column = columnTypes[columnName] switch
            {
                Type t when t == typeof(int) => new PrimitiveDataFrameColumn<int>(columnName, values.Cast<int?>()),
                Type t when t == typeof(string) => new StringDataFrameColumn(columnName, values.Cast<string>()),
                Type t when t == typeof(double) => new PrimitiveDataFrameColumn<double>(columnName,
                    values.Cast<double?>()),
                Type t when t == typeof(bool) => new PrimitiveDataFrameColumn<bool>(columnName, values.Cast<bool?>()),
                _ => throw new NotSupportedException($"Unsupported type for column {columnName}")
            };
            columns.Add(column);
        }

        return new DataFrame(columns);
    }

    public static DataFrame ConcatenateVertically(params DataFrame[] dataFrames)
    {
        if (dataFrames == null || dataFrames.Length == 0)
            throw new ArgumentException("At least one DataFrame is required.");

        // Validate schema compatibility
        var referenceSchema = dataFrames[0].Columns.Select(c => (c.Name, c.DataType)).ToList();
        for (int i = 1; i < dataFrames.Length; i++)
        {
            var schema = dataFrames[i].Columns.Select(c => (c.Name, c.DataType)).ToList();
            if (!referenceSchema.SequenceEqual(schema))
                throw new ArgumentException($"DataFrame {i} has incompatible schema.");
        }

        // Create new columns for the concatenated DataFrame
        var concatenatedColumns = new List<DataFrameColumn>();
        foreach (var columnName in dataFrames[0].Columns.Select(c => c.Name))
        {
            var firstColumn = dataFrames[0].Columns[columnName];
            DataFrameColumn newColumn = firstColumn switch
            {
                PrimitiveDataFrameColumn<int> => new PrimitiveDataFrameColumn<int>(columnName),
                StringDataFrameColumn => new StringDataFrameColumn(columnName),
                PrimitiveDataFrameColumn<double> => new PrimitiveDataFrameColumn<double>(columnName),
                PrimitiveDataFrameColumn<bool> => new PrimitiveDataFrameColumn<bool>(columnName),
                _ => throw new NotSupportedException($"Unsupported column type: {firstColumn.GetType().Name}")
            };

            // Append data from each DataFrame
            foreach (var df in dataFrames)
            {
                var column = df.Columns[columnName];
                for (long i = 0; i < column.Length; i++)
                {
                    switch (newColumn)
                    {
                        case PrimitiveDataFrameColumn<int> intColumn:
                            intColumn.Append(column[i] as int?);
                            break;
                        case StringDataFrameColumn stringColumn:
                            stringColumn.Append(column[i] as string);
                            break;
                        case PrimitiveDataFrameColumn<double> doubleColumn:
                            doubleColumn.Append(column[i] as double?);
                            break;
                        case PrimitiveDataFrameColumn<bool> boolColumn:
                            boolColumn.Append(column[i] as bool?);
                            break;
                        default:
                            throw new NotSupportedException($"Unsupported column type: {newColumn.GetType().Name}");
                    }
                }
            }

            concatenatedColumns.Add(newColumn);
        }

        return new DataFrame(concatenatedColumns);
    }
}

public static class TaskServiceExtension
{
    public static async Task InstallWorkflowTemplate(
        this TaskService.TaskServiceClient taskService, Project project, string templateGitUri, string version,
        string gitPat)
    {
        var task = new ETask
        {
            Gitprojecttask = new GitProjectTask
            {
                Owner = project.Acl.Owner,
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
                            Value = project.Id
                        },
                        new Pair
                        {
                            Key = "PROJECT_REV",
                            Value = project.Rev
                        },
                        new Pair
                        {
                            Key = "GIT_ACTION",
                            Value = "reset/pull"
                        },
                        new Pair
                        {
                            Key = "GIT_PAT",
                            Value = gitPat
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
        var result = await taskService.createAsync(task);

        await taskService.runTaskAsync(new ReqRunTask
        {
            TaskId = result.Gitprojecttask.Id
        });

        await taskService.waitDoneAsync(new ReqWaitDone()
        {
            TaskId = result.Gitprojecttask.Id
        });
    }
}

public static class TeamServiceExtension
{
    public static async Task<Team> GetOrCreateTeam(
        this TeamService.TeamServiceClient teamService, string teamName)

    {
        try
        {
            var teamResponse = await teamService.getAsync(new GetRequest { Id = teamName });
            return teamResponse.Team;
        }
        catch (RpcException e)
        {
            if (!new TercenException(e).IsNotFound())
            {
                throw;
            }
        }

        var team = await teamService.createAsync(new ETeam
        {
            Team = new Team
            {
                Name = teamName,
            }
        });

        return team.Team;
    }


    public static async Task<Team> GetOrCreateTestLibraryTeam(
        this TeamService.TeamServiceClient teamService, string teamName)

    {
        try
        {
            var teamResponse = await teamService.getAsync(new GetRequest { Id = teamName });
            return teamResponse.Team;
        }
        catch (RpcException e)
        {
            if (!new TercenException(e).IsNotFound())
            {
                throw;
            }
        }

        var team = await teamService.createAsync(new ETeam
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
        });

        return team.Team;
    }
}

public static class DocumentServiceExtension
{
    public static async Task<ProjectDocument?> GetWorkflowTemplate(
        this DocumentService.DocumentServiceClient documentService, string templateGitUri, string version)
    {
        // var response = await documentService
        //     .getLibraryAsync(new ReqGetLibrary { DocTypes = { "Workflow" }, TeamIds = { TestLibraryTeamId } });

        var response = await documentService
            .getLibraryAsync(new ReqGetLibrary { DocTypes = { "Workflow" } });

        var templates = response.List.Where(doc => doc.Projectdocument != null).Where(doc =>
            doc.Projectdocument.Url.Uri == templateGitUri &&
            doc.Projectdocument.Version == version).ToList();

        return templates.Count > 0 ? templates.First().Projectdocument : null;
    }


    public static async Task<List<Project>> FindProjectsByOwner(
        this DocumentService.DocumentServiceClient documentService, string teamOrUserId)
    {
        var request = new KeyRangeRequest { Name = "Project/findProjectByOwnersAndName" };

        request.StartKeys.Add(new IndexKeyValue { IndexField = "acl.owner", StringValue = teamOrUserId });
        request.StartKeys.Add(new IndexKeyValue { IndexField = "name", StringValue = "\ufff0" });

        request.EndKeys.Add(new IndexKeyValue { IndexField = "acl.owner", StringValue = teamOrUserId });
        request.EndKeys.Add(new IndexKeyValue { IndexField = "name", StringValue = "" });

        request.UseFactory = true;
        request.Limit = 10000000;

        return (await documentService.findKeyRangeAsync(request)).List.Select(doc => doc.Project).ToList();
    }

    public static async Task<Project?> FindProjectByOwnerAndName(
        this DocumentService.DocumentServiceClient documentService, string teamOrUserId, string projectName)
    {
        var request = new KeyRangeRequest { Name = "Project/findProjectByOwnersAndName" };

        request.StartKeys.Add(new IndexKeyValue { IndexField = "acl.owner", StringValue = teamOrUserId });
        request.StartKeys.Add(new IndexKeyValue { IndexField = "name", StringValue = projectName });

        request.EndKeys.Add(new IndexKeyValue { IndexField = "acl.owner", StringValue = teamOrUserId });
        request.EndKeys.Add(new IndexKeyValue { IndexField = "name", StringValue = projectName });

        request.UseFactory = true;
        request.Limit = 1;

        return (await documentService.findKeyRangeAsync(request)).List.Select(doc => doc.Project)
            .ToList()
            .FirstOrDefault();
    }
}

public static class ETaskExtension
{
    public static string Id(this ETask task)
    {
        switch (task.ObjectCase)
        {
            case ETask.ObjectOneofCase.Csvtask:
                return task.Csvtask.Id;
            case ETask.ObjectOneofCase.Computationtask:
                return task.Computationtask.Id;
            case ETask.ObjectOneofCase.Creategitoperatortask:
                return task.Creategitoperatortask.Id;
            case ETask.ObjectOneofCase.Cubequerytask:
                return task.Cubequerytask.Id;
            case ETask.ObjectOneofCase.Exporttabletask:
                return task.Exporttabletask.Id;
            case ETask.ObjectOneofCase.Exportworkflowtask:
                return task.Exportworkflowtask.Id;
            case ETask.ObjectOneofCase.Gitprojecttask:
                return task.Gitprojecttask.Id;
            case ETask.ObjectOneofCase.Gltask:
                return task.Gltask.Id;
            case ETask.ObjectOneofCase.Importgitdatasettask:
                return task.Importgitdatasettask.Id;
            case ETask.ObjectOneofCase.Importgitworkflowtask:
                return task.Importgitworkflowtask.Id;
            case ETask.ObjectOneofCase.Importworkflowtask:
                return task.Importworkflowtask.Id;
            case ETask.ObjectOneofCase.Librarytask:
                return task.Librarytask.Id;
            case ETask.ObjectOneofCase.Projecttask:
                return task.Projecttask.Id;
            case ETask.ObjectOneofCase.Runcomputationtask:
                return task.Runcomputationtask.Id;
            case ETask.ObjectOneofCase.Runwebapptask:
                return task.Runwebapptask.Id;
            case ETask.ObjectOneofCase.Runworkflowtask:
                return task.Runworkflowtask.Id;
            case ETask.ObjectOneofCase.Savecomputationresulttask:
                return task.Savecomputationresulttask.Id;
            case ETask.ObjectOneofCase.Task:
                return task.Task.Id;
            case ETask.ObjectOneofCase.Testoperatortask:
                return task.Testoperatortask.Id;

            case ETask.ObjectOneofCase.None:
            default:
                throw new ArgumentOutOfRangeException();
        }
    }
}

public static class EStateExtension
{
    public static void ThrowIfNotDone(this EState state)
    {
        switch (state.ObjectCase)
        {
            case EState.ObjectOneofCase.Donestate:
                break;

            case EState.ObjectOneofCase.Failedstate:
                var error = state.Failedstate.Error;
                throw new Exception(error);

            case EState.ObjectOneofCase.Canceledstate:
            case EState.ObjectOneofCase.Initstate:
            case EState.ObjectOneofCase.Pendingstate:
            case EState.ObjectOneofCase.Runningdependentstate:
            case EState.ObjectOneofCase.Runningstate:
            case EState.ObjectOneofCase.State:
            case EState.ObjectOneofCase.None:
                throw new Exception("bad state : done state expected");
            default:
                throw new ArgumentOutOfRangeException();
        }
    }
}

public static class EColumnSchemaExtension
{
    public static string Name(this EColumnSchema column)
    {
        switch (column.ObjectCase)
        {
            case EColumnSchema.ObjectOneofCase.Column:
                return column.Column.Name;
            case EColumnSchema.ObjectOneofCase.Columnschema:
                return column.Columnschema.Name;
            case EColumnSchema.ObjectOneofCase.None:
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    public static string Type(this EColumnSchema column)
    {
        switch (column.ObjectCase)
        {
            case EColumnSchema.ObjectOneofCase.Column:
                return column.Column.Type;
            case EColumnSchema.ObjectOneofCase.Columnschema:
                return column.Columnschema.Type;
            case EColumnSchema.ObjectOneofCase.None:
            default:
                throw new ArgumentOutOfRangeException();
        }
    }
}

public static class ESchemaExtension
{
    public static string Id(this ESchema schema)
    {
        switch (schema.ObjectCase)
        {
            case ESchema.ObjectOneofCase.Computedtableschema:
                return schema.Computedtableschema.Id;
            case ESchema.ObjectOneofCase.Cubequerytableschema:
                return schema.Cubequerytableschema.Id;
            case ESchema.ObjectOneofCase.Schema:
                return schema.Schema.Id;
            case ESchema.ObjectOneofCase.Tableschema:
                return schema.Tableschema.Id;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    public static int NRows(this ESchema schema)
    {
        switch (schema.ObjectCase)
        {
            case ESchema.ObjectOneofCase.Computedtableschema:
                return schema.Computedtableschema.NRows;
            case ESchema.ObjectOneofCase.Cubequerytableschema:
                return schema.Cubequerytableschema.NRows;
            case ESchema.ObjectOneofCase.Schema:
                return schema.Schema.NRows;
            case ESchema.ObjectOneofCase.Tableschema:
                return schema.Tableschema.NRows;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    public static EColumnSchema[] Columns(this ESchema schema)
    {
        switch (schema.ObjectCase)
        {
            case ESchema.ObjectOneofCase.Computedtableschema:
                return schema.Computedtableschema.Columns.ToArray();
            case ESchema.ObjectOneofCase.Cubequerytableschema:
                return schema.Cubequerytableschema.Columns.ToArray();
            case ESchema.ObjectOneofCase.Schema:
                return schema.Schema.Columns.ToArray();
            case ESchema.ObjectOneofCase.Tableschema:
                return schema.Tableschema.Columns.ToArray();
            default:
                throw new ArgumentOutOfRangeException();
        }
    }
}