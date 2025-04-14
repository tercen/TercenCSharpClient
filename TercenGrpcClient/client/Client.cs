using System.Text.Json;
using Grpc.Core;

using Grpc.Net.Client;
using TercenGrpcClient;

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
    
    public DocumentService.DocumentServiceClient DocumentService()
    {
        return new DocumentService.DocumentServiceClient(_channel);
    }
    
    public TableSchemaService.TableSchemaServiceClient TableSchemaService()
    {
        return new TableSchemaService.TableSchemaServiceClient(_channel);
    }
    
    public ProjectDocumentService.ProjectDocumentServiceClient ProjectDocumentService()
    {
        return new ProjectDocumentService.ProjectDocumentServiceClient(_channel);
    }
    
    public TaskService.TaskServiceClient TaskService()
    {
        return new TaskService.TaskServiceClient(_channel);
    }
    
    public  WorkflowService.WorkflowServiceClient WorkflowService()
    {
        return new WorkflowService.WorkflowServiceClient(_channel);
    }
}