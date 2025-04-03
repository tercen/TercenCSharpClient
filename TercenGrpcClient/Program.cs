using Grpc.Net.Client;
using TercenGrpcClient;

namespace MyApp
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Hello, Tercen!");
            
            string? uri = Environment.GetEnvironmentVariable("TERCEN_URI");
            string? tenant = Environment.GetEnvironmentVariable("TERCEN_TENANT");
            string? username = Environment.GetEnvironmentVariable("TERCEN_USERNAME");
            string? password = Environment.GetEnvironmentVariable("TERCEN_PASSWORD");
        
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
        
            
            using var channel = GrpcChannel.ForAddress(uri);

            var client = new UserService.UserServiceClient(channel);

            var replyConnect2 = await client.connect2Async(
                new ReqConnect2
                {
                    Domain = tenant,
                    UsernameOrEmail = username,
                    Password = password,
                });

            Console.WriteLine("Tercen Session : " + replyConnect2.Result);
        }
    }
}