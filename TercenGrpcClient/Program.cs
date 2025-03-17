using Grpc.Net.Client;
using TercenGrpcClient;

namespace MyApp
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Hello, Tercen!");
            
            using var channel = GrpcChannel.ForAddress("http://127.0.0.1:50051");

            var client = new UserService.UserServiceClient(channel);

            var replyConnect2 = await client.connect2Async(
                new ReqConnect2
                {
                    Domain = "",
                    UsernameOrEmail = "admin",
                    Password = "admin",
                });

            Console.WriteLine("Tercen Session : " + replyConnect2.Result);
        }
    }
}