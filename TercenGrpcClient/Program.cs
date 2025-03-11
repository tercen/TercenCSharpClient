using Grpc.Net.Client;

// The port number must match the port of the gRPC server.
using var channel = GrpcChannel.ForAddress("http://127.0.0.1:50051");

var client = new TercenGrpcClient.UserService.UserServiceClient(channel);

var connectReq = new TercenGrpcClient.ReqConnect2();
connectReq.Domain = "";
connectReq.UsernameOrEmail = "admin";
connectReq.Password = "admin";

var replyConnect2 = await client.connect2Async(connectReq);

 
Console.WriteLine("Tercen Session : " + replyConnect2.Result);
 