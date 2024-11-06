using System.Net;
using System.Net.WebSockets;
using System.Text;

var builder = WebApplication.CreateBuilder(args);

builder.WebHost.UseUrls("http://localhost:6969");

var app = builder.Build();

app.UseWebSockets();

var connections = new List<WebSocket>(); // Multiple

app.Map("ws", async context =>
{

    //using var ws = await context.WebSockets.AcceptWebSocketAsync(); // Single

    if (context.WebSockets.IsWebSocketRequest)
    {
        var curName = context.Request.Query["name"];

        using var ws = await context.WebSockets.AcceptWebSocketAsync();

        connections.Add(ws);

        await Broadcast($"{curName} joined the room");

        await Broadcast($"{connections.Count} users connected");

        await ReceiveMessage(ws,
            async (result, buffer) =>
            {
                if (result.MessageType == WebSocketMessageType.Text)
                {
                    string message = Encoding.UTF8.GetString(buffer, 0, result.Count);
                    await Broadcast(curName + ": " + message);
                }
                else if (result.MessageType == WebSocketMessageType.Close || ws.State == WebSocketState.Aborted)
                {
                    connections.Remove(ws);
                    await Broadcast($"{curName} left the room");
                    await Broadcast($"{connections.Count} users connected");
                    await ws.CloseAsync(result.CloseStatus.Value, result.CloseStatusDescription, CancellationToken.None);
                }
            });
        #region Single
        //while (true)
        //{

        //    var message = "The current time is: " + DateTime.Now.ToString("HH:mm:ss");

        //    var bytes = Encoding.UTF8.GetBytes(message);

        //    var arraySagments = new ArraySegment<byte>(bytes, 0, bytes.Length);

        //    if (ws.State == WebSocketState.Open)
        //    {
        //        await ws.SendAsync(arraySagments, WebSocketMessageType.Text, true, CancellationToken.None);
        //    }
        //    else if (ws.State == WebSocketState.Closed || ws.State == WebSocketState.Aborted)
        //    {
        //        break;
        //    }

        //    Thread.Sleep(2000);
        //}
        #endregion
    }
    else
    {
        context.Response.StatusCode = (int)HttpStatusCode.BadRequest;
    }


});

async Task ReceiveMessage(WebSocket socket, Action<WebSocketReceiveResult, byte[]> handleMessage)
{
    var buffer = new byte[1024 * 4];
    while (socket.State == WebSocketState.Open)
    {
        var result = await socket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
        handleMessage(result, buffer);
    }
}

async Task Broadcast(string message)
{
    var bytes = Encoding.UTF8.GetBytes(message);
    foreach (var socket in connections)
    {
        if (socket.State == WebSocketState.Open)
        {
            var arraySegment = new ArraySegment<byte>(bytes, 0, bytes.Length);
            await socket.SendAsync(arraySegment, WebSocketMessageType.Text, true, CancellationToken.None);
        }
    }
}

await app.RunAsync();
