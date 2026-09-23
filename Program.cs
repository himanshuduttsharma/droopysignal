using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();
app.UseDefaultFiles();
app.UseStaticFiles();
app.UseWebSockets();

var rooms = new ConcurrentDictionary<string, ConcurrentDictionary<Guid, Participant>>();

app.MapGet("/health", () => Results.Ok(new { status = "ok" }));
app.Map("/ws", async context =>
{
    if (!context.WebSockets.IsWebSocketRequest)
    {
        context.Response.StatusCode = StatusCodes.Status400BadRequest;
        return;
    }

    using var socket = await context.WebSockets.AcceptWebSocketAsync();
    var id = Guid.NewGuid();
    string? roomName = null;
    try
    {
        var join = await ReceiveAsync(socket, context.RequestAborted);
        if (join is null || join.Value.GetProperty("type").GetString() != "join") return;
        roomName = join.Value.GetProperty("room").GetString() ?? "demo-room";
        var room = rooms.GetOrAdd(roomName, _ => new());
        room[id] = new Participant(socket, join.Value.GetProperty("role").GetString() ?? "unknown");
        if (room[id].Role == "receiver")
            await BroadcastAsync(room, id, new { type = "receiver-ready" }, context.RequestAborted);
        else if (room.Values.Any(p => p.Role == "receiver"))
            await socket.SendAsync(Encoding.UTF8.GetBytes("{\"type\":\"receiver-ready\"}"), WebSocketMessageType.Text, true, context.RequestAborted);

        while (socket.State == WebSocketState.Open)
        {
            var message = await ReceiveAsync(socket, context.RequestAborted);
            if (message is null) break;
            var bytes = Encoding.UTF8.GetBytes(message.Value.GetRawText());
            foreach (var peer in room.Where(p => p.Key != id && p.Value.Socket.State == WebSocketState.Open))
                await peer.Value.Socket.SendAsync(bytes, WebSocketMessageType.Text, true, context.RequestAborted);
        }
    }
    catch (OperationCanceledException) { }
    catch (WebSocketException) { }
    finally
    {
        if (roomName is not null && rooms.TryGetValue(roomName, out var room))
        {
            room.TryRemove(id, out _);
            if (room.IsEmpty) rooms.TryRemove(roomName, out _);
        }
    }
});
var listernUrl=builder.Configuration["urls"] ?? "http://0.0.0.0:8080";
app.Run(listernUrl);

static async Task<JsonElement?> ReceiveAsync(WebSocket socket, CancellationToken cancellationToken)
{
    var buffer = new byte[64 * 1024];
    using var message = new MemoryStream();
    WebSocketReceiveResult result;
    do
    {
        result = await socket.ReceiveAsync(buffer, cancellationToken);
        if (result.MessageType == WebSocketMessageType.Close) return null;
        message.Write(buffer, 0, result.Count);
    } while (!result.EndOfMessage);
    return JsonDocument.Parse(message.ToArray()).RootElement.Clone();
}

static async Task BroadcastAsync(ConcurrentDictionary<Guid, Participant> room, Guid senderId, object message, CancellationToken cancellationToken)
{
    var bytes = JsonSerializer.SerializeToUtf8Bytes(message);
    foreach (var peer in room.Where(p => p.Key != senderId && p.Value.Socket.State == WebSocketState.Open))
        await peer.Value.Socket.SendAsync(bytes, WebSocketMessageType.Text, true, cancellationToken);
}

record Participant(WebSocket Socket, string Role);
