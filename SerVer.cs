// ╔══════════════════════════════════════════════════════════════════╗
// ║  SERVER.CS — v1.0.7 | 2026-04-06                                ║
// ║  Fix: Ẩn tên client khi ghi log - chỉ hiển thị thông báo chung  ║
// ╚══════════════════════════════════════════════════════════════════╝
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using MaSoiServer;

class Server
{
    static string ConnStr =
        "Server=localhost\\SQLEXPRESS;Database=MaSoiDB;" +
        "Trusted_Connection=True;TrustServerCertificate=True;";

    const int PORT = 8888;
    const int MIN_PLAYERS = 6;
    const int ROLE_TIME = 20;
    const int WITCH_ROLE_TIME = 20;
    const int ROOM_PICK_TIME = 20;
    const int DISCUSS_TIME = 20;
    const int VOTE_TIME = 15;

    const int WOLF_PHASE_END_DELAY = 2000;

    static readonly Dictionary<int, string[]> ROLE_CONFIGS = new()
    {
        { 6, new[]{ "werewolf","werewolf","seer","guard","witch","villager" } },
        { 7, new[]{ "werewolf","werewolf","seer","guard","witch","villager","villager" } },
        { 8, new[]{ "werewolf","werewolf","werewolf","seer","guard","witch","villager","villager" } },
    };

    static readonly string[] NIGHT_ORDER = { "seer", "guard", "werewolf", "witch" };

    static List<Player> players = new();
    static object pLock = new();
    static bool gameStarted = false;
    static bool gameEnded = false;
    static DateTime gameStartTime;
    static string phase = "waiting";
    static int roundNum = 0;

    static string? activeNightRole = null;
    static object activeRoleLock = new();

    static string? nightKillId = null;
    static string? protectId = null;
    static string? witchSaveId = null;
    static string? witchKillId = null;
    static string? witchUsedPotionThisNight = null;
    static Dictionary<string, string> wolfVotes = new();
    static Dictionary<string, string> votes = new();

    static ManualResetEventSlim? nightActionEvent = null;
    static ManualResetEventSlim? wolfVoteEvent = null;

    static object voteLock = new();
    static bool voteResolved = false;

    static Dictionary<string, int> wolfDoorAllowance = new();
    static bool bonusUnlocked = false;
    static Dictionary<string, WolfHuntState> huntStates = new();
    static string? huntTargetId = null;
    static HashSet<string> wolvesFinishedHunt = new();

    static Dictionary<int, DoorResult> openedDoorResults = new();
    static object huntLock = new();
    static string? activeHuntingWolfId = null;

    static ManualResetEventSlim? wolfDoorEvent = null;

    static async Task Main()
    {
        Console.OutputEncoding = Encoding.UTF8;
        Console.InputEncoding = Encoding.UTF8;
        Console.Clear();
        Console.WriteLine("╔══════════════════════════════════════╗");
        Console.WriteLine("║      🐺  MÁY CHỦ MA SÓI  🐺           ║");
        Console.WriteLine("║         Version 1.0.7                ║");
        Console.WriteLine("╚══════════════════════════════════════╝\n");

        KiemTraKetNoiSQL();

        string localIp = "localhost";
        try
        {
            var host = System.Net.Dns.GetHostEntry(System.Net.Dns.GetHostName());
            foreach (var ip in host.AddressList)
                if (ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
                { localIp = ip.ToString(); break; }
        }
        catch { }

        var http = new HttpListener();
        bool boundAll = false;
        try
        {
            http.Prefixes.Add($"http://+:{PORT}/");
            http.Start();
            boundAll = true;
        }
        catch
        {
            http = new HttpListener();
            http.Prefixes.Add($"http://localhost:{PORT}/");
            http.Start();
        }

        if (boundAll)
        {
            Ghi($"✅ Máy chủ lắng nghe TẤT CẢ địa chỉ", ConsoleColor.Green);
            Ghi($"🖥️  Máy tính  : http://localhost:{PORT}", ConsoleColor.Green);
            Ghi($"📱 Điện thoại : http://{localIp}:{PORT}", ConsoleColor.Cyan);
        }
        else
        {
            Ghi($"⚠️  Chỉ chạy trên localhost", ConsoleColor.Yellow);
        }
        Ghi($"Cần ít nhất {MIN_PLAYERS} người\n");

        while (true)
        {
            var ctx = await http.GetContextAsync();
            if (ctx.Request.IsWebSocketRequest)
                _ = Task.Run(() => XuLyWebSocket(ctx));
            else
                PhucVuFile(ctx);
        }
    }

    static async Task XuLyWebSocket(HttpListenerContext ctx)
    {
        WebSocketContext wsCtx;
        try { wsCtx = await ctx.AcceptWebSocketAsync(null); }
        catch { ctx.Response.Close(); return; }

        var ws = wsCtx.WebSocket;
        Player? me = null;
        var buf = new byte[8192];

        try
        {
            while (ws.State == WebSocketState.Open)
            {
                WebSocketReceiveResult res;
                try { res = await ws.ReceiveAsync(new ArraySegment<byte>(buf), CancellationToken.None); }
                catch { break; }
                if (res.MessageType == WebSocketMessageType.Close) break;

                string raw = Encoding.UTF8.GetString(buf, 0, res.Count).Trim();
                if (string.IsNullOrEmpty(raw)) continue;

                JsonDocument doc;
                try { doc = JsonDocument.Parse(raw); }
                catch { continue; }

                var root = doc.RootElement;
                string mType = root.TryGetProperty("type", out var tp) ? tp.GetString() ?? "" : "";
                var data = root.TryGetProperty("data", out var dp) ? dp : default;

                if (mType != "ping") Ghi($"📨 [{me?.Name ?? "?"}] → {mType}", ConsoleColor.Cyan);

                switch (mType)
                {
                    case "create_room":
                        {
                            if (me != null) break;
                            string pname = LayChuoi(data, "playerName", "Ẩn Danh").Trim();
                            string gender = LayChuoi(data, "gender", "male");
                            int maxP = Math.Clamp(LaySo(data, "maxPlayers", 7), 6, 8);
                            if (string.IsNullOrWhiteSpace(pname)) { Gui(ws, new { type = "error", message = "Tên không được để trống!" }); break; }
                            string pid = Guid.NewGuid().ToString();
                            string rid = TaoMaPhong();
                            me = new Player(pid, pname, ws) { Gender = gender, IsHost = true, RoomId = rid, MaxPlayers = maxP };
                            lock (pLock) players.Add(me);
                            Ghi($"🏠 [{pname}] tạo phòng {rid}", ConsoleColor.Green);
                            Gui(ws, new { type = "room_created", roomId = rid, playerId = pid, state = TaoTrangThaiPhong(pid, rid) });
                            break;
                        }

                    case "join_room":
                        {
                            if (me != null) break;
                            string pname = LayChuoi(data, "playerName", "Ẩn Danh").Trim();
                            string gender = LayChuoi(data, "gender", "male");
                            string rid = LayChuoi(data, "roomId", "").ToUpper().Trim();
                            Player? host;
                            lock (pLock) host = players.FirstOrDefault(p => p.IsHost && string.Equals(p.RoomId, rid, StringComparison.OrdinalIgnoreCase));
                            if (host == null) { Gui(ws, new { type = "error", message = $"Phòng '{rid}' không tồn tại!" }); break; }
                            if (gameStarted) { Gui(ws, new { type = "error", message = "Trò chơi đã bắt đầu!" }); break; }
                            int curCount; lock (pLock) curCount = players.Count(p => p.RoomId == rid);
                            if (curCount >= host.MaxPlayers) { Gui(ws, new { type = "error", message = "Phòng đã đầy!" }); break; }
                            bool nameTaken; lock (pLock) nameTaken = players.Any(p => p.RoomId == rid && string.Equals(p.Name, pname, StringComparison.OrdinalIgnoreCase));
                            if (nameTaken) { Gui(ws, new { type = "error_name", message = $"Tên '{pname}' đã có người dùng!" }); break; }
                            string pid = Guid.NewGuid().ToString();
                            me = new Player(pid, pname, ws) { Gender = gender, RoomId = rid, MaxPlayers = host.MaxPlayers };
                            lock (pLock) players.Add(me);
                            Ghi($"✅ [{pname}] vào phòng {rid}", ConsoleColor.Green);
                            Gui(ws, new { type = "joined_room", roomId = rid, playerId = pid, state = TaoTrangThaiPhong(pid, rid) });
                            List<Player> roommates2; lock (pLock) roommates2 = players.Where(p => p.RoomId == rid && p.Id != pid).ToList();
                            foreach (var p in roommates2) Gui(p.WS, new { type = "player_joined", name = pname, state = TaoTrangThaiPhong(p.Id, rid) });
                            break;
                        }

                    case "start_game":
                        if (me == null || !me.IsHost) { Gui(ws, new { type = "error", message = "Chỉ host mới được bắt đầu!" }); break; }
                        if (players.Count < MIN_PLAYERS) { Gui(ws, new { type = "error", message = $"Cần ít nhất {MIN_PLAYERS} người!" }); break; }
                        if (gameStarted) break;
                        Ghi($"🎮 Bắt đầu với {players.Count} người!", ConsoleColor.Yellow);
                        new Thread(BatDauGame).Start();
                        break;

                    case "night_action":
                        if (me != null)
                        {
                            string subType = LayChuoi(data, "type", "(không có type)");
                            string curRole;
                            lock (activeRoleLock) curRole = activeNightRole ?? "(null)";
                            Ghi($"   🔔 night_action [{me.Name}] role=[{me.Role}] subType=[{subType}] activeNightRole=[{curRole}]", ConsoleColor.Yellow);
                            XuLyHanhDongDem(me, data);
                        }
                        break;

                    case "wolf_open_door":
                        if (me != null && me.IsAlive && me.Role == "werewolf")
                            XuLySoiMoCua(me, data);
                        break;

                    case "pick_room":
                        if (me != null && me.IsAlive && phase == "pick_room")
                        {
                            int room = LaySo(data, "room", 0);
                            if (room >= 1 && room <= 5)
                            {
                                me.ChosenRoom = room;
                                GhiBiMat($"🚪 [Người chơi] chọn phòng {room}");
                                int picked, alive;
                                lock (pLock) { picked = players.Count(p => p.IsAlive && p.ChosenRoom > 0); alive = players.Count(p => p.IsAlive); }
                                GuiTatCa(new { type = "room_pick_progress", chosen = picked, total = alive });
                            }
                        }
                        break;

                    case "vote":
                        if (me != null) XuLyBoPieu(me, data);
                        break;

                    case "chat":
                        if (me != null) XuLyChat(me, LayChuoi(data, "message", ""));
                        break;

                    case "ping":
                        Gui(ws, new { type = "pong" });
                        break;

                    case "check_name":
                        {
                            string pname = LayChuoi(data, "playerName", "").Trim();
                            string rid = LayChuoi(data, "roomId", "").ToUpper().Trim();
                            if (string.IsNullOrEmpty(pname)) { Gui(ws, new { type = "name_check_result", available = false, message = "Tên không được để trống!" }); break; }
                            if (!string.IsNullOrEmpty(rid))
                            {
                                bool taken; lock (pLock) taken = players.Any(p => p.RoomId == rid && string.Equals(p.Name, pname, StringComparison.OrdinalIgnoreCase));
                                if (taken) { Gui(ws, new { type = "name_check_result", available = false, message = $"Tên '{pname}' đã có người dùng!" }); break; }
                            }
                            Gui(ws, new { type = "name_check_result", available = true, message = "" });
                            break;
                        }

                    case "lobby_chat":
                        if (me != null)
                        {
                            string msg = LayChuoi(data, "message", "");
                            if (!string.IsNullOrWhiteSpace(msg))
                            {
                                List<Player> roommates; lock (pLock) roommates = players.Where(p => p.RoomId == me.RoomId).ToList();
                                foreach (var p in roommates) Gui(p.WS, new { type = "lobby_chat", sender = me.Name, message = msg });
                            }
                        }
                        break;
                }
            }
        }
        catch (Exception ex) { Ghi("❌ Lỗi WebSocket: " + ex.Message, ConsoleColor.DarkRed); }
        finally
        {
            if (me != null)
            {
                string roomId = me.RoomId;
                lock (pLock) players.Remove(me);
                Ghi($"🚪 [{me.Name}] rời phòng", ConsoleColor.Red);
                List<Player> roommates; lock (pLock) roommates = players.Where(p => p.RoomId == roomId).ToList();
                foreach (var p in roommates) Gui(p.WS, new { type = "player_disconnected", name = me.Name });
            }
            try { if (ws.State != WebSocketState.Closed) await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "", CancellationToken.None); } catch { }
            ws.Dispose();
        }
    }

    static void XuLyChat(Player nguoiGui, string text)
    {
        if (string.IsNullOrWhiteSpace(text)) return;
        if (!nguoiGui.IsAlive)
        {
            GuiToi(new { type = "chat", sender = nguoiGui.Name, message = text, team = "dead" }, p => !p.IsAlive);
            return;
        }

        if (nguoiGui.Role == "werewolf" && phase == "night")
        {
            Ghi($"🐺 [SÓI CHAT ĐÊM] [{nguoiGui.Name}]: {text}", ConsoleColor.Red);
            GuiToi(new { type = "chat", sender = nguoiGui.Name, message = text, team = "wolf" },
                   p => p.Role == "werewolf" && p.IsAlive);
            return;
        }

        Ghi($"💬 [{nguoiGui.Name}] (phase={phase}): {text}", ConsoleColor.White);
        GuiTatCa(new { type = "chat", sender = nguoiGui.Name, message = text, team = "all" });
    }

    static void BatDauGame()
    {
        gameStarted = true;
        gameEnded = false;
        gameStartTime = DateTime.Now;
        roundNum = 0;
        wolfDoorAllowance.Clear();
        bonusUnlocked = false;

        var rng = new Random();
        lock (pLock) players = players.OrderBy(_ => rng.Next()).ToList();

        int count = Math.Clamp(players.Count, 6, 8);
        var roles = (ROLE_CONFIGS.TryGetValue(count, out var cfg) ? cfg : ROLE_CONFIGS[8]).OrderBy(_ => rng.Next()).ToArray();

        for (int i = 0; i < players.Count && i < roles.Length; i++)
        {
            players[i].Role = roles[i];
            players[i].IsAlive = true;
            players[i].WitchCanSave = true;
            players[i].WitchCanKill = true;
            players[i].ChosenRoom = 0;
            if (roles[i] == "werewolf") wolfDoorAllowance[players[i].Id] = 1;
        }

        Ghi("🎭 PHÂN VAI:", ConsoleColor.Yellow);
        foreach (var p in players) GhiBiMat($"  {p.Name,-12} → {TenVaiTro(p.Role)}");

        var tatCaSoi = players.Where(p => p.Role == "werewolf").Select(p => new { id = p.Id, name = p.Name, gender = p.Gender }).Cast<object>().ToList();
        foreach (var p in players)
            Gui(p.WS, new { type = "role_assigned", role = p.Role, werewolfTeam = p.Role == "werewolf" ? tatCaSoi : null });

        Thread.Sleep(2000);
        GuiTatCa(new { type = "game_started", state = TaoTrangThaiPhong() });
        Thread.Sleep(500);
        BatDauDem();
    }

    static void ChayPhaseChonPhong()
    {
        phase = "pick_room";
        lock (pLock) foreach (var p in players.Where(x => x.IsAlive)) p.ChosenRoom = 0;

        Ghi($"   🚪 Chọn phòng — {ROOM_PICK_TIME}s", ConsoleColor.Magenta);
        GuiTatCa(new { type = "pick_room_start", duration = ROOM_PICK_TIME * 1000 });

        for (int i = ROOM_PICK_TIME; i >= 0; i--)
        {
            int picked, aliveVillagers;
            lock (pLock)
            {
                picked = players.Count(p => p.IsAlive && p.Role != "werewolf" && p.ChosenRoom > 0);
                aliveVillagers = players.Count(p => p.IsAlive && p.Role != "werewolf");
            }
            if (picked >= aliveVillagers) { Ghi($"   ✅ Tất cả dân làng đã chọn phòng", ConsoleColor.DarkGray); break; }
            if (i == 0) Ghi($"   ⏰ Hết giờ chọn phòng", ConsoleColor.DarkYellow);
            Thread.Sleep(1000);
        }

        var rng = new Random();
        lock (pLock)
        {
            foreach (var p in players.Where(x => x.IsAlive && x.Role != "werewolf" && x.ChosenRoom == 0))
            {
                p.ChosenRoom = rng.Next(1, 6);
                GhiBiMat($"   🎲 [Người chơi] tự động vào phòng {p.ChosenRoom}");
            }
        }

        GhiBiMat("   📋 Sơ đồ phòng đêm nay:");
        lock (pLock)
        {
            foreach (var p in players.Where(x => x.IsAlive && x.Role != "werewolf"))
                GhiBiMat($"      [Dân làng] → Phòng {p.ChosenRoom}");
            foreach (var p in players.Where(x => x.IsAlive && x.Role == "werewolf"))
                GhiBiMat($"      [Ma Sói] → không có phòng");
        }

        GuiTatCa(new { type = "pick_room_done" });
    }

    static void BatDauDem()
    {
        roundNum++;
        nightKillId = null;
        protectId = null;
        witchSaveId = null;
        witchKillId = null;
        witchUsedPotionThisNight = null;
        wolfVotes.Clear();
        lock (huntLock)
        {
            huntStates.Clear();
            wolvesFinishedHunt.Clear();
            huntTargetId = null;
            openedDoorResults.Clear();
            activeHuntingWolfId = null;
        }
        lock (activeRoleLock) activeNightRole = null;
        phase = "night";

        Ghi($"\n🌙 ══ ĐÊM {roundNum} ══ Còn sống: {players.Count(p => p.IsAlive)} người", ConsoleColor.DarkCyan);
        GuiTatCa(new { type = "phase_change", phase = "night", round = roundNum });
        Thread.Sleep(2000);

        new Thread(() =>
        {
            ChayPhaseChonPhong();
            Thread.Sleep(800);

            foreach (var role in NIGHT_ORDER)
            {
                if (role == "werewolf")
                {
                    ChayPhaseSoiDiSan();
                    Thread.Sleep(WOLF_PHASE_END_DELAY);
                }
                else
                {
                    ChayPhaseDem(role);
                }
            }

            lock (activeRoleLock) activeNightRole = null;
            TongKetDem();
        }).Start();
    }

    static void ChayPhaseDem(string role)
    {
        List<Player> nguoiHanhDong;
        lock (pLock) nguoiHanhDong = players.Where(p => p.Role == role && p.IsAlive).ToList();

        if (nguoiHanhDong.Count == 0)
        {
            Ghi($"   ⏭️  Bỏ qua — không có {TenVaiTro(role)} nào còn sống", ConsoleColor.DarkGray);
            Thread.Sleep(300);
            return;
        }

        lock (activeRoleLock) activeNightRole = role;
        Ghi($"   ▶️  [{TenVaiTro(role)}] đang hành động...", ConsoleColor.Magenta);
        GuiTatCa(new { type = "night_phase", role, roleVN = TenVaiTro(role) });

        foreach (var p in nguoiHanhDong) p.HasUsedSkill = false;

        var localEvent = new ManualResetEventSlim(false);
        nightActionEvent = localEvent;

        var danhSachSong = players.Where(p => p.IsAlive)
            .Select(p => new { id = p.Id, name = p.Name, gender = p.Gender }).Cast<object>().ToList();

        foreach (var p in nguoiHanhDong)
        {
            var mucTieu = danhSachSong.Cast<dynamic>().Where(x => (string)x.id != p.Id).Select(x => (object)x).ToList();

            if (role == "witch")
            {
                string? killTargetId;
                lock (pLock) killTargetId = nightKillId;
                Ghi($"   🧙 [PHÙ THỦY] Gửi your_turn → nightKillTarget={killTargetId ?? "null"} canSave={p.WitchCanSave} canKill={p.WitchCanKill}", ConsoleColor.Magenta);
                Gui(p.WS, new
                {
                    type = "your_turn",
                    role,
                    alivePlayers = mucTieu,
                    nightKillTarget = killTargetId,
                    witchPotions = new { save = p.WitchCanSave, kill = p.WitchCanKill },
                    myRoom = p.ChosenRoom
                });
            }
            else
            {
                var mucTieuGuard = role == "guard" ? danhSachSong : (IEnumerable<object>)mucTieu;
                Gui(p.WS, new { type = "your_turn", role, alivePlayers = mucTieuGuard, myRoom = p.ChosenRoom });
            }
        }

        int maxWait = (role == "witch") ? WITCH_ROLE_TIME : ROLE_TIME;
        DateTime startTime = DateTime.Now;
        bool allDone = false;

        while (!allDone && (DateTime.Now - startTime).TotalSeconds < maxWait)
        {
            int msRemaining = (int)Math.Max(0, (maxWait * 1000) - (DateTime.Now - startTime).TotalMilliseconds);
            if (msRemaining == 0) break;

            localEvent.Wait(Math.Min(msRemaining, 200));
            localEvent.Reset();

            lock (pLock) allDone = nguoiHanhDong.All(p => p.HasUsedSkill);

            if (allDone)
            {
                double daQua = (DateTime.Now - startTime).TotalSeconds;
                Ghi($"   ✅ {TenVaiTro(role)} xong sớm sau {daQua:F2}s (tiết kiệm {maxWait - daQua:F2}s)", ConsoleColor.Green);
                break;
            }
        }

        List<Player> chuaHanhDong;
        lock (pLock) chuaHanhDong = nguoiHanhDong.Where(x => !x.HasUsedSkill).ToList();

        if (chuaHanhDong.Count > 0)
        {
            Ghi($"   ⏰ Hết giờ {TenVaiTro(role)} — tự động xử lý cho {chuaHanhDong.Count} người chưa hành động", ConsoleColor.DarkYellow);

            var rng = new Random();
            foreach (var p in chuaHanhDong)
            {
                p.HasUsedSkill = true;

                if (role == "seer")
                {
                    List<Player> targets;
                    lock (pLock) targets = players.Where(q => q.IsAlive && q.Id != p.Id).ToList();
                    if (targets.Count > 0)
                    {
                        var autoTarget = targets[rng.Next(targets.Count)];
                        bool laSoi = autoTarget.Role == "werewolf";
                        int? phongHienThi = (autoTarget.Role != "werewolf") ? autoTarget.ChosenRoom : (int?)null;
                        Gui(p.WS, new
                        {
                            type = "seer_result",
                            targetId = autoTarget.Id,
                            targetName = autoTarget.Name,
                            targetGender = autoTarget.Gender,
                            targetRole = autoTarget.Role,
                            isWerewolf = laSoi,
                            targetRoom = phongHienThi,
                            autoSeer = true
                        });
                        Ghi($"   ⏰ [TIÊN TRI] Auto soi [Người chơi]", ConsoleColor.DarkYellow);
                    }
                }
                else if (role == "guard")
                {
                    List<Player> targets;
                    lock (pLock) targets = players.Where(q => q.IsAlive).ToList();
                    if (targets.Count > 0)
                    {
                        var autoTarget = targets[rng.Next(targets.Count)];
                        protectId = autoTarget.Id;
                        p.GuardedSelfLastNight = (autoTarget.Id == p.Id);
                        Ghi($"   ⏰ [BẢO VỆ] Auto bảo vệ [Người chơi]", ConsoleColor.DarkYellow);
                    }
                }
            }
        }

        nightActionEvent = null;
        localEvent.Dispose();
        lock (pLock) foreach (var p in nguoiHanhDong) p.HasUsedSkill = false;
    }

    static void GuiLaiYourTurn(Player p, string role)
    {
        var danhSachSong = players.Where(q => q.IsAlive)
            .Select(q => new { id = q.Id, name = q.Name, gender = q.Gender }).Cast<object>().ToList();

        if (role == "guard")
        {
            Gui(p.WS, new { type = "your_turn", role = "guard", alivePlayers = danhSachSong, myRoom = p.ChosenRoom });
        }
        else if (role == "seer")
        {
            var mucTieu = danhSachSong.Cast<dynamic>().Where(x => (string)x.id != p.Id).Select(x => (object)x).ToList();
            Gui(p.WS, new { type = "your_turn", role = "seer", alivePlayers = mucTieu, myRoom = p.ChosenRoom });
        }
        else if (role == "witch")
        {
            var mucTieu = danhSachSong.Cast<dynamic>().Where(x => (string)x.id != p.Id).Select(x => (object)x).ToList();
            Gui(p.WS, new
            {
                type = "your_turn",
                role = "witch",
                alivePlayers = mucTieu,
                nightKillTarget = nightKillId,
                witchPotions = new { save = p.WitchCanSave, kill = p.WitchCanKill },
                myRoom = p.ChosenRoom
            });
        }

        Gui(p.WS, new { type = "action_log", actor = "system", message = $"Vui lòng chọn lại mục tiêu hợp lệ!" });
    }

    static void XuLyHanhDongDem(Player p, JsonElement data)
    {
        if (!p.IsAlive || phase != "night") return;

        string loaiHanhDong = LayChuoi(data, "type", "");

        if (p.Role == "werewolf" && loaiHanhDong == "wolf_choose_target")
        {
            if (wolfVotes.ContainsKey(p.Id)) return;
            string tid = LayChuoi(data, "target", LayChuoi(data, "targetId", ""));
            if (string.IsNullOrEmpty(tid)) return;

            Player? mucTieuSoi;
            lock (pLock) mucTieuSoi = players.Find(x => x.Id == tid && x.IsAlive && x.Role != "werewolf");
            if (mucTieuSoi == null)
            {
                Ghi($"   ⚠️ Wolf vote target không hợp lệ!", ConsoleColor.DarkYellow);
                return;
            }

            wolfVotes[p.Id] = mucTieuSoi.Id;
            wolfVoteEvent?.Set();

            List<Player> danhSachSoi; lock (pLock) danhSachSoi = players.Where(x => x.IsAlive && x.Role == "werewolf").ToList();
            int tongSoSoi = danhSachSoi.Count;
            int soVote = wolfVotes.Count;
            int soVoteTid = wolfVotes.Values.Count(v => v == mucTieuSoi.Id);
            int canMaj = (tongSoSoi / 2) + 1;
            bool daDatDaSo = soVoteTid >= canMaj;
            bool tatCaDaVote = soVote >= tongSoSoi;
            bool daThongNhat = tatCaDaVote || daDatDaSo;

            Ghi($"   🐺 [Ma Sói] đã chọn mục tiêu — {soVote}/{tongSoSoi} sói đã vote", ConsoleColor.Red);

            GuiToi(new
            {
                type = "wolf_consensus",
                voterName = p.Name,
                killedId = mucTieuSoi.Id,
                killedName = mucTieuSoi.Name,
                votedCount = soVote,
                totalWolves = tongSoSoi,
                isConfirmed = daThongNhat,
                isTie = false,
                majorityReached = daDatDaSo,
                currentTargetVotes = soVoteTid,
                neededVotes = canMaj
            }, x => x.Role == "werewolf" && x.IsAlive);
            return;
        }

        if (p.Role == "werewolf") return;

        if (p.Role == "witch")
        {
            XuLyHanhDongPhuThuy(p, loaiHanhDong, data);
            return;
        }

        string? vaiTroHienTai;
        lock (activeRoleLock) vaiTroHienTai = activeNightRole;

        if (vaiTroHienTai == null || p.Role != vaiTroHienTai)
        {
            Ghi($"   ⚠️ [{p.Role}] không phải lượt hành động — bỏ qua", ConsoleColor.DarkYellow);
            return;
        }

        if (p.Role == "seer")
        {
            if (p.HasUsedSkill) return;

            string tid = LayChuoi(data, "target", LayChuoi(data, "targetId", ""));

            if (string.IsNullOrEmpty(tid))
            {
                Ghi($"   ⚠️ [TIÊN TRI] gửi target rỗng!", ConsoleColor.DarkYellow);
                GuiLaiYourTurn(p, "seer");
                return;
            }

            List<Player> snapPlayers;
            lock (pLock) snapPlayers = players.ToList();

            Player? mucTieu = snapPlayers.FirstOrDefault(x => x.Id == tid && x.IsAlive && x.Id != p.Id);

            if (mucTieu == null)
            {
                Ghi($"   ⚠️ [TIÊN TRI] Target không tìm thấy!", ConsoleColor.DarkYellow);
                GuiLaiYourTurn(p, "seer");
                return;
            }

            p.HasUsedSkill = true;

            var evt = nightActionEvent;
            if (evt != null) evt.Set();

            bool laSoi = mucTieu.Role == "werewolf";
            int? phongHienThi = (mucTieu.Role != "werewolf") ? mucTieu.ChosenRoom : (int?)null;

            Ghi($"   🔮 [TIÊN TRI] đã thực hiện khả năng soi", ConsoleColor.Magenta);

            Gui(p.WS, new
            {
                type = "seer_result",
                targetId = mucTieu.Id,
                targetName = mucTieu.Name,
                targetGender = mucTieu.Gender,
                targetRole = mucTieu.Role,
                isWerewolf = laSoi,
                targetRoom = phongHienThi
            });

            Gui(p.WS, new { type = "action_log", actor = "seer", message = $"Bạn đã soi {mucTieu.Name}" });
            return;
        }

        if (p.Role == "guard")
        {
            if (p.HasUsedSkill) return;

            string tid = LayChuoi(data, "target", LayChuoi(data, "targetId", LayChuoi(data, "guardTarget", "")));

            if (string.IsNullOrEmpty(tid))
            {
                Ghi($"   ⚠️ [BẢO VỆ] gửi target rỗng!", ConsoleColor.DarkYellow);
                GuiLaiYourTurn(p, "guard");
                return;
            }

            Player? targetPlayer;
            lock (pLock) targetPlayer = players.Find(x => x.Id == tid && x.IsAlive);

            if (targetPlayer == null)
            {
                Ghi($"   ⚠️ [BẢO VỆ] Target không tồn tại!", ConsoleColor.DarkYellow);
                GuiLaiYourTurn(p, "guard");
                return;
            }

            if (tid == p.Id && p.GuardedSelfLastNight)
            {
                Ghi($"   ⚠️ [BẢO VỆ] từ chối tự bảo vệ 2 đêm liên tiếp", ConsoleColor.DarkYellow);
                Gui(p.WS, new { type = "guard_denied", reason = "Không được tự bảo vệ 2 đêm liên tiếp! Hãy chọn người khác." });
                GuiLaiYourTurn(p, "guard");
                return;
            }

            p.GuardedSelfLastNight = (tid == p.Id);
            protectId = targetPlayer.Id;
            p.HasUsedSkill = true;

            var evt = nightActionEvent;
            if (evt != null) evt.Set();

            Ghi($"   🛡️ [BẢO VỆ] đã thực hiện khả năng bảo vệ", ConsoleColor.Green);
            Gui(p.WS, new { type = "guard_confirmed", targetId = targetPlayer.Id, targetName = targetPlayer.Name });
            Gui(p.WS, new { type = "action_log", actor = "guard", message = $"Bạn đang bảo vệ {targetPlayer.Name} đêm nay" });
            return;
        }
    }

    static void ChayPhaseSoiDiSan()
    {
        List<Player> danhSachSoi;
        lock (pLock) danhSachSoi = players.Where(p => p.IsAlive && p.Role == "werewolf").ToList();
        if (danhSachSoi.Count == 0) return;

        lock (activeRoleLock) activeNightRole = "werewolf";
        Ghi($"   🐺 Ma Sói đang chọn mục tiêu...", ConsoleColor.Red);
        GuiTatCa(new { type = "night_phase", role = "werewolf", roleVN = TenVaiTro("werewolf") });

        var mucTieuDanLang = players.Where(p => p.IsAlive && p.Role != "werewolf")
            .Select(p => new { id = p.Id, name = p.Name, gender = p.Gender }).Cast<object>().ToList();

        var localWolfEvent = new ManualResetEventSlim(false);
        wolfVoteEvent = localWolfEvent;

        foreach (var s in danhSachSoi)
        {
            s.HasUsedSkill = false;
            Gui(s.WS, new
            {
                type = "wolf_your_turn_hunt",
                targets = mucTieuDanLang,
                doorsAllowed = wolfDoorAllowance.TryGetValue(s.Id, out int d) ? d : 1,
                myRoom = s.ChosenRoom
            });
        }

        {
            bool xongNgay; lock (pLock) xongNgay = danhSachSoi.All(s => wolfVotes.ContainsKey(s.Id));
            if (xongNgay)
            {
                Ghi($"   ✅ Tất cả sói đã vote ngay lập tức!", ConsoleColor.Green);
                wolfVoteEvent = null;
                localWolfEvent.Dispose();
                goto ProcessVote;
            }
        }

        DateTime wolfVoteStart = DateTime.Now;
        while (true)
        {
            int msRemaining = (int)Math.Max(0, (ROLE_TIME * 1000) - (DateTime.Now - wolfVoteStart).TotalMilliseconds);
            if (msRemaining == 0) break;

            localWolfEvent.Wait(Math.Min(msRemaining, 200));
            localWolfEvent.Reset();

            bool tatCaDaVote; lock (pLock) tatCaDaVote = danhSachSoi.All(s => wolfVotes.ContainsKey(s.Id));
            if (tatCaDaVote)
            {
                double daQua = (DateTime.Now - wolfVoteStart).TotalSeconds;
                Ghi($"   ✅ Tất cả Ma Sói đã vote sau {daQua:F2}s (tiết kiệm {ROLE_TIME - daQua:F2}s)", ConsoleColor.Green);
                break;
            }

            if ((DateTime.Now - wolfVoteStart).TotalSeconds >= ROLE_TIME) break;
        }

        {
            bool coSoiChuaVote; lock (pLock) coSoiChuaVote = danhSachSoi.Any(s => !wolfVotes.ContainsKey(s.Id));
            if (coSoiChuaVote)
            {
                Ghi($"   ⏰ Hết giờ — tự động chọn mục tiêu cho sói chưa vote", ConsoleColor.DarkYellow);
                var rng = new Random();
                lock (pLock)
                    foreach (var s in danhSachSoi.Where(x => !wolfVotes.ContainsKey(x.Id)))
                    {
                        var ds = mucTieuDanLang.Cast<dynamic>().ToList();
                        if (ds.Count > 0) wolfVotes[s.Id] = (string)ds[rng.Next(ds.Count)].id;
                    }
            }
        }

        wolfVoteEvent = null;
        localWolfEvent.Dispose();

    ProcessVote:
        var bangVote = new Dictionary<string, int>();
        foreach (var v in wolfVotes.Values) bangVote[v] = bangVote.TryGetValue(v, out int c) ? c + 1 : 1;

        if (bangVote.Count == 0)
        {
            lock (activeRoleLock) activeNightRole = null;
            GuiToi(new { type = "wolf_hunt_cancelled" }, p => p.Role == "werewolf" && p.IsAlive);
            CapNhatBonusCua(false);
            return;
        }

        int soVoteMax = bangVote.Values.Max();
        var danhSachTop = bangVote.Where(kv => kv.Value == soVoteMax).ToList();

        if (danhSachTop.Count > 1)
        {
            var tenHoa = danhSachTop
                .Select(kv => { Player? t; lock (pLock) t = players.Find(p => p.Id == kv.Key); return t?.Name ?? kv.Key; })
                .ToList();
            GuiToi(new
            {
                type = "wolf_consensus",
                isConfirmed = true,
                isTie = true,
                tieNames = tenHoa,
                message = $"Hòa phiếu giữa: {string.Join(", ", tenHoa)} — không săn đêm nay!"
            }, x => x.Role == "werewolf" && x.IsAlive);
            lock (activeRoleLock) activeNightRole = null;
            CapNhatBonusCua(false);
            return;
        }

        Thread.Sleep(500);

        string resolvedHuntTargetId = danhSachTop[0].Key;
        Player? nguoiBiSan;
        lock (pLock) nguoiBiSan = players.Find(p => p.Id == resolvedHuntTargetId);

        lock (huntLock)
        {
            huntTargetId = resolvedHuntTargetId;
            huntStates[huntTargetId] = new WolfHuntState { TargetId = huntTargetId };
        }

        Ghi($"   🎯 Mục tiêu: [Người chơi] — Phòng {nguoiBiSan?.ChosenRoom}", ConsoleColor.Red);

        var roomOccupants = new Dictionary<string, bool>();
        lock (pLock)
        {
            for (int r = 1; r <= 5; r++)
                roomOccupants[r.ToString()] = players.Any(p => p.IsAlive && p.Role != "werewolf" && p.ChosenRoom == r);
        }

        var rngOrder = new Random();
        var soiTheoThuTu = danhSachSoi.OrderBy(_ => rngOrder.Next()).ToList();
        bool coSoiTrung = false;

        for (int soiIdx = 0; soiIdx < soiTheoThuTu.Count; soiIdx++)
        {
            if (coSoiTrung) break;

            var activeSoi = soiTheoThuTu[soiIdx];
            int sooCuaDuocMo = wolfDoorAllowance.TryGetValue(activeSoi.Id, out int d) ? d : 1;

            WolfHuntState state;
            lock (huntLock)
            {
                state = huntStates[huntTargetId!];
                state.DoorsOpenedByCurrentWolf = 0;
            }

            var soiConLai = soiTheoThuTu.Skip(soiIdx + 1).ToList();

            Dictionary<string, object> prevResultsSnap;
            lock (huntLock)
            {
                prevResultsSnap = openedDoorResults.ToDictionary(
                    kv => kv.Key.ToString(),
                    kv => (object)new { hit = kv.Value.Hit }
                );
            }

            foreach (var soiCho in soiConLai)
            {
                Gui(soiCho.WS, new
                {
                    type = "wolf_wait_teammate",
                    activeWolfName = activeSoi.Name,
                    targetName = nguoiBiSan?.Name ?? "?",
                    targetId = huntTargetId,
                    doorsAllowed = wolfDoorAllowance.TryGetValue(soiCho.Id, out int dCho) ? dCho : 1,
                    roomOccupants,
                    previouslyOpenedResults = prevResultsSnap
                });
            }

            List<int> alreadyOpenedList;
            lock (huntLock) alreadyOpenedList = state.OpenedDoors.ToList();

            activeSoi.HasUsedSkill = false;
            lock (huntLock) activeHuntingWolfId = activeSoi.Id;

            var localDoorEvent = new ManualResetEventSlim(false);
            wolfDoorEvent = localDoorEvent;

            if (soiIdx == 0)
            {
                Gui(activeSoi.WS, new
                {
                    type = "wolf_hunt_start",
                    targetId = huntTargetId,
                    targetName = nguoiBiSan?.Name ?? "?",
                    doorsAllowed = sooCuaDuocMo,
                    alreadyOpenedDoors = alreadyOpenedList,
                    roomOccupants,
                    previouslyOpenedResults = prevResultsSnap
                });
            }
            else
            {
                Dictionary<string, object> updatedResults;
                lock (huntLock)
                {
                    updatedResults = openedDoorResults.ToDictionary(
                        kv => kv.Key.ToString(),
                        kv => (object)new { hit = kv.Value.Hit }
                    );
                    alreadyOpenedList = state.OpenedDoors.ToList();
                }

                Gui(activeSoi.WS, new
                {
                    type = "wolf_your_turn_door",
                    targetId = huntTargetId,
                    targetName = nguoiBiSan?.Name ?? "?",
                    doorsAllowed = sooCuaDuocMo,
                    alreadyOpenedDoors = alreadyOpenedList,
                    roomOccupants,
                    previouslyOpenedResults = updatedResults
                });
            }

            DateTime doorWaitStart = DateTime.Now;
            int doorTimeoutMs = ROLE_TIME * 1000;
            while (true)
            {
                int msLeft = (int)Math.Max(0, doorTimeoutMs - (DateTime.Now - doorWaitStart).TotalMilliseconds);
                if (msLeft == 0) break;

                localDoorEvent.Wait(Math.Min(msLeft, 200));
                localDoorEvent.Reset();

                bool soiXong; lock (pLock) soiXong = activeSoi.HasUsedSkill;
                if (soiXong)
                {
                    Ghi($"   ✅ [Ma Sói] mở cửa xong", ConsoleColor.DarkGray);
                    break;
                }

                if ((DateTime.Now - doorWaitStart).TotalMilliseconds >= doorTimeoutMs) break;
            }

            wolfDoorEvent = null;
            localDoorEvent.Dispose();

            bool xongKiemTra; lock (pLock) xongKiemTra = activeSoi.HasUsedSkill;
            if (!xongKiemTra)
            {
                TuDongMoCua(activeSoi, huntTargetId!, sooCuaDuocMo);
            }

            bool isHitNow;
            lock (huntLock) isHitNow = huntStates[huntTargetId!].IsHit;

            if (isHitNow)
            {
                coSoiTrung = true;
                Ghi($"   💥 [Ma Sói] TRÚNG phòng {nguoiBiSan?.ChosenRoom} — [Người chơi] sẽ bị giết!", ConsoleColor.Red);

                Dictionary<string, object> finalResults;
                lock (huntLock)
                {
                    finalResults = openedDoorResults.ToDictionary(
                        kv => kv.Key.ToString(),
                        kv => (object)new { hit = kv.Value.Hit }
                    );
                }

                foreach (var soiKhac in soiTheoThuTu.Where(s => s.Id != activeSoi.Id))
                {
                    Gui(soiKhac.WS, new
                    {
                        type = "wolf_target_caught",
                        catcherName = activeSoi.Name,
                        targetName = nguoiBiSan?.Name ?? "?",
                        targetId = huntTargetId,
                        room = nguoiBiSan?.ChosenRoom ?? 0,
                        previouslyOpenedResults = finalResults
                    });
                }
            }
            else
            {
                Dictionary<string, object> currentResults;
                List<int> currentOpenedDoors;
                lock (huntLock)
                {
                    currentResults = openedDoorResults.ToDictionary(
                        kv => kv.Key.ToString(),
                        kv => (object)new { hit = kv.Value.Hit }
                    );
                    currentOpenedDoors = state.OpenedDoors.ToList();
                }

                foreach (var soiCho in soiConLai)
                {
                    Gui(soiCho.WS, new
                    {
                        type = "wolf_door_update",
                        openerName = activeSoi.Name,
                        alreadyOpenedDoors = currentOpenedDoors,
                        previouslyOpenedResults = currentResults
                    });
                }
            }
        }

        if (coSoiTrung)
        {
            nightKillId = huntTargetId;
            CapNhatBonusCua(true);
        }
        else
        {
            CapNhatBonusCua(false);
        }

        lock (activeRoleLock) activeNightRole = null;

        GuiToi(new
        {
            type = "wolf_phase_done",
            hit = nightKillId != null,
            targetName = nightKillId != null ? (players.Find(p => p.Id == nightKillId)?.Name ?? "?") : null
        }, p => p.Role == "werewolf" && p.IsAlive);
    }

    static void XuLySoiMoCua(Player soi, JsonElement data)
    {
        int waitMs = 0;
        while (waitMs < 3000)
        {
            lock (huntLock)
            {
                if (huntTargetId != null && huntStates.ContainsKey(huntTargetId))
                    break;
            }
            Thread.Sleep(100);
            waitMs += 100;
        }

        lock (huntLock)
        {
            if (activeHuntingWolfId != null && activeHuntingWolfId != soi.Id)
            {
                Ghi($"   ⚠️ wolf_open_door — không phải lượt — bỏ qua", ConsoleColor.DarkYellow);
                return;
            }
            activeHuntingWolfId = soi.Id;
        }

        string? currentTarget;
        WolfHuntState? state;
        lock (huntLock)
        {
            currentTarget = huntTargetId;
            if (currentTarget == null || !huntStates.TryGetValue(currentTarget, out state))
            {
                Ghi($"   ⚠️ wolf_open_door nhưng huntTargetId=null — bỏ qua", ConsoleColor.DarkYellow);
                return;
            }
        }

        if (soi.HasUsedSkill) return;

        int phong = LaySo(data, "room", 0);
        if (phong < 1 || phong > 5) return;

        int duocPhep = wolfDoorAllowance.TryGetValue(soi.Id, out int d) ? d : 1;

        lock (huntLock)
        {
            if (state.OpenedDoors.Contains(phong))
            {
                Ghi($"   ⚠️ [Ma Sói] mở lại phòng {phong} đã mở — bỏ qua", ConsoleColor.DarkYellow);
                return;
            }
            if (state.DoorsOpenedByCurrentWolf >= duocPhep) return;
        }

        Player? mucTieu; lock (pLock) mucTieu = players.Find(p => p.Id == currentTarget);
        if (mucTieu == null) return;

        bool trung, phongCoNguoi;
        bool duocBaoVe;

        lock (huntLock)
        {
            state.OpenedDoors.Add(phong);
            state.DoorsOpenedByCurrentWolf++;

            trung = (phong == mucTieu.ChosenRoom);
            duocBaoVe = !string.IsNullOrEmpty(protectId) && (currentTarget == protectId);

            if (trung && !duocBaoVe)
                state.IsHit = true;
            else if (trung && duocBaoVe)
            {
                state.IsHit = false;
                state.DoorsOpenedByCurrentWolf = duocPhep;
            }

            lock (pLock) phongCoNguoi = players.Any(p => p.IsAlive && p.Role != "werewolf" && p.ChosenRoom == phong);
            openedDoorResults[phong] = new DoorResult { Hit = trung && !duocBaoVe, HasOccupant = phongCoNguoi };
        }

        if (trung && !duocBaoVe)
            Ghi($"   🐺 [Ma Sói] mở cửa {phong} → TRÚNG!", ConsoleColor.Red);
        else if (trung && duocBaoVe)
            Ghi($"   🛡️ [Ma Sói] mở cửa {phong} → TRÚNG nhưng nạn nhân ĐƯỢC BẢO VỆ!", ConsoleColor.Green);
        else
            Ghi($"   🐺 [Ma Sói] mở cửa {phong} → {(phongCoNguoi ? "Có người (không phải mục tiêu)" : "Trống")}", ConsoleColor.DarkGray);

        bool hitChoSoi = trung && !duocBaoVe;

        Gui(soi.WS, new
        {
            type = "door_opened",
            room = phong,
            hit = hitChoSoi,
            targetName = hitChoSoi ? mucTieu.Name : (string?)null,
            hasOccupant = phongCoNguoi,
            doorsLeft = duocPhep - state.DoorsOpenedByCurrentWolf
        });

        bool hetLuot;
        lock (huntLock) hetLuot = state.DoorsOpenedByCurrentWolf >= duocPhep;

        if (hitChoSoi || hetLuot)
        {
            soi.HasUsedSkill = true;
            wolfDoorEvent?.Set();
        }

        lock (huntLock) activeHuntingWolfId = null;
    }

    static void TuDongMoCua(Player soi, string targetId, int sooCuaDuocMo)
    {
        WolfHuntState? state;
        lock (huntLock)
        {
            if (!huntStates.TryGetValue(targetId, out state)) return;
        }

        var rng = new Random();
        Player? mucTieu; lock (pLock) mucTieu = players.Find(p => p.Id == targetId);
        if (mucTieu == null) return;

        bool duocBaoVe = !string.IsNullOrEmpty(protectId) && (targetId == protectId);

        while (true)
        {
            int doorsOpened;
            lock (huntLock) doorsOpened = state.DoorsOpenedByCurrentWolf;
            if (doorsOpened >= sooCuaDuocMo) break;

            int phong;
            lock (huntLock)
            {
                do { phong = rng.Next(1, 6); } while (state.OpenedDoors.Contains(phong));
                state.OpenedDoors.Add(phong);
                state.DoorsOpenedByCurrentWolf++;
            }

            bool phongCoNguoi;
            lock (pLock) phongCoNguoi = players.Any(p => p.IsAlive && p.Role != "werewolf" && p.ChosenRoom == phong);
            bool trung = (phong == mucTieu.ChosenRoom);

            lock (huntLock)
            {
                openedDoorResults[phong] = new DoorResult { Hit = trung && !duocBaoVe, HasOccupant = phongCoNguoi };
                if (trung && !duocBaoVe) { state.IsHit = true; break; }
                if (trung && duocBaoVe) break;
            }
        }

        soi.HasUsedSkill = true;
        Ghi($"   🤖 Tự động mở cửa cho [Ma Sói]", ConsoleColor.DarkGray);
    }

    static void CapNhatBonusCua(bool coSoiTrung)
    {
        if (coSoiTrung)
        {
            bonusUnlocked = false;
            lock (pLock)
                foreach (var s in players.Where(p => p.IsAlive && p.Role == "werewolf"))
                    wolfDoorAllowance[s.Id] = 1;
        }
        else
        {
            if (!bonusUnlocked)
            {
                bonusUnlocked = true;
                Ghi($"   🔓 BONUS: Từ đêm sau mỗi sói mở được 2 cửa", ConsoleColor.Yellow);
            }
            lock (pLock)
                foreach (var s in players.Where(p => p.IsAlive && p.Role == "werewolf"))
                    wolfDoorAllowance[s.Id] = 2;
        }
    }

    static void XuLyHanhDongPhuThuy(Player p, string loaiHanhDong, JsonElement data)
    {
        if (!p.IsAlive) return;
        if (p.HasUsedSkill) return;

        Ghi($"   🧙 [PHÙ THỦY] → {loaiHanhDong}", ConsoleColor.Magenta);

        if (loaiHanhDong == "witch_save")
        {
            if (!p.WitchCanSave)
            {
                Gui(p.WS, new { type = "error", message = "Bạn đã dùng hết bình cứu!" });
                return;
            }

            if (string.IsNullOrEmpty(nightKillId))
            {
                Gui(p.WS, new { type = "error", message = "Đêm nay không có ai bị Ma Sói tấn công!" });
                return;
            }

            string sid = LayChuoi(data, "saveTarget", LayChuoi(data, "target", LayChuoi(data, "targetId", "")));
            if (string.IsNullOrEmpty(sid)) return;

            Player? saveTarget;
            lock (pLock) saveTarget = players.Find(x => x.Id == sid && x.IsAlive);
            if (saveTarget == null) return;

            if (saveTarget.Id != nightKillId)
            {
                Gui(p.WS, new { type = "error", message = $"Bạn chỉ có thể cứu người bị Ma Sói tấn công!" });
                return;
            }

            witchSaveId = saveTarget.Id;
            witchUsedPotionThisNight = "save";
            p.WitchCanSave = false;
            p.HasUsedSkill = true;

            var evt = nightActionEvent;
            if (evt != null) evt.Set();

            Ghi($"   🧪 [PHÙ THỦY] đã sử dụng THUỐC CỨU ✅", ConsoleColor.Green);
            Gui(p.WS, new { type = "witch_potion_update", witchPotions = new { save = p.WitchCanSave, kill = p.WitchCanKill } });
            return;
        }

        if (loaiHanhDong == "witch_kill")
        {
            if (!p.WitchCanKill)
            {
                Gui(p.WS, new { type = "error", message = "Bạn đã dùng hết bình độc!" });
                return;
            }

            string kid = LayChuoi(data, "killTarget", LayChuoi(data, "target", LayChuoi(data, "targetId", "")));
            if (string.IsNullOrEmpty(kid)) return;

            Player? killTarget;
            lock (pLock) killTarget = players.Find(x => x.Id == kid && x.IsAlive);
            if (killTarget == null) return;

            if (killTarget.Id == p.Id)
            {
                Gui(p.WS, new { type = "error", message = "Bạn không thể tự đầu độc chính mình!" });
                return;
            }

            witchKillId = killTarget.Id;
            witchUsedPotionThisNight = "kill";
            p.WitchCanKill = false;
            p.HasUsedSkill = true;

            var evt = nightActionEvent;
            if (evt != null) evt.Set();

            Ghi($"   🧪 [PHÙ THỦY] đã sử dụng THUỐC ĐỘC ✅", ConsoleColor.Red);
            Gui(p.WS, new { type = "witch_potion_update", witchPotions = new { save = p.WitchCanSave, kill = p.WitchCanKill } });
            return;
        }

        if (loaiHanhDong == "witch_skip")
        {
            p.HasUsedSkill = true;

            var evt = nightActionEvent;
            if (evt != null) evt.Set();

            Ghi($"   🧪 [PHÙ THỦY] bỏ qua lượt", ConsoleColor.DarkGray);
            return;
        }
    }

    static void GuiLaiYourTurnChoWitch(Player p)
    {
        var danhSachSong = players.Where(q => q.IsAlive && q.Id != p.Id)
            .Select(q => new { id = q.Id, name = q.Name, gender = q.Gender }).Cast<object>().ToList();

        Gui(p.WS, new
        {
            type = "your_turn",
            role = "witch",
            alivePlayers = danhSachSong,
            nightKillTarget = nightKillId,
            witchPotions = new { save = p.WitchCanSave, kill = p.WitchCanKill },
            myRoom = p.ChosenRoom
        });
    }

    static void TongKetDem()
    {
        Ghi($"   🔍 [TỔNG KẾT ĐÊM] nightKillId=[{nightKillId ?? "null"}] | witchSaveId=[{witchSaveId ?? "null"}] | witchKillId=[{witchKillId ?? "null"}] | protectId=[{protectId ?? "null"}]", ConsoleColor.Yellow);

        var danhSachChet = new List<object>();

        if (nightKillId != null)
        {
            Player? nanNhan; lock (pLock) nanNhan = players.Find(p => p.Id == nightKillId);
            if (nanNhan != null && nanNhan.IsAlive)
            {
                bool duocBaoVe = !string.IsNullOrEmpty(protectId) && (nightKillId == protectId);
                bool duocCuu = !string.IsNullOrEmpty(witchSaveId) && (nightKillId == witchSaveId);
                Ghi($"   🔍 [Nạn nhân] duocBaoVe={duocBaoVe} duocCuu={duocCuu}", ConsoleColor.Yellow);
                if (!duocBaoVe && !duocCuu)
                {
                    nanNhan.IsAlive = false;
                    danhSachChet.Add(new { id = nanNhan.Id, name = nanNhan.Name, role = nanNhan.Role });
                    Ghi($"   💀 [Người chơi] bị Ma Sói giết", ConsoleColor.Red);
                }
                else
                {
                    string lyDo = duocBaoVe ? "Bảo Vệ che chắn" : "Phù Thủy cứu sống";
                    Ghi($"   🛡️ [Người chơi] được cứu ({lyDo}) — sống sót!", ConsoleColor.Green);
                }
            }
        }

        if (witchKillId != null)
        {
            Player? nanNhan; lock (pLock) nanNhan = players.Find(p => p.Id == witchKillId);
            if (nanNhan != null && nanNhan.IsAlive)
            {
                nanNhan.IsAlive = false;
                danhSachChet.Add(new { id = nanNhan.Id, name = nanNhan.Name, role = nanNhan.Role });
                Ghi($"   ☠️ [Người chơi] bị Phù Thủy đầu độc", ConsoleColor.Red);
            }
        }

        if (danhSachChet.Count == 0) Ghi($"   ✨ Đêm bình yên — không ai chết", ConsoleColor.Green);

        GuiTatCa(new { type = "night_results", deaths = danhSachChet });
        if (!KiemTraThangThua()) { Thread.Sleep(2500); if (gameStarted && !gameEnded) BatDauNgay(); }
    }

    static void BatDauNgay()
    {
        phase = "day";
        votes.Clear();
        voteResolved = false;

        Ghi($"\n☀️ ══ NGÀY {roundNum} ══", ConsoleColor.Yellow);
        GuiTatCa(new { type = "phase_change", phase = "day", round = roundNum, state = TaoTrangThaiPhong() });
        GuiTatCa(new { type = "discussion_start", duration = DISCUSS_TIME * 1000, chatEnabled = true });

        Thread.Sleep(DISCUSS_TIME * 1000);
        if (!gameStarted || gameEnded) return;

        var danhSachBoPieu = players.Where(p => p.IsAlive)
            .Select(p => new { id = p.Id, name = p.Name, gender = p.Gender }).Cast<object>().ToList();

        GuiTatCa(new { type = "voting_start", duration = VOTE_TIME * 1000, alivePlayers = danhSachBoPieu });
        Thread.Sleep(VOTE_TIME * 1000);
        if (!gameStarted || gameEnded) return;
        GiaiQuyetBoPieu();
    }

    static void XuLyBoPieu(Player nguoiBoPieu, JsonElement data)
    {
        if (!nguoiBoPieu.IsAlive || phase != "day") return;
        if (votes.ContainsKey(nguoiBoPieu.Id)) return;

        string targetId = "";
        bool laPhieuTrang = false;

        if (data.TryGetProperty("targetId", out var tidProp))
        {
            if (tidProp.ValueKind == JsonValueKind.Null || tidProp.ValueKind == JsonValueKind.Undefined)
                laPhieuTrang = true;
            else
                targetId = tidProp.GetString() ?? "";
        }

        if (data.TryGetProperty("abstain", out var absProp) && absProp.ValueKind == JsonValueKind.True)
            laPhieuTrang = true;

        if (laPhieuTrang)
        {
            votes[nguoiBoPieu.Id] = "__abstain__";
            int tongSong; lock (pLock) tongSong = players.Count(p => p.IsAlive);
            GuiTatCa(new { type = "vote_update", voterId = nguoiBoPieu.Id, targetId = (string?)null, voterName = nguoiBoPieu.Name, targetName = "Phiếu Trắng", isAbstain = true });
            if (votes.Count >= tongSong) GiaiQuyetBoPieu();
            return;
        }

        Player? mucTieu; lock (pLock) mucTieu = players.Find(p => p.Id == targetId && p.IsAlive);
        if (mucTieu == null || mucTieu.Id == nguoiBoPieu.Id) return;

        votes[nguoiBoPieu.Id] = targetId;
        int tongSong2; lock (pLock) tongSong2 = players.Count(p => p.IsAlive);
        GuiTatCa(new { type = "vote_update", voterId = nguoiBoPieu.Id, targetId, voterName = nguoiBoPieu.Name, targetName = mucTieu.Name });
        if (votes.Count >= tongSong2) GiaiQuyetBoPieu();
    }

    static void GiaiQuyetBoPieu()
    {
        lock (voteLock) { if (voteResolved) return; voteResolved = true; }
        if (gameEnded) return;

        var bangKiemPhieu = new Dictionary<string, int>();
        foreach (var v in votes.Values.Where(v => v != "__abstain__"))
            bangKiemPhieu[v] = bangKiemPhieu.TryGetValue(v, out int c) ? c + 1 : 1;

        if (bangKiemPhieu.Count == 0)
        {
            GuiTatCa(new { type = "no_execution", message = "Không ai bỏ phiếu hôm nay." });
        }
        else
        {
            int max = bangKiemPhieu.Values.Max();
            var top = bangKiemPhieu.Where(kv => kv.Value == max).ToList();
            if (top.Count > 1)
            {
                GuiTatCa(new { type = "no_execution", message = "Hòa phiếu! Không ai bị treo cổ hôm nay." });
            }
            else
            {
                string idBiXuTu = top[0].Key;
                Player? nguoiBiXuTu; lock (pLock) nguoiBiXuTu = players.Find(p => p.Id == idBiXuTu);
                if (nguoiBiXuTu != null)
                {
                    nguoiBiXuTu.IsAlive = false;
                    GuiTatCa(new { type = "player_executed", playerId = nguoiBiXuTu.Id, name = nguoiBiXuTu.Name, role = nguoiBiXuTu.Role, votes = max });
                }
            }
        }

        if (!KiemTraThangThua() && gameStarted && !gameEnded)
            new Thread(() => { Thread.Sleep(3000); BatDauDem(); }).Start();
    }

    static bool KiemTraThangThua()
    {
        int soSoi = players.Count(p => p.IsAlive && p.Role == "werewolf");
        int soDan = players.Count(p => p.IsAlive && p.Role != "werewolf");
        string? nguoiThang = soSoi == 0 ? "villager" : soSoi >= soDan ? "werewolf" : null;
        if (nguoiThang == null) return false;

        gameEnded = true;
        gameStarted = false;
        int thoiGian = (int)(DateTime.Now - gameStartTime).TotalSeconds;
        GuiTatCa(new { type = "game_over", winner = nguoiThang, rounds = roundNum, duration = thoiGian, players = players.Select(p => new { id = p.Id, name = p.Name, gender = p.Gender, role = p.Role, alive = p.IsAlive }).ToList() });
        LuuKetQuaVaoDB(nguoiThang);
        Thread.Sleep(4000);
        ResetGame();
        return true;
    }

    static void ResetGame()
    {
        phase = "waiting"; roundNum = 0; gameEnded = false;
        nightKillId = protectId = witchSaveId = witchKillId = null;
        witchUsedPotionThisNight = null;
        wolfVotes.Clear(); votes.Clear(); wolfDoorAllowance.Clear();
        lock (huntLock) { huntStates.Clear(); wolvesFinishedHunt.Clear(); openedDoorResults.Clear(); huntTargetId = null; }
        bonusUnlocked = false;
        voteResolved = false;
        nightActionEvent = null;
        wolfVoteEvent = null;
        wolfDoorEvent = null;
        lock (activeRoleLock) activeNightRole = null;
        lock (pLock) foreach (var p in players) p.Reset();
    }

    static object TaoTrangThaiPhong(string? forId = null, string? roomId = null)
    {
        List<Player> snap;
        lock (pLock)
        {
            if (roomId != null) snap = players.Where(p => p.RoomId == roomId).ToList();
            else if (forId != null) { var me = players.FirstOrDefault(p => p.Id == forId); snap = me != null ? players.Where(p => p.RoomId == me.RoomId).ToList() : players.ToList(); }
            else snap = players.ToList();
        }
        string rid = snap.FirstOrDefault(p => p.IsHost)?.RoomId ?? "----";
        int maxP = snap.FirstOrDefault()?.MaxPlayers ?? 7;
        string hostId = snap.FirstOrDefault(p => p.IsHost)?.Id ?? "";
        return new { roomId = rid, hostId, phase, round = roundNum, maxPlayers = maxP, players = snap.Select(p => new { id = p.Id, name = p.Name, gender = p.Gender, alive = p.IsAlive, role = (!p.IsAlive || p.Id == forId) ? p.Role : (string?)null, isMe = p.Id == forId }).ToList() };
    }

    static void Gui(WebSocket ws, object obj)
    {
        try
        {
            if (ws.State != WebSocketState.Open) return;
            byte[] d = JsonSerializer.SerializeToUtf8Bytes(obj);
            ws.SendAsync(new ArraySegment<byte>(d), WebSocketMessageType.Text, true, CancellationToken.None).GetAwaiter().GetResult();
        }
        catch { }
    }

    static void GuiTatCa(object obj) { List<Player> snap; lock (pLock) snap = players.ToList(); foreach (var p in snap) Gui(p.WS, obj); }
    static void GuiToi(object obj, Func<Player, bool> filter) { List<Player> snap; lock (pLock) snap = players.Where(filter).ToList(); foreach (var p in snap) Gui(p.WS, obj); }

    static void PhucVuFile(HttpListenerContext ctx)
    {
        try
        {
            string urlPath = ctx.Request.Url?.AbsolutePath.TrimStart('/') ?? "";
            if (string.IsNullOrEmpty(urlPath)) urlPath = "index.html";
            string[] roots = {
                Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "..", "..", "..", "client"),
                Path.Combine(Directory.GetCurrentDirectory(), "client"),
                Directory.GetCurrentDirectory()
            };
            string? file = roots.Select(r => Path.Combine(r, urlPath.Replace('/', Path.DirectorySeparatorChar))).FirstOrDefault(File.Exists);
            if (file != null)
            {
                ctx.Response.ContentType = Path.GetExtension(file).ToLower() switch
                {
                    ".html" => "text/html;charset=utf-8",
                    ".css" => "text/css;charset=utf-8",
                    ".js" => "application/javascript;charset=utf-8",
                    _ => "application/octet-stream"
                };
                ctx.Response.Headers["Cache-Control"] = "no-cache, no-store, must-revalidate";
                ctx.Response.Headers["Pragma"] = "no-cache";
                ctx.Response.Headers["Expires"] = "0";
                ctx.Response.Headers["ETag"] = File.GetLastWriteTimeUtc(file).Ticks.ToString();
                byte[] bytes = File.ReadAllBytes(file);
                if (Path.GetExtension(file).ToLower() == ".html")
                {
                    string firstLine = Encoding.UTF8.GetString(bytes).Split('\n')[0].Trim();
                    if (firstLine.Contains("BUILD_VERSION"))
                        Ghi($"📄 Serving: {Path.GetFileName(file)} — {firstLine.Replace("<!--", "").Replace("-->", "").Trim()}", ConsoleColor.Cyan);
                }
                ctx.Response.ContentLength64 = bytes.Length;
                ctx.Response.OutputStream.Write(bytes);
            }
            else { ctx.Response.StatusCode = 404; }
        }
        catch { }
        finally { ctx.Response.Close(); }
    }

    static string TaoMaPhong() { const string kyTu = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"; var rng = new Random(); return new string(Enumerable.Range(0, 5).Select(_ => kyTu[rng.Next(kyTu.Length)]).ToArray()); }
    static string LayChuoi(JsonElement el, string key, string def) { if (el.ValueKind == JsonValueKind.Undefined) return def; return el.TryGetProperty(key, out var v) ? v.GetString() ?? def : def; }
    static int LaySo(JsonElement el, string key, int def) { if (el.ValueKind == JsonValueKind.Undefined) return def; return el.TryGetProperty(key, out var v) && v.TryGetInt32(out int i) ? i : def; }

    static void KiemTraKetNoiSQL()
    {
        try { using var c = new SqlConnection(ConnStr); c.Open(); Ghi("✅ Kết nối cơ sở dữ liệu thành công", ConsoleColor.Green); }
        catch (Exception ex) { Ghi("⚠️ Không kết nối được cơ sở dữ liệu (máy chủ vẫn chạy bình thường)", ConsoleColor.Yellow); Ghi("   " + ex.Message, ConsoleColor.DarkYellow); }
    }

    static void LuuKetQuaVaoDB(string nguoiThang)
    {
        try
        {
            using var conn = new SqlConnection(ConnStr); conn.Open();
            var cmd = new SqlCommand("INSERT INTO History(StartTime,EndTime,Winner,PlayerCount) OUTPUT INSERTED.Id VALUES(@s,@e,@w,@c)", conn);
            cmd.Parameters.AddWithValue("@s", gameStartTime);
            cmd.Parameters.AddWithValue("@e", DateTime.Now);
            cmd.Parameters.AddWithValue("@w", nguoiThang);
            cmd.Parameters.AddWithValue("@c", players.Count);
            int id = (int)cmd.ExecuteScalar();
            foreach (var p in players)
            {
                var cp = new SqlCommand("INSERT INTO Matches(MatchId,PlayerName,Role,IsAlive) VALUES(@m,@n,@r,@a)", conn);
                cp.Parameters.AddWithValue("@m", id);
                cp.Parameters.AddWithValue("@n", p.Name);
                cp.Parameters.AddWithValue("@r", p.Role ?? "");
                cp.Parameters.AddWithValue("@a", p.IsAlive);
                cp.ExecuteNonQuery();
            }
        }
        catch (Exception ex) { Ghi("❌ Lưu kết quả thất bại: " + ex.Message, ConsoleColor.Red); }
    }

    static object logLock = new();
    static void Ghi(string m, ConsoleColor c = ConsoleColor.Gray) { lock (logLock) { Console.ForegroundColor = c; Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] {m}"); Console.ResetColor(); } }
    static void GhiBiMat(string m) { lock (logLock) { Console.ForegroundColor = ConsoleColor.DarkYellow; Console.WriteLine($"[🔒] {m}"); Console.ResetColor(); } }
    static string TenVaiTro(string? role) => role switch { "werewolf" => "Ma Sói", "seer" => "Tiên Tri", "guard" => "Bảo Vệ", "witch" => "Phù Thủy", "villager" => "Dân Làng", _ => role ?? "?" };
}

public class DoorResult { public bool Hit { get; set; } public bool HasOccupant { get; set; } }

public class WolfHuntState
{
    public string TargetId { get; set; } = "";
    public HashSet<int> OpenedDoors { get; set; } = new();
    public HashSet<int> AllOpenedDoors { get; set; } = new();
    public int DoorsOpenedByCurrentWolf { get; set; } = 0;
    public bool IsHit { get; set; } = false;
    public bool IsReady { get; set; } = false;
    public string? CurrentWolfId { get; set; } = null;
    public int TotalDoorsAllowed { get; set; } = 1;
    public int DoorsOpenedSoFar { get; set; } = 0;
    public Dictionary<int, DoorResult> DoorResults { get; set; } = new();
    public bool IsHuntFinished { get; set; } = false;
}