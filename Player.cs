using System.Net.WebSockets;

namespace MaSoiServer
{
    public class Player
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public WebSocket WS { get; set; }

        public string? Role { get; set; }
        public string Gender { get; set; } = "male";
        public bool IsHost { get; set; } = false;
        public bool IsAlive { get; set; } = false;
        public string RoomId { get; set; } = "";
        public int MaxPlayers { get; set; } = 7;

        // Skill tracking
        public bool HasUsedSkill { get; set; } = false;
        public bool WitchCanSave { get; set; } = true;
        public bool WitchCanKill { get; set; } = true;
        public bool GuardedSelfLastNight { get; set; } = false;

        // ── [MỚI] Phòng ẩn náu đêm nay (1–5, 0 = chưa chọn) ──
        public int ChosenRoom { get; set; } = 0;

        public Player(string id, string name, WebSocket ws)
        {
            Id = id;
            Name = name;
            WS = ws;
        }

        public void Reset()
        {
            Role = null;
            IsAlive = false;
            HasUsedSkill = false;
            WitchCanSave = true;
            WitchCanKill = true;
            GuardedSelfLastNight = false;
            ChosenRoom = 0;  // [MỚI]
        }

    }
}
