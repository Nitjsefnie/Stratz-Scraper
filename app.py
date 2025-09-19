#!/usr/bin/env python3
import sqlite3
from pathlib import Path
from flask import Flask, request, jsonify, Response

app = Flask(__name__)
DB_PATH = "dota.db"

# --- HERO mapping ---
HEROES_JSON = [
    {"id":1,"localized_name":"Anti-Mage"},
    {"id":2,"localized_name":"Axe"},
    {"id":3,"localized_name":"Bane"},
    {"id":4,"localized_name":"Bloodseeker"},
    {"id":5,"localized_name":"Crystal Maiden"},
    {"id":6,"localized_name":"Drow Ranger"},
    {"id":7,"localized_name":"Earthshaker"},
    {"id":8,"localized_name":"Juggernaut"},
    {"id":9,"localized_name":"Mirana"},
    {"id":10,"localized_name":"Morphling"},
    {"id":11,"localized_name":"Shadow Fiend"},
    {"id":12,"localized_name":"Phantom Lancer"},
    {"id":13,"localized_name":"Puck"},
    {"id":14,"localized_name":"Pudge"},
    {"id":15,"localized_name":"Razor"},
    {"id":16,"localized_name":"Sand King"},
    {"id":17,"localized_name":"Storm Spirit"},
    {"id":18,"localized_name":"Sven"},
    {"id":19,"localized_name":"Tiny"},
    {"id":20,"localized_name":"Vengeful Spirit"},
    {"id":21,"localized_name":"Windranger"},
    {"id":22,"localized_name":"Zeus"},
    {"id":23,"localized_name":"Kunkka"},
    {"id":25,"localized_name":"Lina"},
    {"id":26,"localized_name":"Lion"},
    {"id":27,"localized_name":"Shadow Shaman"},
    {"id":28,"localized_name":"Slardar"},
    {"id":29,"localized_name":"Tidehunter"},
    {"id":30,"localized_name":"Witch Doctor"},
    {"id":31,"localized_name":"Lich"},
    {"id":32,"localized_name":"Riki"},
    {"id":33,"localized_name":"Enigma"},
    {"id":34,"localized_name":"Tinker"},
    {"id":35,"localized_name":"Sniper"},
    {"id":36,"localized_name":"Necrophos"},
    {"id":37,"localized_name":"Warlock"},
    {"id":38,"localized_name":"Beastmaster"},
    {"id":39,"localized_name":"Queen of Pain"},
    {"id":40,"localized_name":"Venomancer"},
    {"id":41,"localized_name":"Faceless Void"},
    {"id":42,"localized_name":"Wraith King"},
    {"id":43,"localized_name":"Death Prophet"},
    {"id":44,"localized_name":"Phantom Assassin"},
    {"id":45,"localized_name":"Pugna"},
    {"id":46,"localized_name":"Templar Assassin"},
    {"id":47,"localized_name":"Viper"},
    {"id":48,"localized_name":"Luna"},
    {"id":49,"localized_name":"Dragon Knight"},
    {"id":50,"localized_name":"Dazzle"},
    {"id":51,"localized_name":"Clockwerk"},
    {"id":52,"localized_name":"Leshrac"},
    {"id":53,"localized_name":"Nature's Prophet"},
    {"id":54,"localized_name":"Lifestealer"},
    {"id":55,"localized_name":"Dark Seer"},
    {"id":56,"localized_name":"Clinkz"},
    {"id":57,"localized_name":"Omniknight"},
    {"id":58,"localized_name":"Enchantress"},
    {"id":59,"localized_name":"Huskar"},
    {"id":60,"localized_name":"Night Stalker"},
    {"id":61,"localized_name":"Broodmother"},
    {"id":62,"localized_name":"Bounty Hunter"},
    {"id":63,"localized_name":"Weaver"},
    {"id":64,"localized_name":"Jakiro"},
    {"id":65,"localized_name":"Batrider"},
    {"id":66,"localized_name":"Chen"},
    {"id":67,"localized_name":"Spectre"},
    {"id":68,"localized_name":"Ancient Apparition"},
    {"id":69,"localized_name":"Doom"},
    {"id":70,"localized_name":"Ursa"},
    {"id":71,"localized_name":"Spirit Breaker"},
    {"id":72,"localized_name":"Gyrocopter"},
    {"id":73,"localized_name":"Alchemist"},
    {"id":74,"localized_name":"Invoker"},
    {"id":75,"localized_name":"Silencer"},
    {"id":76,"localized_name":"Outworld Destroyer"},
    {"id":77,"localized_name":"Lycan"},
    {"id":78,"localized_name":"Brewmaster"},
    {"id":79,"localized_name":"Shadow Demon"},
    {"id":80,"localized_name":"Lone Druid"},
    {"id":81,"localized_name":"Chaos Knight"},
    {"id":82,"localized_name":"Meepo"},
    {"id":83,"localized_name":"Treant Protector"},
    {"id":84,"localized_name":"Ogre Magi"},
    {"id":85,"localized_name":"Undying"},
    {"id":86,"localized_name":"Rubick"},
    {"id":87,"localized_name":"Disruptor"},
    {"id":88,"localized_name":"Nyx Assassin"},
    {"id":89,"localized_name":"Naga Siren"},
    {"id":90,"localized_name":"Keeper of the Light"},
    {"id":91,"localized_name":"Io"},
    {"id":92,"localized_name":"Visage"},
    {"id":93,"localized_name":"Slark"},
    {"id":94,"localized_name":"Medusa"},
    {"id":95,"localized_name":"Troll Warlord"},
    {"id":96,"localized_name":"Centaur Warrunner"},
    {"id":97,"localized_name":"Magnus"},
    {"id":98,"localized_name":"Timbersaw"},
    {"id":99,"localized_name":"Bristleback"},
    {"id":100,"localized_name":"Tusk"},
    {"id":101,"localized_name":"Skywrath Mage"},
    {"id":102,"localized_name":"Abaddon"},
    {"id":103,"localized_name":"Elder Titan"},
    {"id":104,"localized_name":"Legion Commander"},
    {"id":105,"localized_name":"Techies"},
    {"id":106,"localized_name":"Ember Spirit"},
    {"id":107,"localized_name":"Earth Spirit"},
    {"id":108,"localized_name":"Underlord"},
    {"id":109,"localized_name":"Terrorblade"},
    {"id":110,"localized_name":"Phoenix"},
    {"id":111,"localized_name":"Oracle"},
    {"id":112,"localized_name":"Winter Wyvern"},
    {"id":113,"localized_name":"Arc Warden"},
    {"id":114,"localized_name":"Monkey King"},
    {"id":119,"localized_name":"Dark Willow"},
    {"id":120,"localized_name":"Pangolier"},
    {"id":121,"localized_name":"Grimstroke"},
    {"id":123,"localized_name":"Hoodwink"},
    {"id":126,"localized_name":"Void Spirit"},
    {"id":128,"localized_name":"Snapfire"},
    {"id":129,"localized_name":"Mars"},
    {"id":131,"localized_name":"Ringmaster"},
    {"id":135,"localized_name":"Dawnbreaker"},
    {"id":136,"localized_name":"Marci"},
    {"id":137,"localized_name":"Primal Beast"},
    {"id":138,"localized_name":"Muerta"},
    {"id":145,"localized_name":"Kez"}
]
HEROES = {h["id"]: h["localized_name"] for h in HEROES_JSON}

# --- DB ---
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_schema():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = db()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS players (
        id INTEGER PRIMARY KEY,
        assigned_to TEXT,
        assigned_at INTEGER,
        done INTEGER DEFAULT 0
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS hero_stats (
        player_id INTEGER,
        hero_id INTEGER,
        hero_name TEXT,
        matches INTEGER,
        wins INTEGER,
        PRIMARY KEY (player_id, hero_id)
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS best (
        hero_id INTEGER PRIMARY KEY,
        hero_name TEXT,
        player_id INTEGER,
        matches INTEGER,
        wins INTEGER
    )
    """)
    conn.commit()
    conn.close()

ensure_schema()

INDEX_HTML = """<!doctype html>
<meta charset="utf-8">
<title>Dota Distributed Scraper (Stratz)</title>
<style>
body { font-family: sans-serif; max-width: 900px; margin: 40px auto; }
button { padding: 8px 12px; margin: 4px; }
pre { background: #f6f8fa; padding: 12px; border-radius: 6px; overflow:auto; }
table { border-collapse: collapse; width: 100%; margin-top: 20px; }
th, td { border: 1px solid #ccc; padding: 6px 10px; text-align: left; }
th { background: #eee; }
</style>
<h1>Dota Distributed Scraper (Stratz)</h1>

<p>Enter your Stratz token (stored in a cookie, used only in your browser):</p>
<input id="token" type="text" size="60" placeholder="Paste Stratz API token here">
<button id="saveToken">Save</button>

<p>
  <button id="begin">Begin</button>
  <button id="stop">Stop</button>
  <button id="progress">Progress</button>
  <button id="best">Best</button>
</p>

<p>
  Seed IDs: 
  <input id="seedStart" type="number" placeholder="start id">
  <input id="seedEnd" type="number" placeholder="end id">
  <button id="seedBtn">Seed</button>
</p>

<pre id="log"></pre>
<div id="bestTable"></div>

<script>
function setCookie(name, value, days) {
  let d = new Date(); d.setTime(d.getTime() + (days*86400000));
  document.cookie = name+"="+value+"; expires="+d.toUTCString()+"; path=/";
}
function getCookie(name) {
  let cname = name+"=";
  let ca = document.cookie.split(';');
  for(let c of ca){c=c.trim(); if(c.indexOf(cname)==0) return c.substring(cname.length);}
  return "";
}
document.getElementById("saveToken").onclick = () => {
  const token = document.getElementById("token").value.trim();
  setCookie("stratz_token", token, 30);
  alert("Token saved.");
};
const log = (m) => { const el = document.getElementById('log'); el.textContent += m+"\\n"; el.scrollTop=el.scrollHeight; };

async function getTask() {
  const r = await fetch("/task", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({client:"browser"})});
  const j = await r.json(); return j.task;
}

async function fetchPlayerHeroes(pid) {
  const token = getCookie("stratz_token");
  if(!token) throw new Error("No Stratz token set");
  const query = `
    query HeroPerf($id: Long!) {
      player(steamAccountId: $id) {
        steamAccountId
        heroesPerformance(
          request: { take: 999999, gameModeIds: [1, 22] }
          take: 200
        ) {
          heroId
          matchCount
          winCount
        }
      }
    }
  `;
  const resp = await fetch("https://api.stratz.com/graphql", {
    method: "POST",
    headers: {"Authorization":"Bearer "+token,"Content-Type":"application/json"},
    body: JSON.stringify({ query, variables: { id: pid } })
  });
  if(!resp.ok) throw new Error("HTTP "+resp.status);
  const data = await resp.json();
  if(!data.data || !data.data.player) return [];
  return data.data.player.heroesPerformance.map(h => ({
    hero_id: h.heroId, games: h.matchCount, wins: h.winCount
  }));
}

async function submitBulk(playerId, heroes) {
  await fetch("/submit", {method:"POST", headers:{"Content-Type":"application/json"},
    body: JSON.stringify({ player_id: playerId, heroes })});
}

let running = false;
async function workLoop() {
  while(running) {
    let task=null;
    try {
      task = await getTask();
      if(!task){log("No tasks left."); break;}
      log("Task "+task);
      const heroes = await fetchPlayerHeroes(task);
      await submitBulk(task, heroes);
      log("Submitted "+heroes.length+" heroes for "+task);
      await new Promise(res=>setTimeout(res,500));
    } catch(e) {
      log("Error "+task+": "+e);
      await new Promise(res=>setTimeout(res,2000));
    }
  }
}
document.getElementById("begin").onclick=()=>{if(!running){running=true;workLoop();log("Started.");}};
document.getElementById("stop").onclick=()=>{running=false;log("Stopped.");};
document.getElementById("progress").onclick=async()=>{const r=await fetch("/progress");const j=await r.json();log("Progress "+j.done+"/"+j.total);};

document.getElementById("best").onclick = async () => {
  const r = await fetch("/best");
  const j = await r.json();
  if (!j.length) {
    document.getElementById("bestTable").innerHTML = "<p>No data yet.</p>";
    return;
  }
  let html = "<table><tr><th>Hero</th><th>Hero ID</th><th>Player ID</th><th>Matches</th><th>Wins</th></tr>";
  for (const row of j) {
    html += `<tr><td>${row.hero_name}</td><td>${row.hero_id}</td><td>${row.player_id}</td><td>${row.matches}</td><td>${row.wins}</td></tr>`;
  }
  html += "</table>";
  document.getElementById("bestTable").innerHTML = html;
};
document.getElementById("seedBtn").onclick = async () => {
  const start = parseInt(document.getElementById("seedStart").value);
  const end = parseInt(document.getElementById("seedEnd").value);
  if (!start || !end || end < start) {
    alert("Enter valid start/end IDs");
    return;
  }
  const r = await fetch(`/seed?start=${start}&end=${end}`);
  const j = await r.json();
  log(`Seeded IDs ${j.seeded[0]} - ${j.seeded[1]}`);
};
</script>
"""

@app.get("/")
def index():
    return Response(INDEX_HTML, mimetype="text/html")

# --- API ---
@app.post("/task")
def task():
    conn = db()
    cur = conn.execute("""
        UPDATE players
        SET assigned_to='browser', assigned_at=strftime('%s','now')
        WHERE id = (SELECT id FROM players WHERE done=0 AND assigned_to IS NULL LIMIT 1)
        RETURNING id
    """)
    row = cur.fetchone()
    conn.commit(); conn.close()
    return jsonify({"task": row["id"] if row else None})

@app.post("/submit")
def submit():
    data = request.get_json(force=True)
    pid = int(data["player_id"])
    heroes = data.get("heroes", [])
    conn = db(); cur = conn.cursor()
    for h in heroes:
        hid, matches, wins = int(h["hero_id"]), int(h["games"]), int(h.get("wins", 0))
        name = HEROES.get(hid)
        if not name: continue
        # update hero_stats
        cur.execute("""
        INSERT INTO hero_stats (player_id, hero_id, hero_name, matches, wins)
        VALUES (?,?,?,?,?)
        ON CONFLICT(player_id, hero_id) DO UPDATE SET
          matches=excluded.matches, wins=excluded.wins
        """,(pid,hid,name,matches,wins))
        # update best
        cur.execute("""
        INSERT INTO best (hero_id, hero_name, player_id, matches, wins)
        VALUES (?,?,?,?,?)
        ON CONFLICT(hero_id) DO UPDATE SET
          matches=excluded.matches,
          wins=excluded.wins,
          player_id=excluded.player_id
        WHERE excluded.matches > best.matches
        """,(hid,name,pid,matches,wins))
    cur.execute("UPDATE players SET done=1 WHERE id=?",(pid,))
    conn.commit(); conn.close()
    return jsonify({"status":"ok"})

@app.get("/progress")
def progress():
    conn=db()
    total=conn.execute("SELECT COUNT(*) AS c FROM players").fetchone()["c"]
    done=conn.execute("SELECT COUNT(*) AS c FROM players WHERE done=1").fetchone()["c"]
    conn.close(); return jsonify({"total":total,"done":done})

@app.get("/seed")
def seed():
    try:
        start=int(request.args.get("start")); end=int(request.args.get("end"))
    except: return Response("Use /seed?start=1&end=100",status=400)
    conn=db(); cur=conn.cursor(); cur.execute("BEGIN")
    for pid in range(start,end+1):
        cur.execute("INSERT OR IGNORE INTO players (id) VALUES (?)",(pid,))
    conn.commit(); conn.close()
    return jsonify({"seeded":[start,end]})

@app.get("/best")
def best():
    conn = db()
    rows = conn.execute("SELECT * FROM best ORDER BY matches DESC").fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

if __name__=="__main__":
    app.run(host="0.0.0.0",port=8000,debug=True)
