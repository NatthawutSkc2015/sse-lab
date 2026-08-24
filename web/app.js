(() => {
  const BACKEND_URL = location.hostname === "localhost" || location.hostname === "127.0.0.1"
    ? "http://localhost:9000"
    : "https://sse-lab-chat.fly.dev";
  const USERNAME_KEY = "sse-chat-username";
  const MAX_MESSAGES = 500;

  const joinScreen = document.getElementById("join-screen");
  const chatScreen = document.getElementById("chat-screen");
  const userInput = document.getElementById("user-input");
  const joinBtn = document.getElementById("join-btn");
  const statusEl = document.getElementById("conn-status");
  const countEl = document.getElementById("client-count");
  const messagesEl = document.getElementById("messages");
  const composer = document.getElementById("composer");
  const msgInput = document.getElementById("msg-input");

  let username = "";
  let es = null;

  function setStatus(kind, text) {
    statusEl.textContent = text;
    statusEl.className = "pill " + kind;
  }

  function scrollToBottom() {
    messagesEl.scrollTop = messagesEl.scrollHeight;
  }

  function trimHistory() {
    while (messagesEl.children.length > MAX_MESSAGES) {
      messagesEl.removeChild(messagesEl.firstChild);
    }
  }

  // Deterministic per-user color so the same name always gets the same
  // color, making it easy to tell people apart in a busy chat.
  function colorForUser(name) {
    let hash = 0;
    for (let i = 0; i < name.length; i++) {
      hash = (hash * 31 + name.charCodeAt(i)) >>> 0;
    }
    const hue = hash % 360;
    return "hsl(" + hue + " 75% 65%)";
  }

  function userNameSpan(user) {
    const span = document.createElement("span");
    span.className = "user-name";
    span.style.color = colorForUser(user);
    span.textContent = user; // textContent only: never trust chat content as HTML
    return span;
  }

  function addSystemMessage(user, suffix) {
    const li = document.createElement("li");
    li.className = "msg system";
    const bubble = document.createElement("div");
    bubble.className = "bubble";
    bubble.appendChild(userNameSpan(user));
    bubble.appendChild(document.createTextNode(" " + suffix));
    li.appendChild(bubble);
    messagesEl.appendChild(li);
    trimHistory();
    scrollToBottom();
  }

  function addChatMessage(user, message, time, mine) {
    const li = document.createElement("li");
    li.className = "msg " + (mine ? "mine" : "other");

    const meta = document.createElement("div");
    meta.className = "meta";
    meta.appendChild(userNameSpan(user));
    meta.appendChild(document.createTextNode(" · " + new Date(time).toLocaleTimeString()));

    const bubble = document.createElement("div");
    bubble.className = "bubble";
    bubble.textContent = message; // textContent only: never trust chat content as HTML

    li.appendChild(meta);
    li.appendChild(bubble);
    messagesEl.appendChild(li);
    trimHistory();
    scrollToBottom();
  }

  function connect() {
    const url = BACKEND_URL + "/events?user=" + encodeURIComponent(username);
    es = new EventSource(url);

    es.onopen = () => setStatus("ok", "เชื่อมต่อแล้ว");
    es.onerror = () => setStatus("err", "หลุดการเชื่อมต่อ กำลังลองใหม่…");

    es.onmessage = (e) => {
      let data;
      try {
        data = JSON.parse(e.data);
      } catch {
        return;
      }
      if (typeof data.clients === "number") {
        countEl.textContent = data.clients;
      }
      switch (data.type) {
        case "join":
          addSystemMessage(data.user, "เข้าร่วมห้องแชท");
          break;
        case "leave":
          addSystemMessage(data.user, "ออกจากห้องแชท");
          break;
        case "chat":
          addChatMessage(data.user, data.message, data.time, data.user === username);
          break;
      }
    };
  }

  async function refreshStats() {
    try {
      const res = await fetch(BACKEND_URL + "/stats");
      const data = await res.json();
      countEl.textContent = data.clients;
    } catch {
      // ignore transient failures; the SSE stream is the source of truth anyway
    }
  }

  async function sendMessage(text) {
    try {
      await fetch(BACKEND_URL + "/broadcast", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ user: username, message: text }),
      });
    } catch (err) {
      addSystemMessage(username, "ส่งข้อความไม่สำเร็จ: " + err.message);
    }
  }

  function join(rawName) {
    username = rawName.trim().slice(0, 24) || "Anonymous";
    localStorage.setItem(USERNAME_KEY, username);
    joinScreen.classList.add("hidden");
    chatScreen.classList.remove("hidden");
    connect();
    refreshStats();
    msgInput.focus();
  }

  joinBtn.addEventListener("click", () => {
    if (userInput.value.trim()) join(userInput.value);
  });
  userInput.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && userInput.value.trim()) join(userInput.value);
  });

  composer.addEventListener("submit", (e) => {
    e.preventDefault();
    const text = msgInput.value.trim();
    if (!text) return;
    msgInput.value = "";
    sendMessage(text);
  });

  setInterval(refreshStats, 10000);

  const savedName = localStorage.getItem(USERNAME_KEY);
  if (savedName) userInput.value = savedName;
})();
