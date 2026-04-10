const state = {
  users: [],
  sortBy: "post",
  minPosts: 0,
  query: "",
};

const formatNumber = new Intl.NumberFormat("en-US");

const els = {
  statusPill: document.getElementById("statusPill"),
  kpiUsers: document.getElementById("kpiUsers"),
  kpiPosts: document.getElementById("kpiPosts"),
  kpiViews: document.getElementById("kpiViews"),
  kpiEngagement: document.getElementById("kpiEngagement"),
  heroLeader: document.getElementById("heroLeader"),
  podiumGrid: document.getElementById("podiumGrid"),
  leaderboardBody: document.getElementById("leaderboardBody"),
  searchInput: document.getElementById("searchInput"),
  sortSelect: document.getElementById("sortSelect"),
  minPostsSelect: document.getElementById("minPostsSelect"),
  jumpToLeaderboard: document.getElementById("jumpToLeaderboard"),
  leaderTemplate: document.getElementById("leaderTemplate"),
  leaderboardSection: document.getElementById("leaderboardSection"),
  fallbackLoader: document.getElementById("fallbackLoader"),
  usersFileInput: document.getElementById("usersFileInput"),
  loadLocalFiles: document.getElementById("loadLocalFiles"),
};

function compactNumber(value) {
  if (value >= 1_000_000) {
    return `${(value / 1_000_000).toFixed(1)}M`;
  }
  if (value >= 1_000) {
    return `${(value / 1_000).toFixed(1)}K`;
  }
  return `${value}`;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function normalizeUser(user) {
  const post = Number(user.post || 0);
  const like = Number(user.like || 0);
  const reply = Number(user.reply || 0);
  const repost = Number(user.repost || 0);
  const views = Number(user.views || 0);

  return {
    ...user,
    post,
    like,
    reply,
    repost,
    views,
    engagement: like + reply + repost,
  };
}

function compareUsers(a, b) {
  const metric = state.sortBy;
  const metricDiff = (b[metric] || 0) - (a[metric] || 0);
  if (metricDiff !== 0) {
    return metricDiff;
  }

  if (metric === "post") {
    const viewsDiff = b.views - a.views;
    if (viewsDiff !== 0) {
      return viewsDiff;
    }
  }

  const postsDiff = b.post - a.post;
  if (postsDiff !== 0) {
    return postsDiff;
  }

  const viewsDiff = b.views - a.views;
  if (viewsDiff !== 0) {
    return viewsDiff;
  }

  return a.tagname.localeCompare(b.tagname);
}

function getFilteredUsers() {
  const query = state.query.trim().toLowerCase();
  const rankedUsers = [...state.users]
    .sort(compareUsers)
    .map((user, index) => ({
      ...user,
      globalRank: index + 1,
    }));

  return rankedUsers
    .filter((user) => user.post >= state.minPosts)
    .filter((user) => {
      if (!query) {
        return true;
      }
      return `${user.name} ${user.tagname}`.toLowerCase().includes(query);
    });
}

function setStatus(text, type = "default") {
  els.statusPill.textContent = text;
  els.statusPill.classList.remove("is-ready", "is-error");
  if (type === "ready") {
    els.statusPill.classList.add("is-ready");
  }
  if (type === "error") {
    els.statusPill.classList.add("is-error");
  }
}

function renderHeroLeader() {
  const [leader] = [...state.users].sort((a, b) => b.post - a.post || b.views - a.views);
  if (!leader) {
    els.heroLeader.innerHTML = '<div class="empty-state">No leader data available.</div>';
    return;
  }

  const node = els.leaderTemplate.content.firstElementChild.cloneNode(true);
  node.href = `https://x.com/${leader.tagname}`;
  node.querySelector(".leader-mini-avatar").src = leader.pfp || "";
  node.querySelector(".leader-mini-avatar").alt = leader.name || leader.tagname;
  node.querySelector(".leader-mini-name").textContent = leader.name || leader.tagname;
  node.querySelector(".leader-mini-handle").textContent = `@${leader.tagname}`;
  node.querySelector(".leader-mini-value").textContent = formatNumber.format(leader.post);
  node.querySelector(".leader-mini-label").textContent = "posts";

  els.heroLeader.innerHTML = "";
  els.heroLeader.appendChild(node);
}

function renderKpis() {
  const users = state.users.length;
  const posts = state.users.reduce((sum, user) => sum + user.post, 0);
  const views = state.users.reduce((sum, user) => sum + user.views, 0);
  const engagement = state.users.reduce((sum, user) => sum + user.engagement, 0);

  els.kpiUsers.textContent = formatNumber.format(users);
  els.kpiPosts.textContent = compactNumber(posts);
  els.kpiViews.textContent = compactNumber(views);
  els.kpiEngagement.textContent = compactNumber(engagement);
}

function renderPodium() {
  const leaders = [...state.users]
    .sort((a, b) => b.post - a.post || b.views - a.views || b.engagement - a.engagement)
    .slice(0, 3);

  if (!leaders.length) {
    els.podiumGrid.innerHTML = '<div class="empty-state">No podium data available.</div>';
    return;
  }

  els.podiumGrid.innerHTML = leaders
    .map((user, index) => {
      const rank = index + 1;
      return `
        <article class="podium-card" data-rank="${rank}">
          <div class="podium-rank">0${rank}</div>
          <div class="podium-user">
            <img class="podium-avatar" src="${escapeHtml(user.pfp || "")}" alt="${escapeHtml(user.name || user.tagname)}">
            <div>
              <strong class="podium-name">${escapeHtml(user.name || user.tagname)}</strong>
              <a class="podium-handle" href="https://x.com/${escapeHtml(user.tagname)}" target="_blank" rel="noreferrer">@${escapeHtml(user.tagname)}</a>
            </div>
          </div>
          <div class="podium-stats">
            <div class="podium-stat">
              <span>Posts</span>
              <strong>${formatNumber.format(user.post)}</strong>
            </div>
            <div class="podium-stat">
              <span>Views</span>
              <strong>${compactNumber(user.views)}</strong>
            </div>
            <div class="podium-stat">
              <span>Likes</span>
              <strong>${formatNumber.format(user.like)}</strong>
            </div>
            <div class="podium-stat">
              <span>Engagement</span>
              <strong>${compactNumber(user.engagement)}</strong>
            </div>
          </div>
        </article>
      `;
    })
    .join("");
}

function renderLeaderboard() {
  const users = getFilteredUsers();
  if (!users.length) {
    els.leaderboardBody.innerHTML = `
      <tr>
        <td colspan="8">
          <div class="empty-state">No users match the current filters.</div>
        </td>
      </tr>
    `;
    return;
  }

  els.leaderboardBody.innerHTML = users
    .map((user) => {
      return `
        <tr>
          <td class="table-rank">${user.globalRank}</td>
          <td>
            <div class="table-user">
              <img class="table-avatar" src="${escapeHtml(user.pfp || "")}" alt="${escapeHtml(user.name || user.tagname)}">
              <div class="table-user-meta">
                <div class="table-name">${escapeHtml(user.name || user.tagname)}</div>
                <a class="table-handle" href="https://x.com/${escapeHtml(user.tagname)}" target="_blank" rel="noreferrer">@${escapeHtml(user.tagname)}</a>
              </div>
            </div>
          </td>
          <td>${formatNumber.format(user.post)}</td>
          <td>${formatNumber.format(user.views)}</td>
          <td>${formatNumber.format(user.like)}</td>
          <td>${formatNumber.format(user.reply)}</td>
          <td>${formatNumber.format(user.repost)}</td>
          <td>${formatNumber.format(user.engagement)}</td>
        </tr>
      `;
    })
    .join("");
}

function render() {
  renderKpis();
  renderHeroLeader();
  renderPodium();
  renderLeaderboard();
}

async function readJsonFile(file) {
  const text = await file.text();
  return JSON.parse(text);
}

function wireEvents() {
  els.searchInput.addEventListener("input", (event) => {
    state.query = event.target.value;
    renderLeaderboard();
  });

  els.sortSelect.addEventListener("change", (event) => {
    state.sortBy = event.target.value;
    renderLeaderboard();
  });

  els.minPostsSelect.addEventListener("change", (event) => {
    state.minPosts = Number(event.target.value || 0);
    renderLeaderboard();
  });

  els.jumpToLeaderboard.addEventListener("click", () => {
    els.leaderboardSection.scrollIntoView({ behavior: "smooth", block: "start" });
  });

  els.loadLocalFiles.addEventListener("click", async () => {
    const usersFile = els.usersFileInput.files?.[0];

    if (!usersFile) {
      setStatus("Choose users_summary.json", "error");
      return;
    }

    try {
      const usersRaw = await readJsonFile(usersFile);
      state.users = Array.isArray(usersRaw) ? usersRaw.map(normalizeUser) : [];
      render();
      setStatus("Loaded locally", "ready");
      els.fallbackLoader.classList.add("is-hidden");
    } catch (error) {
      console.error(error);
      setStatus("Invalid JSON", "error");
    }
  });
}

async function loadJson(path) {
  const response = await fetch(path);
  if (!response.ok) {
    throw new Error(`Failed to load ${path}: ${response.status}`);
  }
  return response.json();
}

async function bootstrap() {
  wireEvents();

  try {
    const usersRaw = await loadJson("./users_summary.json");
    state.users = Array.isArray(usersRaw) ? usersRaw.map(normalizeUser) : [];
    render();
    setStatus("Data loaded", "ready");
  } catch (error) {
    console.error(error);
    setStatus("Need local server", "error");
    els.fallbackLoader.classList.remove("is-hidden");

    els.heroLeader.innerHTML = `
      <div class="empty-state">
        The browser blocked local file access through <code>file://</code>.
        Open the folder through a simple static server or choose the JSON manually below.
      </div>
    `;
    els.podiumGrid.innerHTML = '<div class="empty-state">Could not load users_summary.json.</div>';
    els.leaderboardBody.innerHTML = `
      <tr>
        <td colspan="8">
          <div class="empty-state">
            Start any local static server from the project root or choose the JSON manually in the top panel.
          </div>
        </td>
      </tr>
    `;
  }
}

bootstrap();
