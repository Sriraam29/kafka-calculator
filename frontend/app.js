const API_BASE = "http://localhost:8080";
const TOKEN_KEY = "calculator_access_token";
const ALIAS_KEY = "calculator_alias_name";

const state = {
  token: sessionStorage.getItem(TOKEN_KEY) || "",
  alias: sessionStorage.getItem(ALIAS_KEY) || "",
  operation: "add",
};



const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => [...document.querySelectorAll(selector)];

const operationMeta = {
  add: { symbol: "+", label: "Add" },
  subtract: { symbol: "−", label: "Subtract" },
  multiply: { symbol: "×", label: "Multiply" },
  divide: { symbol: "÷", label: "Divide" },
};

function showToast(message, type = "success") {
  const toast = document.createElement("div");
  toast.className = `toast ${type}`;
  toast.textContent = message;
  $("#toastRoot").appendChild(toast);
  setTimeout(() => toast.remove(), 3200);
}

function setButtonLoading(button, loading) {
  if (!button) return;
  button.disabled = loading;
  const spinner = button.querySelector(".btn-spinner");
  const text = button.querySelector("span:not(.btn-spinner)");
  if (spinner) spinner.classList.toggle("hidden", !loading);
  if (text) text.style.opacity = loading ? "0.72" : "1";
}

function setApiStatus(online, label) {
  const pill = $("#apiStatus");
  pill.classList.remove("status-checking", "status-online", "status-offline");
  pill.classList.add(online ? "status-online" : "status-offline");
  pill.querySelector("span:last-child").textContent = label;
}

async function apiRequest(path, options = {}) {
  const headers = new Headers(options.headers || {});
  if (options.body && !headers.has("Content-Type")) headers.set("Content-Type", "application/json");
  if (state.token) headers.set("Authorization", `Bearer ${state.token}`);

  const response = await fetch(`${API_BASE}${path}`, { ...options, headers });
  let payload = null;
  try { payload = await response.json(); } catch { /* non-JSON response */ }

  if (!response.ok || !payload?.success) {
    const code = payload?.code || `HTTP_${response.status}`;
    const error = new Error(code);
    error.code = code;
    error.status = response.status;
    throw error;
  }
  return payload;
}

async function checkApi() {
  try {
    const response = await fetch(`${API_BASE}/health`, { cache: "no-store" });
    const payload = await response.json();
    if (response.ok && payload?.success) setApiStatus(true, "API online");
    else setApiStatus(false, "API unavailable");
  } catch {
    setApiStatus(false, "API unavailable");
  }
}

function setView(authenticated) {
  $("#authView").classList.toggle("hidden", authenticated);
  $("#workspaceView").classList.toggle("hidden", !authenticated);
  $("#logoutBtn").classList.toggle("hidden", !authenticated);
  if (authenticated) {
    $("#aliasName").textContent = state.alias || "user";
    setTodayLabel();
    loadHistory();
  }
}

function setTodayLabel() {
  const now = new Date();
  $("#todayLabel").textContent = new Intl.DateTimeFormat(undefined, {
    weekday: "short", month: "short", day: "numeric"
  }).format(now);
}

function switchAuthTab(tab) {
  $$(".auth-tab").forEach(btn => btn.classList.toggle("active", btn.dataset.authTab === tab));
  $("#loginPanel").classList.toggle("hidden", tab !== "login");
  $("#registerPanel").classList.toggle("hidden", tab !== "register");
}

function saveSession(alias, token) {
  state.alias = alias || "";
  state.token = token || "";
  if (state.token) sessionStorage.setItem(TOKEN_KEY, state.token);
  if (state.alias) sessionStorage.setItem(ALIAS_KEY, state.alias);
}

function clearSession() {
  state.token = "";
  state.alias = "";
  sessionStorage.removeItem(TOKEN_KEY);
  sessionStorage.removeItem(ALIAS_KEY);
}

function formatNumber(value) {
  if (!Number.isFinite(value)) return String(value);
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: 10 }).format(value);
}

function operationDisplay(operation, a, b, result) {
  const meta = operationMeta[operation] || operationMeta.add;
  $("#expressionLabel").textContent = `${formatNumber(a)} ${meta.symbol} ${formatNumber(b)}`;
  $("#resultDisplay").textContent = formatNumber(result);
}

function resetCalculator() {
  $("#calcA").value = "";
  $("#calcB").value = "";
  $("#expressionLabel").textContent = "Ready for a calculation";
  $("#resultDisplay").textContent = "0";
  $("#calcA").focus();
}

function mapError(code) {
  const messages = {
    INVALID_ACCESS_TOKEN: "Your session has expired. Please sign in again.",
    DIVISION_BY_ZERO: "Division by zero is not allowed.",
    INVALID_OPERATION: "That operation is not supported.",
    INVALID_REQUEST: "Please check the values you entered.",
    AUTHENTICATION_FAILED: "Username or password is incorrect.",
    USERNAME_ALREADY_EXISTS: "That username is already in use.",
  };
  return messages[code] || code.replaceAll("_", " ").toLowerCase().replace(/^./, c => c.toUpperCase());
}

async function handleLogin(event) {
  event.preventDefault();
  const button = event.currentTarget.querySelector("button[type=submit]");
  const username = $("#loginUsername").value.trim();
  const password = $("#loginPassword").value;
  if (!username || !password) return showToast("Enter your username and password.", "error");

  setButtonLoading(button, true);
  try {
    const payload = await apiRequest("/api/auth/login", {
      method: "POST",
      body: JSON.stringify({ username, password }),
    });
    saveSession(payload.data.alias_name, payload.data.access_token);
    showToast("Signed in successfully.");
    setView(true);
  } catch (error) {
    showToast(mapError(error.code || "LOGIN_FAILED"), "error");
  } finally {
    setButtonLoading(button, false);
  }
}

async function handleRegister(event) {
  event.preventDefault();
  const button = event.currentTarget.querySelector("button[type=submit]");
  const username = $("#registerUsername").value.trim();
  const password = $("#registerPassword").value;
  const confirm = $("#registerConfirm").value;
  if (!username || !password || !confirm) return showToast("Complete all fields.", "error");
  if (password !== confirm) return showToast("Passwords do not match.", "error");
  if (password.length < 8) return showToast("Use at least 8 characters for the password.", "error");

  setButtonLoading(button, true);
  try {
    const payload = await apiRequest("/api/auth/register", {
      method: "POST",
      body: JSON.stringify({ username, password }),
    });
    $("#loginUsername").value = username;
    $("#loginPassword").value = "";
    $("#registerForm").reset();
    switchAuthTab("login");
    showToast(`Account created. Your alias is ${payload.data.alias_name}.`);
  } catch (error) {
    showToast(mapError(error.code || "REGISTRATION_FAILED"), "error");
  } finally {
    setButtonLoading(button, false);
  }
}

async function handleCalculate(event) {
  event.preventDefault();
  const button = event.currentTarget.querySelector(".calculate-btn");
  const a = Number($("#calcA").value);
  const b = Number($("#calcB").value);
  if (!Number.isFinite(a) || !Number.isFinite(b)) return showToast("Enter both numbers.", "error");

  setButtonLoading(button, true);
  try {
    const payload = await apiRequest("/api/calculator/calculate", {
      method: "POST",
      body: JSON.stringify({ operation: state.operation, a, b }),
    });
    const data = payload.data;
    operationDisplay(data.operation, data.a, data.b, data.result);
    showToast("Calculation complete.");
    await loadHistory();
  } catch (error) {
    if (error.code === "INVALID_ACCESS_TOKEN") logout(false);
    showToast(mapError(error.code || "CALCULATION_FAILED"), "error");
  } finally {
    setButtonLoading(button, false);
  }
}

function operationButtonHandler(button) {
  state.operation = button.dataset.operation;
  $$(".operation-btn").forEach(btn => btn.classList.toggle("active", btn === button));
  const a = $("#calcA").value;
  const b = $("#calcB").value;
  if (a !== "" && b !== "") {
    const meta = operationMeta[state.operation];
    $("#expressionLabel").textContent = `${formatNumber(Number(a))} ${meta.symbol} ${formatNumber(Number(b))}`;
  }
}

function renderHistory(items) {
  const root = $("#historyList");
  if (!items?.length) {
    root.innerHTML = `<div class="empty-state"><div class="empty-icon">∑</div><strong>No calculations yet</strong><span>Run your first calculation to see it here.</span></div>`;
    return;
  }

  root.innerHTML = items.map(item => {
    const meta = operationMeta[item.operation] || { symbol: "?", label: item.operation };
    const when = item.created_at ? new Date(item.created_at).toLocaleString(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" }) : "";
    return `<article class="history-item">
      <div class="history-op">${meta.symbol}</div>
      <div class="history-main"><strong>${escapeHtml(formatNumber(item.a))} ${meta.symbol} ${escapeHtml(formatNumber(item.b))}</strong><small>${escapeHtml(meta.label)} · ${escapeHtml(when)}</small></div>
      <div class="history-result">${escapeHtml(formatNumber(item.result))}</div>
    </article>`;
  }).join("");
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, char => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#039;" }[char]));
}

async function loadHistory() {
  if (!state.token) return;
  try {
    const payload = await apiRequest("/api/calculator/history", { method: "GET" });
    renderHistory(payload.data);
  } catch (error) {
    if (error.code === "INVALID_ACCESS_TOKEN") logout(false);
    else showToast(mapError(error.code || "HISTORY_FAILED"), "error");
  }
}

async function restoreSession() {
  if (!state.token) return setView(false);
  try {
    const payload = await apiRequest("/api/auth/me", { method: "GET" });
    if (payload.data?.alias_name) state.alias = payload.data.alias_name;
    saveSession(state.alias, state.token);
    setView(true);
  } catch {
    clearSession();
    setView(false);
  }
}

function logout(showMessage = true) {
  clearSession();
  setView(false);
  if (showMessage) showToast("You have been signed out.");
  switchAuthTab("login");
}

function bindEvents() {
  $$(".auth-tab").forEach(btn => btn.addEventListener("click", () => switchAuthTab(btn.dataset.authTab)));
  $$(".toggle-password").forEach(btn => btn.addEventListener("click", () => {
    const input = document.getElementById(btn.dataset.target);
    const visible = input.type === "text";
    input.type = visible ? "password" : "text";
    btn.textContent = visible ? "Show" : "Hide";
  }));
  $$(".operation-btn").forEach(btn => btn.addEventListener("click", () => operationButtonHandler(btn)));
  $("#loginForm").addEventListener("submit", handleLogin);
  $("#registerForm").addEventListener("submit", handleRegister);
  $("#calcForm").addEventListener("submit", handleCalculate);
  $("#clearCalcBtn").addEventListener("click", resetCalculator);
  $("#refreshHistoryBtn").addEventListener("click", loadHistory);
  $("#deleteHistoryBtn").addEventListener("click", clearHistory);
  $("#logoutBtn").addEventListener("click", () => logout(true));
}
async function clearHistory() {
    if (!state.token) {
        showToast("Please login first.", "error");
        return;
    }

    const confirmed = window.confirm(
        "Are you sure you want to delete your calculation history?"
    );

    if (!confirmed) {
        return;
    }

    try {
        const payload = await apiRequest(
            "/api/calculator/history",
            {
                method: "DELETE"
            }
        );

        renderHistory([]);

        showToast("History cleared.", "success");

    } catch (error) {
        console.error("Clear history failed:", error);

        if (error.code === "INVALID_ACCESS_TOKEN") {
            logout(false);
            return;
        }

        showToast(
            mapError(error.code || "HISTORY_DELETE_FAILED"),
            "error"
        );
    }
}


bindEvents();
setTodayLabel();
checkApi();
restoreSession();
