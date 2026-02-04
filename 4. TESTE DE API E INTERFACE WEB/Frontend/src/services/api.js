const BASE = "/api";

async function httpGet(path, params = {}) {
  const url = new URL(BASE + path, window.location.origin);
  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== null && String(v).trim() !== "") url.searchParams.set(k, v);
  });

  const res = await fetch(url.toString(), { method: "GET" });
  const text = await res.text();

  if (!res.ok) {
    let msg = `Erro ${res.status}`;
    try {
      const j = JSON.parse(text);
      msg = j?.detail || msg;
    } catch (_) {}
    throw new Error(msg);
  }

  return text ? JSON.parse(text) : null;
}

export function listOperadoras({ page = 1, limit = 10, q = "" } = {}) {
  return httpGet("/operadoras", { page, limit, q });
}

export function getOperadora(cnpj) {
  return httpGet(`/operadoras/${encodeURIComponent(cnpj)}`);
}

export function getDespesas(cnpj) {
  return httpGet(`/operadoras/${encodeURIComponent(cnpj)}/despesas`);
}

export function getEstatisticas() {
  return httpGet("/estatisticas");
}
