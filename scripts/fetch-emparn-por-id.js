/**
 * scripts/fetch-emparn-por-id.js
 * Coleta o boletim diário da EMPARN pelo ID sequencial.
 * - Base: 29/10/2025 => ID 10965
 * - Calcula ID do dia em America/Fortaleza, tenta [id, id-1, id+1]
 * - Tenta acesso direto (HTTPS)
 * - Fallback: r.jina.ai com /https:// e /http://
 * - Extrai as 4 abas e salva data/latest.json e data/latest.csv
 *
 * Requisitos: Node 20+, cheerio, papaparse
 *   npm i cheerio papaparse
 */

const fs = require("fs");
const cheerio = require("cheerio");
const Papa = require("papaparse");

// ---------- Config ----------
const TZ = "America/Fortaleza";
const BASE_DATE = "2025-10-29"; // 29/10/2025
const BASE_ID = 10965;
const FORCED_DATE = process.env.FORCED_DATE || null;

const ORIGIN = "https://meteorologia.emparn.rn.gov.br";
const UA_BROWSER =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36";
const COMMON_HEADERS = {
  "User-Agent": UA_BROWSER,
  "Accept":
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
  "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
  "Cache-Control": "no-cache",
  Pragma: "no-cache",
  Referer: ORIGIN + "/",
};

// ---------- Utils ----------
function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}
function saveText(path, text) {
  fs.writeFileSync(path, text, "utf8");
}
function parseNumberBR(s) {
  if (s == null || s === "") return null;
  const n = parseFloat(String(s).replace(/\./g, "").replace(",", "."));
  return Number.isFinite(n) ? n : null;
}
function formatDateLocal(dateLike) {
  const d = dateLike ? new Date(dateLike) : new Date();
  const fmt = new Intl.DateTimeFormat("pt-BR", {
    timeZone: TZ,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  return fmt.format(d); // DD/MM/AAAA
}

function idDoDia(dateLike = undefined) {
  let alvo;
  if (FORCED_DATE) {
    if (/\d{2}\/\d{2}\/\d{4}/.test(FORCED_DATE)) {
      const [dd, mm, yyyy] = FORCED_DATE.split("/").map(Number);
      alvo = new Date(Date.UTC(yyyy, mm - 1, dd));
    } else if (/\d{4}-\d{2}-\d{2}/.test(FORCED_DATE)) {
      const [yyyy, mm, dd] = FORCED_DATE.split("-").map(Number);
      alvo = new Date(Date.UTC(yyyy, mm - 1, dd));
    } else {
      alvo = new Date(dateLike || Date.now());
    }
  } else {
    const now = dateLike ? new Date(dateLike) : new Date();
    const [dd, mm, yyyy] = formatDateLocal(now).split("/").map(Number);
    alvo = new Date(Date.UTC(yyyy, mm - 1, dd));
  }
  const [bY, bM, bD] = BASE_DATE.split("-").map(Number);
  const baseMid = new Date(Date.UTC(bY, bM - 1, bD));
  const dias = Math.round((alvo - baseMid) / (1000 * 60 * 60 * 24));
  return BASE_ID + dias;
}

// ---------- Fetchers ----------
async function fetchHtmlDireto(id) {
  const url = `${ORIGIN}/boletim/diario/${id}`;
  const res = await fetch(url, {
    headers: COMMON_HEADERS,
    redirect: "follow",
  });
  if (!res.ok) throw new Error(`Direto ${res.status} em ${url}`);
  return res.text();
}

async function fetchHtmlProxy(id) {
  // Tenta primeiro a forma HTTPS -> HTTPS
  const pHttps = `https://r.jina.ai/https://meteorologia.emparn.rn.gov.br/boletim/diario/${id}`;
  const r1 = await fetch(pHttps, { headers: COMMON_HEADERS, redirect: "follow" });
  if (r1.ok) return r1.text();
  // Se não deu, tenta HTTPS -> HTTP (alguns sites funcionam assim no proxy)
  const pHttp = `https://r.jina.ai/http://meteorologia.emparn.rn.gov.br/boletim/diario/${id}`;
  const r2 = await fetch(pHttp, { headers: COMMON_HEADERS, redirect: "follow" });
  if (r2.ok) return r2.text();

  throw new Error(`Proxy falhou (https=${r1.status}, http=${r2.status}) para id ${id}`);
}

// ---------- Parsing ----------
function parseTabela($, containerSel, regiao) {
  const out = [];
  const $table = $(`${containerSel} table`);
  if (!$table.length) return out;

  $(`${containerSel} table tbody tr`).each((_, tr) => {
    const tds = $(tr)
      .find("td")
      .toArray()
      .map((td) => $(td).text().replace(/\s+/g, " ").trim());
    if (!tds.length) return;
    const [municipio, posto, tipo_posto, horas, prec] = tds;
    out.push({
      regiao,
      municipio: municipio || null,
      posto: posto || null,
      tipo_posto: tipo_posto || null,
      horas: horas || null,
      precipitacao_mm: parseNumberBR(prec),
    });
  });
  return out;
}

function extrairAbasDoHTML(html) {
  const $ = cheerio.load(html);
  const tabs = [
    { id: "agreste_potiguar", nome: "Agreste Potiguar" },
    { id: "central_potiguar", nome: "Central Potiguar" },
    { id: "leste_potiguar", nome: "Leste Potiguar" },
    { id: "oeste_potiguar", nome: "Oeste Potiguar" },
  ];
  let dados = [];
  for (const t of tabs) {
    const sel = `#${t.id}-content`;
    const linhas = parseTabela($, sel, t.nome);
    dados = dados.concat(linhas);
  }
  return dados;
}

// ---------- Main ----------
(async () => {
  ensureDir("data");

  const idHoje = idDoDia();
  const candidatos = [idHoje, idHoje - 1, idHoje + 1];

  console.log(`Fuso: ${TZ}`);
  console.log(`Data (local): ${formatDateLocal()}`);
  console.log(`ID base ${BASE_ID} -> ${BASE_DATE}`);
  console.log(`ID calculado para hoje: ${idHoje}`);
  console.log(`Candidatos: ${candidatos.join(", ")}`);

  for (const id of candidatos) {
    try {
      console.log(`\n>>> Tentando ID ${id} (direto)`);
      let html;
      try {
        html = await fetchHtmlDireto(id);
      } catch (e) {
        console.warn(`Direto falhou (${e.message}). Tentando proxy...`);
        html = await fetchHtmlProxy(id);
      }

      // Salva HTML para debug
      saveText("data/rendered.html", html);

      const dados = extrairAbasDoHTML(html);
      console.log(`ID ${id}: ${dados.length} linhas extraídas`);

      if (dados.length > 0) {
        saveText("data/latest.json", JSON.stringify({ id, dados }, null, 2));
        const csv = Papa.unparse(dados, { newline: "\n" });
        saveText("data/latest.csv", csv);
        console.log(`✅ Sucesso com ID ${id}. Arquivos salvos em data/latest.*`);
        process.exit(0);
      } else {
        console.warn(`ID ${id} sem linhas. Prosseguindo para o próximo candidato...`);
      }
    } catch (err) {
      console.warn(`Falha no ID ${id}: ${err.message}`);
    }
  }

  throw new Error("Não foi possível obter dados do boletim (nenhum candidato retornou linhas).");
})().catch((err) => {
  console.error("❌ ERRO:", err?.message || String(err));
  process.exit(1);
});
