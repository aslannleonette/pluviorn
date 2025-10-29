/**
 * scripts/fetch-inmet.js
 * Coleta dados horários das estações automáticas do INMET (últimos 3 dias) e filtra por UF.
 * Gera: data/inmet.json, data/latest.json e data/latest.csv
 *
 * Requisitos: Node 20+. (sem libs extras)
 * Personalização por env:
 *   UF=RN            → filtra a unidade da federação (padrão RN)
 *   DIAS=3           → janela em dias (padrão 3)
 */

const fs = require("fs");

const UF = (process.env.UF || "RN").toUpperCase();
const DIAS = Math.max(1, parseInt(process.env.DIAS || "3", 10)); // janela de dias
const BASE = "https://apitempo.inmet.gov.br";

function ensureDir(p) { fs.mkdirSync(p, { recursive: true }); }
function saveJSON(path, obj) { fs.writeFileSync(path, JSON.stringify(obj, null, 2), "utf8"); }
function saveText(path, txt) { fs.writeFileSync(path, txt, "utf8"); }

function isoDiaUTC(d, deltaDias = 0) {
  const x = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()) + deltaDias * 86400000);
  const yyyy = x.getUTCFullYear();
  const mm = String(x.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(x.getUTCDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

async function getJSON(url) {
  const r = await fetch(url, {
    headers: {
      "User-Agent": "pluviorn-inmet/1.0",
      "Accept": "application/json,*/*"
    }
  });
  if (!r.ok) throw new Error(`HTTP ${r.status} em ${url}`);
  return r.json();
}

function numberOrNull(v) {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(String(v).replace(",", "."));
  return Number.isFinite(n) ? n : null;
}

function toCSV(arr) {
  const headers = ["source","regiao","municipio","posto","tipo_posto","horas","precipitacao_mm","station_code","lat","lon"];
  const lines = [headers.join(",")];
  for (const r of arr) {
    const row = headers.map(h => {
      let v = r[h];
      if (v === null || v === undefined) return "";
      if (typeof v === "string") {
        v = v.replace(/"/g, '""');
        return (v.includes(",") || v.includes('"') || v.includes("\n")) ? `"${v}"` : v;
      }
      return String(v);
    });
    lines.push(row.join(","));
  }
  return lines.join("\n") + "\n";
}

(async () => {
  ensureDir("data");

  // 1) Metadados das estações
  const stations = await getJSON(`${BASE}/estacoes`);
  const stUF = stations.filter(s => {
    const uf = (s.UF || s.DC_UF || "").toUpperCase();
    return uf === UF;
  });

  const stationByCode = new Map();
  for (const s of stUF) {
    const code =
      String(s.CD_ESTACAO || s.CD_EST || s.CD_WIGOS || s.CD_OMM || s.CD_SIRED || "").trim();
    if (!code) continue;
    stationByCode.set(code, {
      code,
      nome: s.DC_NOME || s.NOME || s.NM_ESTACAO || code,
      uf: s.UF || s.DC_UF || UF,
      lat: numberOrNull(s.VL_LATITUDE || s.LATITUDE || s.LAT),
      lon: numberOrNull(s.VL_LONGITUDE || s.LONGITUDE || s.LON),
      tipo: s.TP_ESTACAO || s.TIPO || "automática",
    });
  }

  // 2) Série operacional (T) — últimas DIAS datas
  const hoje = new Date(); // UTC
  const data_final = isoDiaUTC(hoje, 0);
  const data_inicial = isoDiaUTC(hoje, -DIAS + 1);
  const dadosUrl = `${BASE}/estacoes/T?data_inicial=${data_inicial}&data_final=${data_final}`;
  const rows = await getJSON(dadosUrl);

  const out = [];
  for (const r of rows) {
    const cod = String(r.CD_ESTACAO || r.CD_WIGOS || r.CD_OMM || "").trim();
    const st = stationByCode.get(cod);
    if (!st) continue; // mantém só UF desejada

    // Precipitação horário: chaves comuns no INMET
    const mm =
      r.PRELIQ_TOT ?? r.PRECI_TOT ?? r.CHUVA ?? r.PRECIPITACAO ?? r.PREC ?? null;

    // Timestamp amigável
    const ts = r.DT_MEDICAO && r.HR_MEDICAO
      ? `${r.DT_MEDICAO} ${String(r.HR_MEDICAO).padStart(4, "0").replace(/(\d{2})(\d{2})/, "$1:$2")}`
      : (r.DT_MEDICAO || null);

    out.push({
      source: "INMET",
      regiao: null,
      municipio: r.DC_NOME || r.MUNICIPIO || null,
      posto: st.nome || cod,
      tipo_posto: st.tipo || "automática",
      horas: ts,
      precipitacao_mm: numberOrNull(mm),
      station_code: cod,
      lat: st.lat,
      lon: st.lon,
    });
  }

  // 3) Dedup simples
  const key = r => `${r.station_code}::${r.horas || ""}::${r.precipitacao_mm ?? ""}`;
  const uniq = Array.from(new Map(out.map(x => [key(x), x])).values());

  console.log(`INMET (${UF}) — janela ${data_inicial}..${data_final}:`);
  console.log(`Estações na UF: ${stationByCode.size}`);
  console.log(`Registros brutos: ${out.length} | únicos: ${uniq.length}`);

  saveJSON("data/inmet.json", uniq);
  saveJSON("data/latest.json", uniq);
  saveText("data/latest.csv", toCSV(uniq));
})().catch(e => {
  console.error("INMET ERRO:", e.stack || e.message);
  process.exit(1);
});
