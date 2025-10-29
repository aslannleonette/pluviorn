// scripts/fetch-emparn.js
// Projeto: pluviorn — Observatório Pluviométrico RN

const fs = require("fs");
const { chromium } = require("playwright");
const Papa = require("papaparse");

const URL = "https://meteorologia.emparn.rn.gov.br/boletim/diario";
const UA = "PluvioRN-Bot/1.0 (+github actions)";

const ensureDir = (p) => fs.mkdirSync(p, { recursive: true });
const save = (path, text) => fs.writeFileSync(path, text, "utf8");

async function gotoWithRetry(page, url, retries = 3) {
  for (let i = 1; i <= retries; i++) {
    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 180000 });
      await page.waitForSelector('a#agreste_potiguar', { timeout: 120000 });
      return;
    } catch (err) {
      console.log(`Tentativa ${i} falhou: ${err.message}`);
      await new Promise(r => setTimeout(r, 15000 * i));
    }
  }
  throw new Error("Falha ao carregar página da EMPARN.");
}

async function extractTable(page, sel, regiao) {
  await page.waitForSelector(`${sel} table`, { timeout: 120000 });
  return page.$$eval(`${sel} table tbody tr`, trs =>
    trs.map(tr => {
      const tds = Array.from(tr.querySelectorAll("td")).map(td => td.textContent.trim());
      return {
        regiao,
        municipio: tds[0],
        posto: tds[1],
        tipo_posto: tds[2],
        horas: tds[3],
        precipitacao_mm: parseFloat(tds[4].replace(",", "."))
      };
    })
  );
}

(async () => {
  ensureDir("data");
  const browser = await chromium.launch({ headless: true, args: ["--no-sandbox"] });
  const page = await (await browser.newContext({ userAgent: UA })).newPage();

  console.log(">> Acessando:", URL);
  await gotoWithRetry(page, URL);

  const abas = [
    { id: "agreste_potiguar", nome: "Agreste Potiguar" },
    { id: "central_potiguar", nome: "Central Potiguar" },
    { id: "leste_potiguar", nome: "Leste Potiguar" },
    { id: "oeste_potiguar", nome: "Oeste Potiguar" }
  ];

  let dados = [];
  for (const aba of abas) {
    console.log(`→ Lendo ${aba.nome}`);
    await page.click(`a#${aba.id}`);
    const linhas = await extractTable(page, `#${aba.id}-content`, aba.nome);
    console.log(`   ${linhas.length} registros`);
    dados.push(...linhas);
  }

  save("data/latest.json", JSON.stringify(dados, null, 2));
  save("data/latest.csv", Papa.unparse(dados));
  console.log(`✅ Salvos: ${dados.length} registros`);

  await browser.close();
})().catch(err => {
  console.error("❌ Erro:", err.message);
  process.exit(1);
});
