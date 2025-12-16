const metaApiBase = document.querySelector('meta[name="api-base"]');
const API_BASE = (metaApiBase?.content || "/api").replace(/\/+$/, "");

const form = document.getElementById("markets-form");
const messageEl = document.getElementById("form-message");
const sinceYearEl = document.getElementById("since-year");
const marketDefinitionEl = document.getElementById("market-definition");
const metaWindowEl = document.getElementById("meta-window");
const metaDefinitionEl = document.getElementById("meta-definition");

const statsEls = {
    markets: document.getElementById("stat-markets"),
    passengers: document.getElementById("stat-passengers"),
    top10: document.getElementById("stat-top10"),
    top1: document.getElementById("stat-top1"),
};

const topMarketsBody = document.getElementById("top-markets-body");
const topMarketsExclBody = document.getElementById("top-markets-excl-body");

const stabilitySearchEl = document.getElementById("stability-search");
const stabilityClassEl = document.getElementById("stability-classification");
const stabilityMinTotalEl = document.getElementById("stability-min-total");
const stabilityBody = document.getElementById("stability-body");
const stabilityCountEl = document.getElementById("stability-count");
const stabilityPrevBtn = document.getElementById("stability-prev");
const stabilityNextBtn = document.getElementById("stability-next");

const numberFormatter = new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 });
const decimalFormatter = new Intl.NumberFormat(undefined, { maximumFractionDigits: 3 });
const percentFormatter = new Intl.NumberFormat(undefined, { style: "percent", maximumFractionDigits: 1 });
const currencyFormatter = new Intl.NumberFormat(undefined, { style: "currency", currency: "USD", maximumFractionDigits: 0 });

let stabilityOffset = 0;
const stabilityLimit = 250;
let stabilityTotal = 0;
let stabilityFetchTimer = null;

form.addEventListener("submit", async (event) => {
    event.preventDefault();
    stabilityOffset = 0;
    await refreshAll();
});

function displayMessage(message, variant = "info") {
    if (!messageEl) {
        return;
    }
    messageEl.textContent = message || "";
    messageEl.dataset.variant = variant;
}

function getSettings() {
    const sinceYear = parseInt(sinceYearEl.value, 10) || 2022;
    const directional = (marketDefinitionEl.value || "0") === "1";
    return { sinceYear, directional };
}

async function fetchJson(path) {
    const response = await fetch(path);
    if (!response.ok) {
        const payload = await response.json().catch(() => null);
        const reason = payload?.detail || `Request failed (${response.status}).`;
        throw new Error(reason);
    }
    return response.json();
}

function formatNumber(value) {
    if (value === null || value === undefined || Number.isNaN(value)) {
        return "—";
    }
    return numberFormatter.format(value);
}

function formatCurrency(value) {
    if (value === null || value === undefined || Number.isNaN(value)) {
        return "—";
    }
    return currencyFormatter.format(value);
}

function formatDecimal(value) {
    if (value === null || value === undefined || Number.isNaN(value)) {
        return "—";
    }
    return decimalFormatter.format(value);
}

function formatPercent(value) {
    if (value === null || value === undefined || Number.isNaN(value)) {
        return "—";
    }
    return percentFormatter.format(value);
}

function renderTopMarkets(bodyEl, markets) {
    bodyEl.innerHTML = "";
    if (!markets?.length) {
        bodyEl.innerHTML = `<tr><td colspan="6" class="placeholder-row">No markets found.</td></tr>`;
        return;
    }
    markets.forEach((row) => {
        const tr = document.createElement("tr");
        const passengers = row.passengers_total ?? row.passengers;
        tr.innerHTML = `
            <td>${row.rank ?? "—"}</td>
            <td>${row.market || `${row.market_a || "—"}-${row.market_b || "—"}`}</td>
            <td>${formatNumber(passengers)}</td>
            <td>${formatCurrency(row.avg_fare)}</td>
            <td>${formatNumber(row.distance)}</td>
            <td>${formatDecimal(row.fare_per_mile)}</td>
        `;
        bodyEl.appendChild(tr);
    });
}

function renderConcentration(summary) {
    const top10 = summary?.top_10pct;
    const top1 = summary?.top_1pct;
    const markets = top10?.markets;
    const totalPassengers = top10?.total_passengers;

    statsEls.markets.textContent = markets !== undefined ? formatNumber(markets) : "—";
    statsEls.passengers.textContent = totalPassengers !== undefined ? formatNumber(totalPassengers) : "—";
    statsEls.top10.textContent = top10?.top_passenger_share !== undefined ? formatPercent(top10.top_passenger_share) : "—";
    statsEls.top1.textContent = top1?.top_passenger_share !== undefined ? formatPercent(top1.top_passenger_share) : "—";
}

function updateMeta(settings) {
    metaWindowEl.textContent = `Since ${settings.sinceYear}`;
    metaDefinitionEl.textContent = settings.directional ? "Directional" : "Undirected";
}

function renderStability(payload) {
    const rows = payload?.rows || [];
    stabilityTotal = payload?.total || 0;
    const start = stabilityTotal ? stabilityOffset + 1 : 0;
    const end = Math.min(stabilityTotal, stabilityOffset + stabilityLimit);

    stabilityCountEl.textContent = stabilityTotal
        ? `Showing ${formatNumber(start)}–${formatNumber(end)} of ${formatNumber(stabilityTotal)} markets`
        : "No markets found.";

    stabilityPrevBtn.disabled = stabilityOffset <= 0;
    stabilityNextBtn.disabled = stabilityOffset + stabilityLimit >= stabilityTotal;

    stabilityBody.innerHTML = "";
    if (!rows.length) {
        stabilityBody.innerHTML = `<tr><td colspan="8" class="placeholder-row">No markets match the filters.</td></tr>`;
        return;
    }

    rows.forEach((row) => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
            <td>${row.market || `${row.market_a || "—"}-${row.market_b || "—"}`}</td>
            <td>${formatNumber(row.total_passengers)}</td>
            <td>${formatNumber(row.mean_passengers)}</td>
            <td>${formatDecimal(row.cv)}</td>
            <td>${formatDecimal(row.seasonality_index)}</td>
            <td>${formatDecimal(row.residual_cv)}</td>
            <td>${formatDecimal(row.active_share)}</td>
            <td>${row.classification || "—"}</td>
        `;
        stabilityBody.appendChild(tr);
    });
}

async function refreshAll() {
    const settings = getSettings();
    updateMeta(settings);
    displayMessage("Loading demand markets…", "info");

    try {
        const topUrl = `${API_BASE}/demand/markets/top?since_year=${settings.sinceYear}&directional=${settings.directional ? 1 : 0}&top_n=50`;
        const exclUrl = `${API_BASE}/demand/markets/top?since_year=${settings.sinceYear}&directional=${settings.directional ? 1 : 0}&top_n=50&exclude_big3=1`;
        const concUrl = `${API_BASE}/demand/markets/concentration?since_year=${settings.sinceYear}&directional=${settings.directional ? 1 : 0}`;

        const [top, excl, conc] = await Promise.all([
            fetchJson(topUrl),
            fetchJson(exclUrl),
            fetchJson(concUrl),
        ]);

        renderTopMarkets(topMarketsBody, top?.markets);
        renderTopMarkets(topMarketsExclBody, excl?.markets);
        renderConcentration(conc);
        await refreshStability();
        displayMessage("Market analysis loaded.", "success");
    } catch (error) {
        console.error(error);
        displayMessage(error.message || "Unable to load market analysis.", "error");
        topMarketsBody.innerHTML = `<tr><td colspan="6" class="placeholder-row">—</td></tr>`;
        topMarketsExclBody.innerHTML = `<tr><td colspan="6" class="placeholder-row">—</td></tr>`;
        stabilityBody.innerHTML = `<tr><td colspan="8" class="placeholder-row">—</td></tr>`;
    }
}

async function refreshStability() {
    const settings = getSettings();
    const q = (stabilitySearchEl.value || "").trim();
    const classification = (stabilityClassEl.value || "").trim();
    const minTotal = parseFloat(stabilityMinTotalEl.value || "0") || 0;

    const params = new URLSearchParams({
        since_year: String(settings.sinceYear),
        directional: settings.directional ? "1" : "0",
        offset: String(stabilityOffset),
        limit: String(stabilityLimit),
        sort_by: "total_passengers",
        sort_dir: "desc",
    });
    if (q) {
        params.set("q", q);
    }
    if (classification) {
        params.set("classification", classification);
    }
    if (minTotal > 0) {
        params.set("min_total_passengers", String(minTotal));
    }

    const payload = await fetchJson(`${API_BASE}/demand/markets/stability?${params.toString()}`);
    renderStability(payload);
}

function scheduleStabilityRefresh() {
    window.clearTimeout(stabilityFetchTimer);
    stabilityFetchTimer = window.setTimeout(async () => {
        stabilityOffset = 0;
        try {
            await refreshStability();
        } catch (error) {
            console.warn(error);
        }
    }, 250);
}

stabilitySearchEl.addEventListener("input", scheduleStabilityRefresh);
stabilityClassEl.addEventListener("change", scheduleStabilityRefresh);
stabilityMinTotalEl.addEventListener("input", scheduleStabilityRefresh);

stabilityPrevBtn.addEventListener("click", async () => {
    stabilityOffset = Math.max(0, stabilityOffset - stabilityLimit);
    await refreshStability();
});

stabilityNextBtn.addEventListener("click", async () => {
    stabilityOffset = Math.min(stabilityTotal, stabilityOffset + stabilityLimit);
    await refreshStability();
});

refreshAll();

