const metaApiBase = document.querySelector('meta[name="api-base"]');
const API_BASE = (metaApiBase?.content || "/api").replace(/\/+$/, "");

const form = document.getElementById("alliance-form");
const allianceSelect = document.getElementById("alliance-select");
const analyzeBtn = document.getElementById("analyze-btn");
const messageEl = document.getElementById("form-message");

const metaName = document.getElementById("meta-name");
const metaCode = document.getElementById("meta-code");

const summaryEls = {
    members: document.getElementById("stat-members"),
    routes: document.getElementById("stat-routes"),
    seats: document.getElementById("stat-seats"),
    distance: document.getElementById("stat-distance"),
    longest: document.getElementById("stat-longest"),
};

const networkList = document.getElementById("network-stats");
const carriersBody = document.getElementById("carriers-body");
const routesBody = document.getElementById("routes-body");
const intlRoutesBody = document.getElementById("intl-routes-body");

const numberFormatter = new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 });
const distanceFormatter = new Intl.NumberFormat(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 1 });

form.addEventListener("submit", async (event) => {
    event.preventDefault();
    await handleSubmission();
});

loadAlliances();

async function loadAlliances() {
    displayMessage("Loading alliances…", "info");
    setLoading(true);

    try {
        const response = await fetch(`${API_BASE}/alliances`);
        if (!response.ok) {
            throw new Error("Unable to load alliances.");
        }
        const payload = await response.json();
        renderAllianceOptions(payload || []);
        displayMessage("Select an alliance to begin.", "success");
    } catch (error) {
        console.error(error);
        renderAllianceOptions([]);
        displayMessage(error.message || "Unable to load alliances.", "error");
    } finally {
        setLoading(false);
    }
}

function renderAllianceOptions(alliances) {
    allianceSelect.innerHTML = "";
    if (!alliances.length) {
        const option = document.createElement("option");
        option.value = "";
        option.textContent = "No alliances available";
        allianceSelect.appendChild(option);
        allianceSelect.disabled = true;
        return;
    }

    allianceSelect.disabled = false;
    const placeholder = document.createElement("option");
    placeholder.value = "";
    placeholder.textContent = "Select an alliance…";
    placeholder.selected = true;
    allianceSelect.appendChild(placeholder);

    alliances.forEach((row) => {
        const code = row?.code;
        const name = row?.name;
        if (code === undefined || code === null || name === undefined) {
            return;
        }
        const option = document.createElement("option");
        option.value = String(code);
        option.textContent = `${name} (${code})`;
        allianceSelect.appendChild(option);
    });
}

async function handleSubmission() {
    const raw = (allianceSelect.value || "").trim();
    const carrierGroup = parseInt(raw, 10);
    if (!Number.isFinite(carrierGroup)) {
        displayMessage("Pick an alliance before submitting.", "warning");
        return;
    }

    setLoading(true);
    displayMessage("Analyzing alliance network…", "info");

    try {
        const response = await fetch(`${API_BASE}/alliance`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ carrier_group: carrierGroup, limit: 20 }),
        });

        if (!response.ok) {
            const payload = await response.json().catch(() => null);
            const reason = payload?.detail || "Unable to analyze that alliance right now.";
            throw new Error(reason);
        }

        const payload = await response.json();
        renderResults(payload);
        displayMessage("Alliance analysis loaded.", "success");
    } catch (error) {
        console.error(error);
        displayMessage(error.message || "Something went wrong. Try again.", "error");
        resetDisplay();
    } finally {
        setLoading(false);
    }
}

function renderResults(payload) {
    const alliance = payload.alliance || {};
    const summary = payload.summary || {};
    const network = payload.network || {};
    const carriers = payload.top_carriers || [];
    const topRoutes = payload.top_routes || [];
    const topInternationalRoutes = payload.top_international_routes || [];

    metaName.textContent = alliance.name || "Unknown";
    metaCode.textContent = alliance.code !== undefined && alliance.code !== null ? `Code ${alliance.code}` : "—";

    summaryEls.members.textContent = numberFormatter.format(summary.member_carriers || 0);
    summaryEls.routes.textContent = numberFormatter.format(summary.total_routes || 0);
    summaryEls.seats.textContent = numberFormatter.format(summary.total_seats || 0);
    summaryEls.distance.textContent = summary.average_distance
        ? `${distanceFormatter.format(summary.average_distance)} mi`
        : "—";
    summaryEls.longest.textContent = summary.longest_route
        ? `${distanceFormatter.format(summary.longest_route)} mi`
        : "—";

    renderList(networkList, network);
    renderCarriers(carriersBody, carriers);
    renderRoutes(routesBody, topRoutes, "No top domestic routes available.");
    renderRoutes(intlRoutesBody, topInternationalRoutes, "No top international routes available.");
}

function renderList(listElement, values) {
    listElement.innerHTML = "";
    Object.entries(values || {}).forEach(([label, value]) => {
        const li = document.createElement("li");
        if (label === "Hubs" || label === "Focus Cities") {
            li.classList.add("stats-wide");
        }
        const labelSpan = document.createElement("span");
        labelSpan.textContent = label;
        const valueSpan = document.createElement("span");
        valueSpan.textContent = formatValue(value);
        li.append(labelSpan, valueSpan);
        listElement.appendChild(li);
    });
    if (!listElement.children.length) {
        listElement.innerHTML = `<li class="legend">Network stats appear here after an analysis.</li>`;
    }
}

function renderCarriers(bodyElement, carriers) {
    if (!bodyElement) {
        return;
    }
    bodyElement.innerHTML = "";
    if (!carriers.length) {
        bodyElement.innerHTML = `<tr><td colspan="5" class="placeholder-row">No member carriers available.</td></tr>`;
        return;
    }

    carriers.forEach((row) => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
            <td>${escapeText(row.airline || "—")}</td>
            <td>${escapeText(row.iata || "—")}</td>
            <td>${escapeText(row.country || "—")}</td>
            <td>${formatValue(row.total_routes)}</td>
            <td>${formatValue(row.total_seats)}</td>
        `;
        bodyElement.appendChild(tr);
    });
}

function renderRoutes(bodyElement, routes, emptyLabel) {
    if (!bodyElement) {
        return;
    }
    bodyElement.innerHTML = "";
    if (!routes.length) {
        bodyElement.innerHTML = `<tr><td colspan="6" class="placeholder-row">${emptyLabel}</td></tr>`;
        return;
    }
    routes.forEach((row) => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
            <td>${escapeText(row["Source airport Display"] || "—")}</td>
            <td>${escapeText(row["Destination airport Display"] || "—")}</td>
            <td>${escapeText(row["Equipment Display"] || "—")}</td>
            <td>${formatDistance(row["Distance (miles)"])}</td>
            <td>${formatValue(row["Total"])}</td>
            <td>${formatValue(row["ASM"])}</td>
        `;
        bodyElement.appendChild(tr);
    });
}

function formatValue(value) {
    if (value === null || value === undefined) {
        return "—";
    }
    if (typeof value === "number") {
        const formatter = Number.isInteger(value) ? numberFormatter : distanceFormatter;
        return formatter.format(value);
    }
    if (Array.isArray(value)) {
        return formatArray(value);
    }
    if (typeof value === "object") {
        return formatObject(value);
    }
    const numeric = Number(value);
    if (Number.isFinite(numeric)) {
        return numberFormatter.format(numeric);
    }
    return String(value);
}

function formatArray(values) {
    if (!values.length) {
        return "—";
    }
    const isPairList = values.every((item) => Array.isArray(item) && item.length === 2);
    if (isPairList) {
        return values
            .map(([label, metric]) => `${formatValue(label)} (${formatValue(metric)})`)
            .join(", ");
    }
    return values.map((item) => formatValue(item)).join(", ");
}

function formatObject(obj) {
    if (!obj) {
        return "—";
    }
    const entries = Object.entries(obj);
    if (!entries.length) {
        return "—";
    }
    return entries
        .map(([key, value]) => `${key}: ${formatValue(value)}`)
        .join(" • ");
}

function formatDistance(value) {
    const distance = parseFloat(value);
    if (!Number.isFinite(distance)) {
        return "—";
    }
    return `${distanceFormatter.format(distance)} mi`;
}

function displayMessage(text, type) {
    messageEl.textContent = text;
    messageEl.dataset.variant = type;
}

function resetDisplay() {
    metaName.textContent = "—";
    metaCode.textContent = "—";
    summaryEls.members.textContent = "—";
    summaryEls.routes.textContent = "—";
    summaryEls.seats.textContent = "—";
    summaryEls.distance.textContent = "—";
    summaryEls.longest.textContent = "—";
    networkList.innerHTML = `<li class="legend">Network stats appear here after an analysis.</li>`;
    carriersBody.innerHTML = `<tr><td colspan="5" class="placeholder-row">Select an alliance to display member carriers.</td></tr>`;
    routesBody.innerHTML = `<tr><td colspan="6" class="placeholder-row">Select an alliance to display top domestic routes.</td></tr>`;
    intlRoutesBody.innerHTML = `<tr><td colspan="6" class="placeholder-row">Select an alliance to display top international routes.</td></tr>`;
}

function setLoading(active) {
    analyzeBtn.disabled = active;
    analyzeBtn.textContent = active ? "Analyzing…" : "Analyze alliance";
}

function escapeText(value) {
    const text = value === null || value === undefined ? "" : String(value);
    return text
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
}

