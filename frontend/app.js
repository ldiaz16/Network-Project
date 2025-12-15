const metaApiBase = document.querySelector('meta[name="api-base"]');
const API_BASE = (metaApiBase?.content || "/api").replace(/\/+$/, "");

const form = document.getElementById("analysis-form");
const airlineInput = document.getElementById("airline-input");
const airlineOptions = document.getElementById("airline-options");
const analyzeBtn = document.getElementById("analyze-btn");
const messageEl = document.getElementById("form-message");
const summaryEls = {
    routes: document.getElementById("stat-routes"),
    seats: document.getElementById("stat-seats"),
    distance: document.getElementById("stat-distance"),
    longest: document.getElementById("stat-longest"),
};
const metaName = document.getElementById("meta-name");
const metaCode = document.getElementById("meta-code");
const networkList = document.getElementById("network-stats");
const equipmentList = document.getElementById("equipment-list");
const routesBody = document.getElementById("routes-body");
const intlRoutesBody = document.getElementById("intl-routes-body");

const numberFormatter = new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 });
const distanceFormatter = new Intl.NumberFormat(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 1 });

form.addEventListener("submit", async (event) => {
    event.preventDefault();
    await handleSubmission();
});

let airlineAutocompleteTimer = null;
let airlineAutocompleteController = null;
airlineInput.addEventListener("input", () => {
    if (!airlineOptions) {
        return;
    }
    const query = (airlineInput.value || "").trim();
    if (query.length < 2) {
        airlineOptions.innerHTML = "";
        return;
    }
    window.clearTimeout(airlineAutocompleteTimer);
    airlineAutocompleteTimer = window.setTimeout(() => {
        fetchAirlineSuggestions(query);
    }, 250);
});

async function fetchAirlineSuggestions(query) {
    if (!airlineOptions) {
        return;
    }

    if (airlineAutocompleteController) {
        airlineAutocompleteController.abort();
    }
    airlineAutocompleteController = new AbortController();

    try {
        const response = await fetch(
            `${API_BASE}/airlines?query=${encodeURIComponent(query)}`,
            { signal: airlineAutocompleteController.signal }
        );
        if (!response.ok) {
            return;
        }
        const payload = await response.json();
        airlineOptions.innerHTML = "";
        (payload || []).forEach((row) => {
            const airline = row?.airline;
            if (!airline) {
                return;
            }
            const iata = row?.iata;
            const country = row?.country;
            const option = document.createElement("option");
            option.value = iata ? `${airline} (${iata})` : airline;
            if (iata || country) {
                option.label = `${airline}${iata ? ` • ${iata}` : ""}${country ? ` • ${country}` : ""}`;
            } else {
                option.label = airline;
            }
            airlineOptions.appendChild(option);
        });
    } catch (error) {
        if (error?.name === "AbortError") {
            return;
        }
        console.warn("Unable to load airline suggestions.", error);
    }
}

async function handleSubmission() {
    const raw = (airlineInput.value || "").trim();
    const airline = extractAirlineQuery(raw);
    if (!airline) {
        displayMessage("Enter an airline name or IATA code before submitting.", "warning");
        return;
    }

    setLoading(true);
    displayMessage("Analyzing routes…", "info");

    try {
        const response = await fetch(`${API_BASE}/analysis`, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify({ airline, limit: 20 }),
        });

        if (!response.ok) {
            const payload = await response.json().catch(() => null);
            const reason = payload?.detail || "Unable to analyze that airline right now.";
            throw new Error(reason);
        }

        const payload = await response.json();
        renderResults(payload);
        displayMessage("Route analysis loaded.", "success");
    } catch (error) {
        console.error(error);
        displayMessage(error.message || "Something went wrong. Try again.", "error");
        resetDisplay();
    } finally {
        setLoading(false);
    }
}

function renderResults(payload) {
    const airline = payload.airline || {};
    const summary = payload.summary || {};
    const network = payload.network || {};
    const topRoutes = payload.top_routes || [];
    const topInternationalRoutes = payload.top_international_routes || [];
    const equipment = payload.top_equipment || {};

    metaName.textContent = airline.name || "Unknown";
    metaCode.textContent = airline.iata || airline.normalized || "—";

    summaryEls.routes.textContent = numberFormatter.format(summary.total_routes || 0);
    summaryEls.seats.textContent = numberFormatter.format(summary.total_seats || 0);
    summaryEls.distance.textContent = summary.average_distance
        ? `${distanceFormatter.format(summary.average_distance)} mi`
        : "—";
    summaryEls.longest.textContent = summary.longest_route
        ? `${distanceFormatter.format(summary.longest_route)} mi`
        : "—";

    renderList(networkList, network);
    renderEquipment(equipment);
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

function renderEquipment(equipment) {
    equipmentList.innerHTML = "";
    const entries = Object.entries(equipment);
    if (!entries.length) {
        equipmentList.innerHTML = `<li class="legend">Equipment signals will populate after the analysis.</li>`;
        return;
    }
    entries.forEach(([name, count]) => {
        const li = document.createElement("li");
        li.innerHTML = `<span>${name}</span><span>${formatValue(count)}</span>`;
        equipmentList.appendChild(li);
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
            <td>${row["Source airport Display"] || "—"}</td>
            <td>${row["Destination airport Display"] || "—"}</td>
            <td>${row["Equipment Display"] || "—"}</td>
            <td>${formatDistance(row["Distance (miles)"])}</td>
            <td>${formatValue(row["Total"])}</td>
            <td>${formatValue(row["ASM"])}</td>
        `;
        bodyElement.appendChild(tr);
    });
}

function extractAirlineQuery(raw) {
    const trimmed = (raw || "").trim();
    if (!trimmed) {
        return "";
    }
    const match = trimmed.match(/\(([A-Z0-9]{2,3})\)\s*$/i);
    if (match) {
        return match[1].toUpperCase();
    }
    return trimmed;
}

function formatValue(value) {
    if (value === null || value === undefined) {
        return "—";
    }
    if (typeof value === "number") {
        const formatter = Number.isInteger(value)
            ? numberFormatter
            : distanceFormatter;
        return formatter.format(value);
    }
    if (Array.isArray(value)) {
        return formatArray(value);
    }
    if (typeof value === "object") {
        return formatObject(value);
    }
    return String(value);
}

function formatArray(values) {
    if (!values.length) {
        return "—";
    }

    const isPairList = values.every(
        (item) => Array.isArray(item) && item.length === 2
    );
    if (isPairList) {
        return values
            .map(([label, metric]) => `${formatValue(label)} (${formatValue(metric)})`)
            .join(", ");
    }

    const isScalarList = values.every(
        (item) => item === null || ["string", "number", "boolean"].includes(typeof item)
    );
    if (isScalarList) {
        return values.map((item) => formatValue(item)).join(", ");
    }

    try {
        return JSON.stringify(values);
    } catch {
        return String(values);
    }
}

function formatObject(obj) {
    if (!obj) {
        return "—";
    }

    if ("hubs" in obj || "focus_cities" in obj || "source" in obj) {
        const parts = [];
        const hubs = Array.isArray(obj.hubs) ? obj.hubs.filter(Boolean) : [];
        const focusCities = Array.isArray(obj.focus_cities) ? obj.focus_cities.filter(Boolean) : [];
        if (hubs.length) {
            parts.push(`Hubs: ${hubs.join(", ")}`);
        }
        if (focusCities.length) {
            parts.push(`Focus Cities: ${focusCities.join(", ")}`);
        }
        if (obj.source) {
            parts.push(`Source: ${String(obj.source)}`);
        }
        return parts.join(" • ") || "—";
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
    summaryEls.routes.textContent = "—";
    summaryEls.seats.textContent = "—";
    summaryEls.distance.textContent = "—";
    summaryEls.longest.textContent = "—";
    networkList.innerHTML = `<li class="legend">Network stats appear here after an analysis.</li>`;
    equipmentList.innerHTML = `<li class="legend">Equipment signals will populate after the analysis.</li>`;
    routesBody.innerHTML = `<tr><td colspan="6" class="placeholder-row">Submit an airline to display top domestic routes.</td></tr>`;
    if (intlRoutesBody) {
        intlRoutesBody.innerHTML = `<tr><td colspan="6" class="placeholder-row">Submit an airline to display top international routes.</td></tr>`;
    }
}

function setLoading(active) {
    analyzeBtn.disabled = active;
    analyzeBtn.textContent = active ? "Analyzing…" : "Analyze routes";
}
