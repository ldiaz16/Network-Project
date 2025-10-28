const metaApiBase = document.querySelector('meta[name="api-base"]');
const DEFAULT_API_BASE = "http://localhost:8000/api";
const API_BASE = (() => {
    const candidate = window.API_BASE || (metaApiBase && metaApiBase.content) || DEFAULT_API_BASE;
    return candidate.replace(/\/+$/, "");
})();

const form = document.getElementById("analysis-form");
const statusElement = document.getElementById("status");
const messagesElement = document.getElementById("messages");
const comparisonElement = document.getElementById("comparison-results");
const cbsaElement = document.getElementById("cbsa-results");
const suggestionsList = document.getElementById("airline-suggestions");
const tableTemplate = document.getElementById("table-template");

let lastQuery = "";
let debounceHandle = null;

function setStatus(text, kind = "") {
    statusElement.textContent = text;
    statusElement.className = kind ? `status ${kind}` : "status";
}

function resetResults() {
    messagesElement.innerHTML = "";
    comparisonElement.innerHTML = "";
    cbsaElement.innerHTML = "";
}

function createTable(records, title) {
    if (!records || !records.length) {
        const placeholder = document.createElement("p");
        placeholder.className = "placeholder";
        placeholder.textContent = `No ${title.toLowerCase()} available.`;
        return placeholder;
    }

    const wrapper = tableTemplate.content.firstElementChild.cloneNode(true);
    const table = wrapper.querySelector("table");
    const thead = table.querySelector("thead");
    const tbody = table.querySelector("tbody");

    const headers = Object.keys(records[0]);
    const headerRow = document.createElement("tr");
    headers.forEach((header) => {
        const th = document.createElement("th");
        th.textContent = header;
        headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);

    records.forEach((row) => {
        const tr = document.createElement("tr");
        headers.forEach((header) => {
            const td = document.createElement("td");
            td.textContent = formatValue(row[header]);
            tr.appendChild(td);
        });
        tbody.appendChild(tr);
    });

    return wrapper;
}

function formatValue(value) {
    if (value === null || value === undefined || value === "") {
        return "—";
    }
    if (Array.isArray(value)) {
        return value.map((item) => formatValue(item)).join(", ");
    }
    if (typeof value === "object") {
        return JSON.stringify(value);
    }
    return value;
}

function renderMessages(messages = []) {
    messages.forEach((message) => {
        const pill = document.createElement("div");
        pill.className = "message-pill";
        pill.textContent = message;
        messagesElement.appendChild(pill);
    });
}

function renderNetworkStats(airlines) {
    if (!airlines || !airlines.length) {
        return;
    }

    const heading = document.createElement("h3");
    heading.className = "section-heading";
    heading.textContent = "Network Summary";
    comparisonElement.appendChild(heading);

    const grid = document.createElement("div");
    grid.className = "network-grid";

    airlines.forEach((airline) => {
        const card = document.createElement("div");
        card.className = "network-card";

        const title = document.createElement("h3");
        title.textContent = airline.name;
        card.appendChild(title);

        const list = document.createElement("ul");
        const stats = airline.network_stats || {};

        Object.entries(stats).forEach(([key, value]) => {
            const item = document.createElement("li");
            if (key.toLowerCase().includes("hub") && Array.isArray(value)) {
                const formatted = value
                    .map((hub) => Array.isArray(hub) ? `${hub[0]} — ${hub[1]}` : String(hub))
                    .join(", ");
                item.textContent = `${key}: ${formatted}`;
            } else {
                item.textContent = `${key}: ${formatValue(value)}`;
            }
            list.appendChild(item);
        });

        card.appendChild(list);
        grid.appendChild(card);
    });

    comparisonElement.appendChild(grid);
}

function renderComparison(comparison) {
    if (!comparison) {
        return;
    }

    renderNetworkStats(comparison.airlines);

    const heading = document.createElement("h3");
    heading.className = "section-heading";
    heading.textContent = "Competing Routes";
    comparisonElement.appendChild(heading);
    comparisonElement.appendChild(createTable(comparison.competing_routes, "Competing Routes"));
}

function renderCbsa(cbsaResults) {
    if (!cbsaResults || !cbsaResults.length) {
        return;
    }

    const heading = document.createElement("h3");
    heading.className = "section-heading";
    heading.textContent = "CBSA Opportunities";
    cbsaElement.appendChild(heading);

    cbsaResults.forEach((entry) => {
        const container = document.createElement("section");
        container.className = "cbsa-entry";

        const title = document.createElement("h3");
        title.textContent = entry.airline;
        container.appendChild(title);

        const bestRoutesHeading = document.createElement("h4");
        bestRoutesHeading.textContent = "Top Routes";
        container.appendChild(bestRoutesHeading);
        container.appendChild(createTable(entry.best_routes, "Top Routes"));

        const suggestHeading = document.createElement("h4");
        suggestHeading.textContent = "Suggested Opportunities";
        container.appendChild(suggestHeading);
        container.appendChild(createTable(entry.suggestions, "Suggested Opportunities"));

        cbsaElement.appendChild(container);
    });
}

async function fetchSuggestions(query = "") {
    try {
        const url = query ? `${API_BASE}/airlines?query=${encodeURIComponent(query)}` : `${API_BASE}/airlines`;
        const response = await fetch(url);
        if (!response.ok) {
            return;
        }
        const airlines = await response.json();
        populateDatalist(airlines);
    } catch (error) {
        // Silently ignore suggestion errors to avoid blocking the UI.
    }
}

function populateDatalist(airlines) {
    const existing = new Set(Array.from(suggestionsList.children).map((node) => node.value));
    airlines.forEach((airline) => {
        if (!existing.has(airline.airline)) {
            const option = document.createElement("option");
            option.value = airline.airline;
            suggestionsList.appendChild(option);
        }
    });
}

async function handleSubmit(event) {
    event.preventDefault();
    resetResults();
    setStatus("Running analysis…");
    form.querySelector("button[type='submit']").disabled = true;

    const formData = new FormData(form);
    const skipComparison = formData.get("skip_comparison") === "on";
    const comparisonAirlines = [
        formData.get("comparison_airline_1")?.trim() || "",
        formData.get("comparison_airline_2")?.trim() || "",
    ].filter(Boolean);

    const cbsaAirlines = (formData.get("cbsa_airlines") || "")
        .split(/\r?\n|,/)
        .map((entry) => entry.trim())
        .filter(Boolean);

    const payload = {
        comparison_airlines: skipComparison ? [] : comparisonAirlines,
        skip_comparison: skipComparison,
        cbsa_airlines: cbsaAirlines,
        cbsa_top_n: Number(formData.get("cbsa_top_n")) || 5,
        cbsa_suggestions: Number(formData.get("cbsa_suggestions")) || 3,
        build_cbsa_cache: formData.get("build_cbsa_cache") === "on",
    };

    const countriesRaw = formData.get("cbsa_cache_country");
    if (countriesRaw && countriesRaw.trim()) {
        payload.cbsa_cache_country = countriesRaw.split(",").map((country) => country.trim()).filter(Boolean);
    }

    const cacheLimit = formData.get("cbsa_cache_limit");
    if (cacheLimit) {
        payload.cbsa_cache_limit = Number(cacheLimit);
    }

    const chunkSize = formData.get("cbsa_cache_chunk_size");
    if (chunkSize) {
        payload.cbsa_cache_chunk_size = Number(chunkSize);
    }

    try {
        const response = await fetch(`${API_BASE}/run`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
        });

        const result = await response.json();
        if (!response.ok) {
            throw new Error(result.detail || "Request failed");
        }

        setStatus("Analysis complete.", "success");
        renderMessages(result.messages);
        renderComparison(result.comparison);
        renderCbsa(result.cbsa);
    } catch (error) {
        setStatus(error.message || "Unable to complete the request.", "error");
    } finally {
        form.querySelector("button[type='submit']").disabled = false;
    }
}

function watchSuggestionInputs() {
    const comparisonInputs = form.querySelectorAll("input[list='airline-suggestions']");
    comparisonInputs.forEach((input) => {
        input.addEventListener("input", () => {
            const value = input.value.trim();
            if (value.length < 2) {
                return;
            }
            if (value === lastQuery) {
                return;
            }
            lastQuery = value;
            if (debounceHandle) {
                clearTimeout(debounceHandle);
            }
            debounceHandle = setTimeout(() => {
                fetchSuggestions(value);
            }, 200);
        });
    });
}

form.addEventListener("submit", handleSubmit);
watchSuggestionInputs();
fetchSuggestions();
