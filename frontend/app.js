const metaApiBase = typeof document !== "undefined" ? document.querySelector('meta[name="api-base"]') : null;
const LOCAL_API_BASE = "http://localhost:8000/api";

const isBrowser = typeof window !== "undefined";
const isLocalhost = (() => {
    if (!isBrowser || !window.location) {
        return false;
    }
    const hostname = (window.location.hostname || "").toLowerCase();
    if (!hostname) {
        return true;
    }
    if (hostname === "localhost" || hostname === "0.0.0.0") {
        return true;
    }
    if (hostname === "::1" || hostname === "[::1]") {
        return true;
    }
    if (hostname.startsWith("127.")) {
        return true;
    }
    return false;
})();

const sanitizeBase = (value) => (value || "").replace(/\/+$/, "");

const resolveCandidateBase = () => {
    if (!isBrowser) {
        return null;
    }
    const candidate = window.API_BASE || (metaApiBase && metaApiBase.content);
    if (!candidate) {
        return null;
    }
    if (/^https?:\/\//i.test(candidate)) {
        return sanitizeBase(candidate);
    }
    if (candidate.startsWith("//")) {
        const protocol = window.location && window.location.protocol ? window.location.protocol : "http:";
        return sanitizeBase(`${protocol}${candidate}`);
    }
    if (candidate.startsWith("/")) {
        if (isLocalhost) {
            return sanitizeBase(LOCAL_API_BASE);
        }
        if (window.location && window.location.origin && window.location.origin !== "null") {
            return sanitizeBase(`${window.location.origin}${candidate}`);
        }
        return null;
    }
    if (window.location && window.location.origin && window.location.origin !== "null") {
        const trimmed = candidate.replace(/^\/+/, "");
        return sanitizeBase(`${window.location.origin}/${trimmed}`);
    }
    return null;
};

const DEFAULT_API_BASE = (() => {
    if (isLocalhost) {
        return LOCAL_API_BASE;
    }
    if (isBrowser && window.location && window.location.origin && window.location.origin !== "null") {
        return `${window.location.origin}/api`;
    }
    return LOCAL_API_BASE;
})();

const API_BASE = (() => {
    const resolved = resolveCandidateBase();
    return sanitizeBase(resolved || DEFAULT_API_BASE);
})();

const {
    ThemeProvider,
    createTheme,
    CssBaseline,
    Container,
    Box,
    AppBar,
    Toolbar,
    Typography,
    Paper,
    Grid,
    TextField,
    Button,
    FormControlLabel,
    Switch,
    Checkbox,
    Stack,
    Divider,
    Chip,
    Card,
    CardContent,
    CardHeader,
    Alert,
    Table,
    TableContainer,
    TableHead,
    TableRow,
    TableCell,
    TableBody,
    Autocomplete,
    CircularProgress,
    Tabs,
    Tab,
    Tooltip,
} = MaterialUI;

const darkTheme = createTheme({
    palette: {
        mode: "dark",
        primary: {
            main: "#8ab4f8",
        },
        secondary: {
            main: "#f28b82",
        },
        background: {
            default: "#050c1a",
            paper: "#0f172a",
        },
    },
    shape: {
        borderRadius: 16,
    },
    components: {
        MuiPaper: {
            styleOverrides: {
                root: {
                    backgroundImage: "none",
                },
            },
        },
    },
});

const defaultFormState = {
    comparison_airline_1: "",
    comparison_airline_2: "",
    skip_comparison: false,
    cbsa_airlines: "",
    cbsa_top_n: 5,
    cbsa_suggestions: 3,
    build_cbsa_cache: false,
    cbsa_cache_country: "",
    cbsa_cache_limit: "",
    cbsa_cache_chunk_size: 200,
};

const MAX_AIRLINE_SUGGESTIONS = 25;
const integerNumberFormatter = new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 });
const decimalNumberFormatter = new Intl.NumberFormat(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const percentFormatter = new Intl.NumberFormat(undefined, { style: "percent", maximumFractionDigits: 0 });
const JFK = "JFK";
const MIA = "MIA";
const AMERICAN_ALIASES = ["american airlines", "aa"];
const LOAD_FACTOR_BENCHMARKS = {
    "delta air lines": 0.86,
    "delta": 0.86,
    "dl": 0.86,
    "united airlines": 0.844,
    "united": 0.844,
    "ua": 0.844,
    "jetblue airways corporation": 0.851,
    "jetblue airways": 0.851,
    "jetblue": 0.851,
    "b6": 0.851,
};

function formatValue(value) {
    if (value === null || value === undefined || value === "") {
        return "-";
    }
    if (Array.isArray(value)) {
        return value.map((entry) => formatValue(entry)).join(", ");
    }
    if (typeof value === "number") {
        if (!Number.isFinite(value)) {
            return "-";
        }
        const hasFraction = Math.abs(value - Math.trunc(value)) > 1e-6;
        const formatter = hasFraction ? decimalNumberFormatter : integerNumberFormatter;
        return formatter.format(value);
    }
    if (typeof value === "object") {
        return JSON.stringify(value);
    }
    return value;
}

function formatNetworkStat(key, value) {
    if (value === null || value === undefined) {
        return "-";
    }
    if (key.toLowerCase().includes("hub") && Array.isArray(value)) {
        return value
            .map((hub) => (Array.isArray(hub) ? `${hub[0]} - ${hub[1]}` : formatValue(hub)))
            .join(", ");
    }
    return formatValue(value);
}

function formatPercent(value) {
    if (typeof value !== "number" || Number.isNaN(value)) {
        return "-";
    }
    return percentFormatter.format(value);
}

const InfoHint = ({ label, tooltip }) => (
    <Tooltip title={tooltip} placement="top" enterTouchDelay={0}>
        <Box
            component="span"
            sx={{
                textDecoration: "underline dotted",
                cursor: "help",
            }}
        >
            {label}
        </Box>
    </Tooltip>
);

const localAirlineLogoMap = {
    AA: "logos/AA.png",
    DL: "logos/Delta.png",
    B6: "logos/Jetblue.png",
    UA: "logos/United.png",
    "american airlines": "logos/AA.png",
    "delta air lines": "logos/Delta.png",
    "delta airlines": "logos/Delta.png",
    delta: "logos/Delta.png",
    "jetblue airways": "logos/Jetblue.png",
    jetblue: "logos/Jetblue.png",
    "united airlines": "logos/United.png",
    united: "logos/United.png",
};

const normalizeLogoKey = (value) => (value || "").trim().toLowerCase();
const normalizeAirline = (value) => (value || "").trim().toLowerCase();

const ScoreChip = ({ label, tooltip, color = "default" }) => (
    <Tooltip title={tooltip} placement="top" enterTouchDelay={0}>
        <Chip
            label={label}
            color={color}
            size="medium"
            sx={{ cursor: "help" }}
        />
    </Tooltip>
);

const collectAirlineIdentifierCandidates = (airline) => {
    const candidates = [];
    const pushCandidate = (value) => {
        if (typeof value === "string" && value.trim()) {
            candidates.push(value.trim());
        }
    };
    if (!airline) {
        return candidates;
    }
    if (typeof airline === "string") {
        pushCandidate(airline);
        return candidates;
    }
    if (typeof airline === "object") {
        pushCandidate(airline.iata);
        pushCandidate(airline.icao);
        pushCandidate(airline.name);
        pushCandidate(airline.airline);
        pushCandidate(airline.normalized);
        pushCandidate(airline.slug);
        if (Array.isArray(airline.aliases)) {
            airline.aliases.forEach(pushCandidate);
        }
        if (Array.isArray(airline.alternate_names)) {
            airline.alternate_names.forEach(pushCandidate);
        }
    }
    const seen = new Set();
    return candidates.filter((candidate) => {
        const normalized = normalizeLogoKey(candidate);
        if (seen.has(normalized)) {
            return false;
        }
        seen.add(normalized);
        return true;
    });
};

const getAirlineLogoSrc = (airline) => {
    if (!airline) {
        return null;
    }
    const candidates = collectAirlineIdentifierCandidates(airline);

    for (const candidate of candidates) {
        const exactMatch = localAirlineLogoMap[candidate];
        if (exactMatch) {
            return exactMatch;
        }
        const normalized = normalizeLogoKey(candidate);
        if (localAirlineLogoMap[normalized]) {
            return localAirlineLogoMap[normalized];
        }
        const upper = candidate.toUpperCase();
        if (localAirlineLogoMap[upper]) {
            return localAirlineLogoMap[upper];
        }
    }
    return null;
};

const AirlineLogo = ({ airline, src: explicitSrc, name, size = 48 }) => {
    const fallbackSrc = React.useMemo(() => explicitSrc || getAirlineLogoSrc(airline), [explicitSrc, airline]);
    const [logoSrc, setLogoSrc] = React.useState(fallbackSrc);
    const displayName =
        name || (typeof airline === "string" ? airline : airline && (airline.name || airline.airline)) || "Airline";

    React.useEffect(() => {
        setLogoSrc(fallbackSrc);
    }, [fallbackSrc]);

    const handleError = React.useCallback(() => {
        // Hide the image if both the requested and fallback logos fail
        if (logoSrc && fallbackSrc && logoSrc !== fallbackSrc) {
            setLogoSrc(fallbackSrc);
            return;
        }
        setLogoSrc(null);
    }, [logoSrc, fallbackSrc]);

    if (!logoSrc) {
        return null;
    }
    return (
        <Box
            component="img"
            src={logoSrc}
            alt={`${displayName} logo`}
            sx={{
                width: size,
                height: size,
                borderRadius: 1,
                backgroundColor: "rgba(255,255,255,0.04)",
                border: "1px solid rgba(255,255,255,0.08)",
                objectFit: "contain",
                p: 0.5,
            }}
            onError={handleError}
        />
    );
};

const ScorecardView = ({ data }) => {
    if (!data) {
        return null;
    }
    const competitionEntries = Object.entries(data.competition || {}).filter(([, value]) => typeof value === "number");
    const maturityEntries = Object.entries(data.maturity || {}).filter(([, value]) => typeof value === "number");
    const yieldStats = data.yield || {};
    const yieldEntries = Object.entries(yieldStats).filter(([, value]) => typeof value === "number" && Number.isFinite(value));
    const hasContent = competitionEntries.length || maturityEntries.length || yieldEntries.length;
    if (!hasContent) {
        return null;
    }

    return (
        <Box sx={{ mt: 2 }}>
            <Typography variant="subtitle2" color="text.secondary">
                <InfoHint
                    label="Route Scorecard"
                    tooltip="Competition bands: Monopoly 1.0, Duopoly 0.55, Oligopoly (3-4) 0.35, Multi-carrier 5+ 0.2. Maturity: Established >=75th percentile ASM, Maturing 40-74th, Emerging <40th. Yield Proxy inverts SPM percentile (lower density -> higher score)."
                />
            </Typography>
            <Grid container spacing={2}>
                {competitionEntries.length > 0 && (
                    <Grid item xs={12} md={6}>
                        <Typography variant="caption" color="text.secondary">
                            <InfoHint
                                label="Competition Mix"
                                tooltip="Share of ASM by competition level (Monopoly, Duopoly, Oligopoly, Multi-carrier) based on unique carriers on the city pair."
                            />
                        </Typography>
                        <Stack direction="row" flexWrap="wrap" gap={1} sx={{ mt: 0.5 }}>
                            {competitionEntries.map(([label, value]) => (
                                <Chip key={label} label={`${label}: ${formatPercent(value)}`} size="small" variant="outlined" />
                            ))}
                        </Stack>
                    </Grid>
                )}
                {maturityEntries.length > 0 && (
                    <Grid item xs={12} md={6}>
                        <Typography variant="caption" color="text.secondary">
                            <InfoHint
                                label="Network Maturity"
                                tooltip="Percentile rank of ASM within the airline: Established >=75th percentile, Maturing 40-74th, Emerging <40th."
                            />
                        </Typography>
                        <Stack direction="row" flexWrap="wrap" gap={1} sx={{ mt: 0.5 }}>
                            {maturityEntries.map(([label, value]) => (
                                <Chip key={label} label={`${label}: ${formatPercent(value)}`} size="small" variant="outlined" />
                            ))}
                        </Stack>
                    </Grid>
                )}
                {yieldEntries.length > 0 && (
                    <Grid item xs={12}>
                        <Typography variant="caption" color="text.secondary">
                            <InfoHint
                                label="Yield Proxy"
                                tooltip="Inverted percentile of Seats per Mile (SPM) within the airline; lower seat density pushes the score higher (premium)."
                            />
                        </Typography>
                        <Stack direction="row" flexWrap="wrap" gap={1} sx={{ mt: 0.5 }}>
                            {yieldEntries.map(([label, value]) => (
                                <Chip key={label} label={`${label.toUpperCase()}: ${value.toFixed(2)}`} size="small" variant="outlined" />
                            ))}
                        </Stack>
                    </Grid>
                )}
            </Grid>
        </Box>
    );
};

const MarketShareList = ({ rows, maxRows = 5 }) => {
    if (!rows || !rows.length) {
        return null;
    }
    const topRows = rows.slice(0, maxRows);

    return (
        <Box sx={{ mt: 2 }}>
            <Typography variant="subtitle2" color="text.secondary">
                Top O&D Market Share
            </Typography>
            <Table size="small" sx={{ mt: 1 }}>
                <TableHead>
                    <TableRow>
                        <TableCell>Route</TableCell>
                        <TableCell align="right">Airline Share</TableCell>
                        <TableCell>Competition</TableCell>
                    </TableRow>
                </TableHead>
                <TableBody>
                    {topRows.map((row, index) => (
                        <TableRow key={`${row.Source}-${row.Destination}-${index}`}>
                            <TableCell>{`${row.Source || "?"} -> ${row.Destination || "?"}`}</TableCell>
                            <TableCell align="right">{formatPercent(row["Market Share"])}</TableCell>
                            <TableCell>{row["Competition Level"] || "-"}</TableCell>
                        </TableRow>
                    ))}
                </TableBody>
            </Table>
        </Box>
    );
};

const FleetUtilizationList = ({ rows, maxRows = 5 }) => {
    if (!rows || !rows.length) {
        return null;
    }
    const subset = rows.slice(0, maxRows);
    return (
        <Box sx={{ mt: 2 }}>
            <Typography variant="subtitle2" color="text.secondary">
                <InfoHint
                    label="Fleet Utilization Snapshot"
                    tooltip="Utilization Score blends 60% ASM share and 40% route-count share across equipment (multi-equipment routes split evenly)."
                />
            </Typography>
            <Table size="small" sx={{ mt: 1 }}>
                <TableHead>
                    <TableRow>
                        <TableCell>Equipment</TableCell>
                        <TableCell align="right">Routes</TableCell>
                        <TableCell align="right">Avg Dist (mi)</TableCell>
                        <TableCell align="right">Utilization</TableCell>
                    </TableRow>
                </TableHead>
                <TableBody>
                    {subset.map((row, index) => (
                        <TableRow key={`${row.Equipment}-${index}`}>
                            <TableCell>{row.Equipment || "Unknown"}</TableCell>
                            <TableCell align="right">{formatValue(row["Route Count"])}</TableCell>
                            <TableCell align="right">{formatValue(row["Average Distance"])}</TableCell>
                            <TableCell align="right">{formatPercent(row["Utilization Score"])}</TableCell>
                        </TableRow>
                    ))}
                </TableBody>
            </Table>
        </Box>
    );
};

const StatusAlert = ({ status }) => {
    if (!status.message) {
        return null;
    }
    let severity = "info";
    if (status.kind === "error") {
        severity = "error";
    } else if (status.kind === "success") {
        severity = "success";
    }
    return (
        <Alert severity={severity} sx={{ mb: 2 }}>
            {status.message}
        </Alert>
    );
};

const DataTable = ({ rows, title, maxHeight, enableWrapping = false, disableMargin = false }) => {
    if (!rows || !rows.length) {
        return (
            <Paper variant="outlined" sx={{ p: 2, mb: disableMargin ? 0 : 3 }}>
                <Typography color="text.secondary">No {title.toLowerCase()} available.</Typography>
            </Paper>
        );
    }

    const headers = Object.keys(rows[0]);
    const headerCellSx = enableWrapping ? { whiteSpace: "normal" } : undefined;
    const bodyCellSx = enableWrapping ? { whiteSpace: "normal", wordBreak: "break-word" } : undefined;

    return (
        <Paper
            variant="outlined"
            sx={{
                mb: disableMargin ? 0 : 3,
                overflowX: "auto",
            }}
        >
            <TableContainer sx={maxHeight ? { maxHeight } : undefined}>
                <Table size="small" stickyHeader={Boolean(maxHeight)}>
                    <TableHead>
                        <TableRow>
                            {headers.map((header) => (
                                <TableCell key={header} sx={headerCellSx}>
                                    {header}
                                </TableCell>
                            ))}
                        </TableRow>
                    </TableHead>
                    <TableBody>
                        {rows.map((row, rowIndex) => (
                            <TableRow key={rowIndex}>
                                {headers.map((header) => (
                                    <TableCell key={`${rowIndex}-${header}`} sx={bodyCellSx}>
                                        {formatValue(row[header])}
                                    </TableCell>
                                ))}
                            </TableRow>
                        ))}
                    </TableBody>
                </Table>
            </TableContainer>
        </Paper>
    );
};

const buildEmbeddedInsight = (result) => {
    if (!result) {
        return null;
    }
    const airline = normalizeAirline(result.airline);
    const src = (result.source || "").toUpperCase();
    const dst = (result.destination || "").toUpperCase();
    const isAmerican = AMERICAN_ALIASES.some((alias) => airline.includes(alias));
    const isJfkMia = (src === JFK && dst === MIA) || (src === MIA && dst === JFK);
    if (!isAmerican || !isJfkMia) {
        return null;
    }
    return [
        "Hub alignment: JFK gateway to TATL and premium domestic; MIA is the cornerstone AA hub, so keep it hub-to-hub with strong feed both directions.",
        "Market depth: Miami-New York is a global top-30 O&D; resource: resources/text/TOP 30 O&Ds World ASK.txt lists AA at ~27% capacity share.",
        "Load-factor bar: peer legacy LFs sit ~84-86% (resources/airline_operational_metrics.json); target >=85% here, trimming shoulder banks if needed.",
        "Network posture: multi-competitor trunk; lean on schedule/product versus monopoly-style yield (context from resources/text/29Network Aggressiveness - Website.txt).",
        "Maturity: stable, high-frequency trunk (see resources/text/30Network Maturity Q2 2024 - Website.txt); adjust timing not experimentation.",
    ];
};

const buildGeneralProposalNote = (result) => {
    if (!result) {
        return null;
    }
    const airlineName = (result.airline || "").trim();
    const normalizedAirline = normalizeAirline(airlineName);
    const lf = LOAD_FACTOR_BENCHMARKS[normalizedAirline];
    const lfText = lf ? `Target load factor: >= ${percentFormatter.format(Math.max(lf, 0.85))} (peer current ${percentFormatter.format(lf)} per resources/airline_operational_metrics.json)` : "Target load factor: >= 85% (peer legacy benchmarks from resources/airline_operational_metrics.json).";
    return [
        `Hub fit: confirm ${result.source} and ${result.destination} align to the carrier's hubs/focus cities; prioritize hub-to-hub or hub-to-spoke unless the airline runs a fluid network.`,
        "CBSA analog demand: match origin/destination CBSAs to similar CBSAs already served by the airline; use population/GDP peers to size PDEW and fares.",
        lfText,
        "Competition posture: label the route as monopoly/duopoly/multi-competitor and align with the airline's appetite (see resources/text/29Network Aggressiveness - Website.txt).",
        "Network maturity: stable trunks favor timing tweaks; fluid networks can trial new dayparts (see resources/text/30Network Maturity Q2 2024 - Website.txt).",
        "Product/fleet fit: stage length vs in-fleet types; adjust bank timing to preserve connectivity and avoid over-scheduling low-yield dayparts.",
    ];
};

const NetworkSummary = ({ airlines }) => {
    if (!airlines || !airlines.length) {
        return null;
    }

    return (
        <Stack spacing={2} sx={{ mb: 2 }}>
            <Typography variant="h5">Network Summary</Typography>
            <Grid container spacing={2}>
                {airlines.map((airline, index) => {
                    const displayName = airline.name || airline.airline || "Airline";
                    return (
                        <Grid item xs={12} md={6} key={airline.name || airline.iata || String(index)}>
                            <Card
                                variant="outlined"
                                sx={{
                                    height: "100%",
                                    borderColor: "rgba(255,255,255,0.08)",
                                    backgroundColor: "rgba(255,255,255,0.03)",
                                }}
                            >
                                <CardHeader
                                    avatar={<AirlineLogo airline={airline} name={displayName} />}
                                    title={displayName}
                                    subheader={airline.iata ? `IATA: ${airline.iata}` : null}
                                />
                                <CardContent>
                                    <Stack component="ul" spacing={1} sx={{ listStyle: "none", p: 0, m: 0 }}>
                                        {Object.entries(airline.network_stats || {}).map(([key, value]) => (
                                            <Box
                                                key={`${displayName}-${key}`}
                                                component="li"
                                                sx={{ color: "text.secondary", fontSize: "0.95rem" }}
                                            >
                                                <strong>{key}:</strong> {formatNetworkStat(key, value)}
                                            </Box>
                                        ))}
                                    </Stack>
                                    <ScorecardView data={airline.scorecard} />
                                    <MarketShareList rows={airline.market_share} />
                                    <FleetUtilizationList rows={airline.fleet_utilization} />
                                </CardContent>
                            </Card>
                        </Grid>
                    );
                })}
            </Grid>
        </Stack>
    );
};

const FleetProfile = ({ profile }) => {
    if (!profile) {
        return null;
    }
    const airline = profile.airline || {};
    const networkStats = profile.network_stats || {};
    const quickFacts = [
        { label: "IATA", value: airline.iata },
        { label: "ICAO", value: airline.icao },
        { label: "Country", value: airline.country },
        { label: "Total Routes", value: airline.total_routes ? integerNumberFormatter.format(airline.total_routes) : null },
        { label: "Status", value: airline.active },
        { label: "Callsign", value: airline.callsign },
    ].filter((item) => item.value);

    const networkEntries = Object.entries(networkStats).filter(([, value]) => value !== undefined && value !== null);

    return (
        <Stack spacing={3}>
            <Paper
                variant="outlined"
                sx={{
                    p: { xs: 2.5, md: 3 },
                    border: "1px solid rgba(255,255,255,0.08)",
                    boxShadow: "0 30px 60px rgba(0,0,0,0.35)",
                }}
            >
                <Stack spacing={3}>
                    <Stack direction={{ xs: "column", sm: "row" }} spacing={2} alignItems={{ xs: "flex-start", sm: "center" }}>
                        <AirlineLogo airline={airline} size={72} name={airline.name} />
                        <Box>
                            <Typography variant="h4" fontWeight={700}>
                                {airline.name || "Airline"}
                            </Typography>
                            {airline.alias && (
                                <Typography variant="body2" color="text.secondary">
                                    Also known as {airline.alias}
                                </Typography>
                            )}
                        </Box>
                    </Stack>
                    {quickFacts.length > 0 && (
                        <Grid container spacing={2}>
                            {quickFacts.map((fact) => (
                                <Grid item xs={6} sm={4} md={3} key={fact.label}>
                                    <Typography variant="caption" color="text.secondary">
                                        {fact.label}
                                    </Typography>
                                    <Typography variant="body1">{fact.value}</Typography>
                                </Grid>
                            ))}
                        </Grid>
                    )}
                    {networkEntries.length > 0 && (
                        <Box>
                            <Typography variant="subtitle2" color="text.secondary" sx={{ mb: 1 }}>
                                Network Stats
                            </Typography>
                            <Stack component="ul" spacing={1} sx={{ listStyle: "none", p: 0, m: 0 }}>
                                {networkEntries.map(([key, value]) => (
                                    <Box component="li" key={key} sx={{ color: "text.secondary" }}>
                                        <strong>{key}:</strong> {formatNetworkStat(key, value)}
                                    </Box>
                                ))}
                            </Stack>
                        </Box>
                    )}
                </Stack>
            </Paper>

            <Paper
                variant="outlined"
                sx={{
                    p: { xs: 2.5, md: 3 },
                    border: "1px solid rgba(255,255,255,0.08)",
                }}
            >
                <Typography variant="h6" sx={{ mb: 2 }}>
                    Network Context
                </Typography>
                <ScorecardView data={profile.scorecard} />
                <MarketShareList rows={profile.market_share} maxRows={8} />
                <FleetUtilizationList rows={profile.fleet_utilization} maxRows={8} />
            </Paper>

            <Box>
                <Typography variant="h6" sx={{ mb: 1 }}>
                    Fleet Utilization Detail
                </Typography>
                <DataTable rows={profile.fleet_utilization} title="Fleet Utilization" disableMargin enableWrapping />
            </Box>

            <Box>
                <Typography variant="h6" sx={{ mb: 1 }}>
                    Top Routes by ASM
                </Typography>
                <DataTable rows={profile.top_routes} title="Top Routes" disableMargin enableWrapping maxHeight={420} />
            </Box>

            <Box>
                <Typography variant="h6" sx={{ mb: 1 }}>
                    ASM Source Snapshot
                </Typography>
                <DataTable rows={profile.asm_sources} title="ASM Snapshot" disableMargin enableWrapping />
            </Box>
        </Stack>
    );
};

const FleetAssignmentResults = ({ result }) => {
    if (!result) {
        return null;
    }
    const summary = result.summary || {};
    const assignments = result.assignments || [];
    const tailLogs = result.tail_logs || [];
    const unassigned = result.unassigned || [];
    const fleetOverview = result.fleet_overview || [];
    const summaryChips = [
        { label: "Flights Scheduled", value: `${summary.scheduled_flights || 0} / ${summary.total_flights || 0}` },
        { label: "Coverage", value: formatPercent(summary.coverage || 0) },
        { label: "Utilization", value: formatPercent(summary.utilization || 0) },
        { label: "Block Hours", value: summary.total_block_hours ? `${summary.total_block_hours.toFixed(1)} h` : "0" },
        { label: "Unassigned", value: summary.unassigned || 0 },
    ];

    const assignmentRows = assignments.map((entry) => ({
        Route: entry.route || `${entry.source || "?"} -> ${entry.destination || "?"}`,
        Tail: entry.tail_id || "-",
        Equipment: entry.assigned_equipment || "-",
        "Requested Equip": entry.equipment_requested || "-",
        "Start": entry.start_label || formatValue(entry.start_hour),
        "End": entry.end_label || formatValue(entry.end_hour),
        "Block Hours": entry.block_hours,
        "Turn Hours": entry.turn_hours,
        "Distance (mi)": entry.distance_miles,
        "Seats Planned": entry.required_seats,
    }));

    const tailRows = tailLogs.map((tail) => ({
        Tail: tail.tail_id,
        Equipment: tail.equipment,
        Category: tail.category,
        Flights: tail.flights,
        "Block Hours": tail.block_hours,
        "Duty Hours": tail.duty_hours,
        Utilization: tail.utilization,
        "Maintenance Buffer": tail.maintenance_buffer,
    }));

    const unassignedRows = unassigned.map((item) => ({
        Route: item.route,
        Equipment: item.equipment,
        "Distance (mi)": item.distance_miles,
        "Seats Needed": item.required_seats,
        Reason: item.reason,
    }));

    const fleetRows = fleetOverview.map((entry) => ({
        Equipment: entry.equipment,
        Count: entry.count,
        "Seat Capacity": entry.seat_capacity,
        Category: entry.category,
    }));

    return (
        <Paper
            variant="outlined"
            sx={{
                p: { xs: 2.5, md: 3 },
                border: "1px solid rgba(255,255,255,0.08)",
                boxShadow: "0 30px 60px rgba(0,0,0,0.35)",
            }}
        >
            <Stack spacing={3}>
                <Box>
                    <Typography variant="h5">Live Fleet Assignment Results</Typography>
                    <Typography variant="body2" color="text.secondary">
                        Greedy assignment honors block time, turn buffers, daily crew limits, and maintenance windows.
                    </Typography>
                </Box>
                <Stack direction="row" flexWrap="wrap" gap={1}>
                    {summaryChips.map((chip) => (
                        <Chip key={chip.label} label={`${chip.label}: ${chip.value}`} color="primary" variant="outlined" />
                    ))}
                </Stack>
                {fleetRows.length > 0 && (
                    <Box>
                        <Typography variant="subtitle2" color="text.secondary" sx={{ mb: 1 }}>
                            Fleet Inputs
                        </Typography>
                        <DataTable rows={fleetRows} title="Fleet Overview" disableMargin />
                    </Box>
                )}
                <Box>
                    <Typography variant="h6" sx={{ mb: 1 }}>
                        Feasible Assignments
                    </Typography>
                    <DataTable rows={assignmentRows} title="Assignments" disableMargin enableWrapping maxHeight={420} />
                </Box>
                <Box>
                    <Typography variant="h6" sx={{ mb: 1 }}>
                        Hours Flown per Tail
                    </Typography>
                    <DataTable rows={tailRows} title="Tail Utilization" disableMargin enableWrapping />
                </Box>
                {unassignedRows.length > 0 && (
                    <Box>
                        <Typography variant="h6" sx={{ mb: 1 }}>
                            Unassigned Routes
                        </Typography>
                        <DataTable rows={unassignedRows} title="Unassigned Routes" disableMargin enableWrapping />
                    </Box>
                )}
            </Stack>
        </Paper>
    );
};

const CbsaOpportunities = ({ entries }) => {
    if (!entries || !entries.length) {
        return null;
    }

    return (
        <Stack spacing={3}>
            <Box>
                <Typography variant="h5">CBSA Opportunities</Typography>
                <Typography variant="body2" color="text.secondary">
                    Review each airline&apos;s strongest CBSA corridors alongside network-aligned potential routes.
                </Typography>
            </Box>
            {entries.map((entry, index) => {
                const bestRoutes = entry.best_routes || [];
                const potentialRoutes = entry.suggestions || [];
                const entryName = entry.airline || "Airline";
                return (
                    <Paper
                        key={`${entry.airline}-${index}`}
                        variant="outlined"
                        sx={{
                            p: { xs: 2.5, md: 3 },
                            borderColor: "rgba(255,255,255,0.08)",
                            backgroundColor: "rgba(8,15,33,0.9)",
                            boxShadow: "inset 0 0 0 1px rgba(255,255,255,0.02)",
                        }}
                    >
                        <Stack spacing={2.5}>
                            <Box>
                                <Stack direction="row" spacing={2} alignItems="center">
                                    <AirlineLogo airline={entry} name={entryName} size={56} />
                                    <Box>
                                        <Typography variant="h6" sx={{ fontWeight: 600 }}>
                                            {entryName}
                                        </Typography>
                                        <Typography variant="body2" color="text.secondary">
                                            Based on the airline&apos;s active U.S. network and CBSA coverage.
                                        </Typography>
                                    </Box>
                                </Stack>
                            </Box>
                            <Grid container spacing={2.5}>
                                <Grid item xs={12}>
                                    <ScorecardView data={entry.scorecard} />
                                    <MarketShareList rows={entry.market_share} />
                                    <FleetUtilizationList rows={entry.fleet_utilization} />
                                </Grid>
                                <Grid item xs={12} xl={6}>
                                    <Typography variant="subtitle2" color="text.secondary" sx={{ mb: 1 }}>
                                        Top CBSA Routes
                                    </Typography>
                                    <DataTable rows={bestRoutes} title="Top CBSA Routes" enableWrapping />
                                </Grid>
                                <Grid item xs={12} xl={6}>
                                    <Typography variant="subtitle2" color="text.secondary" sx={{ mb: 1 }}>
                                        Potential CBSA Routes
                                    </Typography>
                                    <DataTable rows={potentialRoutes} title="Potential CBSA Routes" enableWrapping />
                                </Grid>
                            </Grid>
                        </Stack>
                    </Paper>
                );
            })}
        </Stack>
    );
};

const RouteShareResults = ({ routes }) => {
    if (!routes || !routes.length) {
        return (
            <Paper
                variant="outlined"
                sx={{
                    p: { xs: 2.5, md: 3 },
                    border: "1px solid rgba(255,255,255,0.08)",
                }}
            >
                <Typography color="text.secondary">
                    Add routes on the left, then run the analysis to view airline share across each city pair.
                </Typography>
            </Paper>
        );
    }

    return (
        <Stack spacing={3}>
            {routes.map((route, index) => {
                const key = `${route.source || "?"}-${route.destination || "?"}-${index}`;
                const title = `${route.source || "?"} -> ${route.destination || "?"}`;
                const summaryChips = [
                    {
                        label: "Distance",
                        value: route.distance_miles ? `${integerNumberFormatter.format(route.distance_miles)} mi` : "-",
                    },
                    {
                        label: "Market ASM",
                        value: route.market_asm ? `${integerNumberFormatter.format(route.market_asm)} ASM` : "-",
                    },
                    {
                        label: "Competitors",
                        value: route.competitor_count != null ? integerNumberFormatter.format(route.competitor_count) : "-",
                    },
                    { label: "Competition", value: route.competition_level || "-" },
                    { label: "Maturity", value: route.route_maturity_label || "-" },
                    {
                        label: "Yield Proxy",
                        value:
                            route.yield_proxy_score != null
                                ? decimalNumberFormatter.format(route.yield_proxy_score)
                                : "-",
                    },
                ];
                const tableRows = (route.airlines || []).map((entry) => ({
                    Airline: entry.airline || entry.airline_normalized || "Airline",
                    ASM: entry.asm,
                    "Market Share":
                        typeof entry.market_share === "number" ? formatPercent(entry.market_share) : "-",
                    Seats: entry.seats,
                    "Seats / Mile": entry.seats_per_mile,
                    Equipment: entry.equipment && entry.equipment.length ? entry.equipment.join(", ") : "-",
                    "Strategy Score": entry.route_strategy_baseline,
                    "Yield Proxy": entry.yield_proxy_score,
                    "Route Maturity": entry.route_maturity_label || route.route_maturity_label || "-",
                }));

                return (
                    <Paper
                        key={key}
                        variant="outlined"
                        sx={{
                            p: { xs: 2.5, md: 3 },
                            border: "1px solid rgba(255,255,255,0.08)",
                            boxShadow: "0 30px 60px rgba(0,0,0,0.35)",
                        }}
                    >
                        <Stack spacing={2.5}>
                            <Box>
                                <Typography variant="h6">{title}</Typography>
                                <Typography variant="body2" color="text.secondary">
                                    {route.status === "ok"
                                        ? "Top airlines ordered by ASM contribution."
                                        : "No published schedules found for this city pair."}
                                </Typography>
                            </Box>
                            <Stack direction="row" flexWrap="wrap" gap={1}>
                                {summaryChips.map((chip) => (
                                    <Chip
                                        key={`${title}-${chip.label}`}
                                        label={`${chip.label}: ${chip.value}`}
                                        color="primary"
                                        variant="outlined"
                                    />
                                ))}
                            </Stack>
                            {route.status !== "ok" && (
                                <Alert severity="warning">No schedules found for this route pair.</Alert>
                            )}
                            <DataTable rows={tableRows} title="Airline Share" enableWrapping />
                        </Stack>
                    </Paper>
                );
            })}
        </Stack>
    );
};

const MetricsGuide = () => {
    const sections = [
        {
            title: "Route fundamentals",
            points: [
                "ASM (Available Seat Miles) = Total Seats x Distance (miles); marked valid only when both are > 0.",
                "Seats per Mile (SPM) captures seat density (Total Seats / Distance).",
                "Route Strategy Baseline = 0.5 * ASM share on that O&D + 0.3 * seat-density uplift vs. airline median + 0.2 * distance alignment to the airline median stage length. Clipped to [0,1].",
            ],
        },
        {
            title: "Competition, maturity, yield",
            points: [
                "Competition levels: Monopoly (score 1.0), Duopoly (0.6), Competitive (0.2) based on unique carriers on the city pair.",
                "Route Maturity (percentile bands): percentile rank of ASM within the airline. Labels: Established (>=75th pct), Maturing (40-74th), Emerging (<40th). Score is the percentile in [0,1].",
                "Yield Proxy (percentile bands): percentile rank of Seats per Mile within the airline, inverted: yield score = 1 - percentile(SPM). Lower seat density -> higher score.",
            ],
        },
        {
            title: "Dashboards & share",
            points: [
                "Route Scorecard: ASM-weighted share by Competition Level and Route Maturity Label; Yield Proxy percentiles (p25/p50/p75).",
                "Market Share Snapshot: Market ASM from the global routes DB; Market Share = Airline ASM / Market ASM for the pair.",
                "ASM Source quality: per seat source (airline_config, equipment_estimate, unknown) show routes, valid ASM routes, total seats/ASM, ASM share; warnings when estimates or unknown dominate.",
            ],
        },
        {
            title: "CBSA scoring (US-only)",
            points: [
                "Performance Score = 0.7 * normalized ASM + 0.3 * normalized Seats per Mile within CBSA-filtered routes.",
                "ASM Share (CBSA view): route ASM / total ASM of CBSA-eligible set.",
                "Opportunity Score = reference Performance Score x (0.5 + 0.5 x distance similarity to the reference CBSA route).",
            ],
        },
        {
            title: "Fleet & assignment",
            points: [
                "Fleet Utilization Score: split ASM across multi-equipment routes, sum per equipment, normalize by total ASM across all equipment.",
                "Optimal Aircraft (single route): weights utilization, distance fit, seat fit, and airline load factor to rank in-fleet types (see fleet card).",
                "Live Fleet Assignment: Coverage = scheduled / sampled flights; Utilization = flown block hours / (tails x crew_max_hours); tail-level utilization and maintenance buffers shown per tail.",
            ],
        },
    ];

    return (
        <Grid container spacing={3}>
            {sections.map((section) => (
                <Grid item xs={12} md={6} key={section.title}>
                    <Paper
                        variant="outlined"
                        sx={{
                            p: { xs: 2.5, md: 3 },
                            height: "100%",
                            border: "1px solid rgba(255,255,255,0.08)",
                            backgroundColor: "rgba(255,255,255,0.03)",
                        }}
                    >
                        <Typography variant="h6" sx={{ mb: 1 }}>
                            {section.title}
                        </Typography>
                        <Stack component="ul" spacing={1.2} sx={{ listStyle: "none", p: 0, m: 0 }}>
                            {section.points.map((point, idx) => (
                                <Box key={idx} component="li" sx={{ color: "text.secondary", lineHeight: 1.45 }}>
                                    {point}
                                </Box>
                            ))}
                        </Stack>
                    </Paper>
                </Grid>
            ))}
        </Grid>
    );
};

const IndustryTrends = () => {
    const sections = [
        {
            title: "Network strategy & maturity",
            points: [
                "Network Strategy Matrix (since COVID & Q2 2023-Q2 2024): Freeze, Shift, Entrenchment, Expansion based on seat growth vs. share of seats on new routes.",
                "Maturity signals (Network Maturity Q2 2024): stable networks keep most routes >3 years; fluid networks have many routes <12 months.",
                "Aggressiveness vs. competition: share of capacity on monopoly/duopoly/multi-competitor routes by fleet type; higher monopoly share = defensive moat, higher multi-competitor share = aggressive stance.",
            ],
        },
        {
            title: "Pricing & revenue",
            points: [
                "Ticket revenue trends: post-COVID fares rose globally; LCCs captured the largest uplift. Few exceptions lowered fares (flyadeal, flynas).",
                "Market share evolution: capacity growth + fare strategy drive passenger/revenue share. Over-aggressive fare hikes (e.g., Spirit, American) can erode share; balanced growth with moderated fares (Frontier, Southwest, United) holds or gains share.",
            ],
        },
        {
            title: "Fleet utilization & deployment",
            points: [
                "Fleet Utilization (Q2 2024): cycles per aircraft over 12 months; benchmark narrowbody types and lessor portfolios. Combine cycles with stage length to spot over/under-use.",
                "Aggressiveness by fleet type: examples show dispersion-e.g., Lufthansa A321neo ~37% monopoly vs. Vistara A321neo ~95% competitive; Ethiopian 737-8 mostly monopoly vs. Akasa fully competitive.",
            ],
        },
        {
            title: "Route performance & demand",
            points: [
                "Top O&Ds by ASK (2023): London-NYC, LAX-NYC, Dubai-London, London-Singapore, etc. remain the heaviest corridors; largest operators control ~20-80% of seats.",
                "Ryanair vs. easyJet (Europe): Ryanair is the disruptive growth leader; easyJet/Vueling/Transavia often sit in Network Freeze/Shift bands with lower new-route velocity.",
            ],
        },
        {
            title: "Regional snapshots",
            points: [
                "Europe: Iberia leads margins/OTP; Lufthansa/Air France lean on Eurowings/Transavia for network shifts; Wizz/Transavia France/Volotea push aggressive expansion.",
                "Middle East: Emirates sets the pace; Qatar fastest post-COVID capacity ramp with overcapacity risk; Turkish uses narrow + widebody mix for flexibility; Saudia balances flyadeal vs. flynas.",
                "North America: United grew capacity fastest among majors, holding share; Southwest gained passenger share with lower fares; ULCC fare hikes can backfire on share.",
                "Asia: Recovery uneven-Fiji/ATR fleets fluid; AirAsia still behind pre-COVID network; India LCCs kept fare increases modest.",
            ],
        },
        {
            title: "How to use in the app",
            points: [
                "When comparing airlines, overlay competition and maturity labels with these benchmarks to explain if a network is fluid or entrenched relative to peers.",
                "Use equipment competition mix to justify fleet assignments (avoid over-exposing a type to highly competitive markets unless yield supports it).",
                "Stress-test yield proxies against ticket-revenue trends: high yield scores are more credible where fares grew with discipline.",
                "Flag CBSA or route opportunities more strongly when they align with regions showing share gains and dial back when market share is contracting.",
            ],
        },
    ];

    return (
        <Grid container spacing={3}>
            {sections.map((section) => (
                <Grid item xs={12} md={6} key={section.title}>
                    <Paper
                        variant="outlined"
                        sx={{
                            p: { xs: 2.5, md: 3 },
                            height: "100%",
                            border: "1px solid rgba(255,255,255,0.08)",
                            backgroundColor: "rgba(255,255,255,0.03)",
                        }}
                    >
                        <Typography variant="h6" sx={{ mb: 1 }}>
                            {section.title}
                        </Typography>
                        <Stack component="ul" spacing={1.2} sx={{ listStyle: "none", p: 0, m: 0 }}>
                            {section.points.map((point, idx) => (
                                <Box key={idx} component="li" sx={{ color: "text.secondary", lineHeight: 1.45 }}>
                                    {point}
                                </Box>
                            ))}
                        </Stack>
                    </Paper>
                </Grid>
            ))}
        </Grid>
    );
};

const MetricsPlaybook = () => {
    const bullets = [
        'Competition score (Monopoly 1.0, Duopoly 0.55, Oligopoly 0.35, Multi-carrier 0.2): High scores mean you hold more "moat" on that O&D. Lean into capacity or upgauge where scores are high; be cautious adding seats or lowering fares where scores are low (crowded routes). If a fleet type is concentrated in low scores, consider redeploying it.',
        "Maturity (Established >=75th pct ASM, Maturing 40-74th, Emerging <40th): Established routes are stable cash engines-optimize pricing and cost; Emerging routes need watchlists: test frequency, right-size equipment, and watch early performance. If an airline's network skews Emerging, expect volatility; if Established, it can absorb more price/margin.",
        "Yield Proxy (inverted SPM percentile): High score = lower seat density, more premium potential. Use it to decide where to defend price vs. where you must compete on cost. If competition is high but yield proxy is strong, you can still hold price; if both competition is high and yield proxy is weak, expect fare pressure.",
        'Route Strategy Baseline: Blends ASM share, seat-density uplift, and distance fit vs. the airline\'s median. Use it to sort "core" vs. "fringe" routes: high baseline = defend/optimize; low baseline = candidate for reduction or swap to another type.',
        "Performance Score (CBSA): 0.7 ASM_norm + 0.3 SPM_norm. High performers are anchor routes in a metro pair. Use them as references for CBSA-similar suggestions; if suggestions have decent similarity and opportunity scores, they're next to trial.",
        "Opportunity Score (CBSA suggestions): Reference performance x distance similarity. Use to prioritize new CBSA-aligned pairs; start with top scores, sanity-check ops/slots.",
        "Market Share snapshot: ASM and Market Share per top O&D. If your share is low but competition score is high (crowded), winning requires either price moves or a capacity play; if share is low and competition score is high but yield proxy is strong, try premium positioning.",
        "Fleet Utilization Score (60% ASM share + 40% route-count share): Highlights which types are carrying the network vs. underused. High score + high competition exposure? Maybe rebalance to defensible routes. Low score? Consider redeploying, parking, or pairing with Emerging routes to probe demand.",
        "Live Fleet Assignment: Coverage and utilization show whether your proposed fleet can actually fly the top routes under crew/maintenance constraints. Unassigned routes list why tails failed-use it to adjust counts, turn times, or mix.",
        "Optimal Aircraft (per route): Uses utilization, distance fit, seat fit, and load factor. Pick the top recommendations to maximize utilization without overshooting demand; if you see wide gaps between top and second choices, stick with #1; if close, rotate to spread cycles.",
        "Competition/Maturity tooltips in the UI: Hover in the results to see the bands and what they mean; same for fleet utilization weighting.",
    ];

    return (
        <Paper
            variant="outlined"
            sx={{
                p: { xs: 2.5, md: 3 },
                border: "1px solid rgba(255,255,255,0.08)",
                backgroundColor: "rgba(255,255,255,0.03)",
            }}
        >
            <Typography variant="h6" sx={{ mb: 1 }}>
                Metrics playbook
            </Typography>
            <Stack component="ul" spacing={1.2} sx={{ listStyle: "none", p: 0, m: 0 }}>
                {bullets.map((text, idx) => (
                    <Box key={idx} component="li" sx={{ color: "text.secondary", lineHeight: 1.5 }}>
                        {text}
                    </Box>
                ))}
            </Stack>
        </Paper>
    );
};

const PageIntro = ({ activePage }) => {
    const sharedStyles = {
        p: { xs: 2, md: 2.5 },
        border: "1px solid rgba(255,255,255,0.08)",
        backgroundColor: "rgba(255,255,255,0.03)",
    };

    if (activePage === "analysis") {
        return (
            <Paper variant="outlined" sx={sharedStyles}>
                <Typography variant="subtitle1" sx={{ fontWeight: 700 }}>
                    Quick start
                </Typography>
                <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                    Pick one or two airlines (search is fuzzy), keep the defaults, and hit <strong>Run Analysis</strong>. We will
                    show head-to-head routes and CBSA opportunities in the results panel on the right.
                </Typography>
                <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                    Tip: turn on "Skip comparison" if you only want CBSA results for a single carrier.
                </Typography>
            </Paper>
        );
    }

    if (activePage === "routes") {
        return (
            <Paper variant="outlined" sx={sharedStyles}>
                <Typography variant="subtitle1" sx={{ fontWeight: 700 }}>
                    How to use route share
                </Typography>
                <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                    Add airport pairs (IATA codes like JFK or LAX). We will list the top airlines on each route with their share,
                    seats, and maturity.
                </Typography>
            </Paper>
        );
    }

    if (activePage === "fleet") {
        return (
            <Paper variant="outlined" sx={sharedStyles}>
                <Typography variant="subtitle1" sx={{ fontWeight: 700 }}>
                    Fleet tools at a glance
                </Typography>
                <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                    Search an airline to see its fleet profile, then scroll down to simulate a duty-day schedule with your own
                    equipment mix.
                </Typography>
            </Paper>
        );
    }

    if (activePage === "metrics") {
        return (
            <Paper variant="outlined" sx={sharedStyles}>
                <Typography variant="subtitle1" sx={{ fontWeight: 700 }}>
                    Metric definitions
                </Typography>
                <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                    Every score shown in the app is documented below: what it measures, how it is calculated, and how to read it.
                </Typography>
                <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                    Use this when presenting results to anchor stakeholders on what "performance", "maturity", and "yield" mean here.
                </Typography>
            </Paper>
        );
    }

    if (activePage === "metrics") {
        return (
            <Paper variant="outlined" sx={sharedStyles}>
                <Typography variant="subtitle1" sx={{ fontWeight: 700 }}>
                    Metric definitions
                </Typography>
                <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                    Every score shown in the app is documented below: what it measures, how it is calculated, and how to read it.
                </Typography>
                <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                    Use this when presenting results to anchor stakeholders on what "performance", "maturity", and "yield" mean here.
                </Typography>
            </Paper>
        );
    }

    if (activePage === "trends") {
        return (
            <Paper variant="outlined" sx={sharedStyles}>
                <Typography variant="subtitle1" sx={{ fontWeight: 700 }}>
                    Industry trends
                </Typography>
                <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                    Condensed insights from all resource files (network strategy, aggressiveness, maturity, pricing, market share, fleet
                    utilization) to keep the analysis grounded in current market patterns.
                </Typography>
            </Paper>
        );
    }

    if (activePage === "proposal") {
        return (
            <Paper variant="outlined" sx={sharedStyles}>
                <Typography variant="subtitle1" sx={{ fontWeight: 700 }}>
                    Route proposal
                </Typography>
                <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                    Pick an airline, enter a new route, and we'll score it using competition, market depth, and distance fit.
                </Typography>
                <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                    Recommendations: good (score >= 0.65), watch (0.45-0.65), avoid (&lt; 0.45).
                </Typography>
            </Paper>
        );
    }

    return null;
};

function App() {
    const [formState, setFormState] = React.useState(defaultFormState);
    const [status, setStatus] = React.useState({ message: "", kind: "" });
    const [messages, setMessages] = React.useState([]);
    const [comparison, setComparison] = React.useState(null);
    const [cbsaResults, setCbsaResults] = React.useState([]);
    const [loading, setLoading] = React.useState(false);
    const [suggestions, setSuggestions] = React.useState([]);
    const [resultsTab, setResultsTab] = React.useState("competing");
    const debounceRef = React.useRef(null);
    const lastQueryRef = React.useRef("");
    const [optimalConfig, setOptimalConfig] = React.useState({
        airline: "",
        route_distance: "",
        seat_demand: "",
        top_n: "3",
    });
    const [optimalResults, setOptimalResults] = React.useState([]);
    const [optimalStatus, setOptimalStatus] = React.useState({ message: "", kind: "" });
    const [optimalLoading, setOptimalLoading] = React.useState(false);
    const [activePage, setActivePage] = React.useState("analysis");
    const [fleetQuery, setFleetQuery] = React.useState("");
    const [fleetStatus, setFleetStatus] = React.useState({ message: "", kind: "" });
    const [fleetLoading, setFleetLoading] = React.useState(false);
    const [fleetProfile, setFleetProfile] = React.useState(null);
    const [fleetAssignmentConfig, setFleetAssignmentConfig] = React.useState({
        airline: "",
        route_limit: "60",
        day_hours: "18",
        maintenance_hours: "6",
        crew_max_hours: "14",
        fleet: [{ equipment: "A320", count: "5" }],
    });
    const [fleetAssignmentStatus, setFleetAssignmentStatus] = React.useState({ message: "", kind: "" });
    const [fleetAssignmentLoading, setFleetAssignmentLoading] = React.useState(false);
    const [fleetAssignmentResults, setFleetAssignmentResults] = React.useState(null);
    const [routeShareRows, setRouteShareRows] = React.useState([{ source: "", destination: "" }]);
    const [routeShareTopN, setRouteShareTopN] = React.useState("5");
    const [routeShareIncludeAll, setRouteShareIncludeAll] = React.useState(true);
    const [routeShareStatus, setRouteShareStatus] = React.useState({ message: "", kind: "" });
    const [routeShareLoading, setRouteShareLoading] = React.useState(false);
    const [routeShareResults, setRouteShareResults] = React.useState([]);
    const [proposal, setProposal] = React.useState({ airline: "", source: "", destination: "", seat_demand: "" });
    const [proposalStatus, setProposalStatus] = React.useState({ message: "", kind: "" });
    const [proposalLoading, setProposalLoading] = React.useState(false);
    const [proposalResult, setProposalResult] = React.useState(null);
    const embeddedInsight = React.useMemo(() => buildEmbeddedInsight(proposalResult), [proposalResult]);
    const generalProposalNote = React.useMemo(() => buildGeneralProposalNote(proposalResult), [proposalResult]);
    const playbookNotes = React.useMemo(() => {
        const backendPlaybook = proposalResult?.playbook;
        if (backendPlaybook && backendPlaybook.length) {
            return backendPlaybook;
        }
        return generalProposalNote;
    }, [proposalResult, generalProposalNote]);

    const fetchSuggestions = React.useCallback(async (query = "") => {
        try {
            const url = query ? `${API_BASE}/airlines?query=${encodeURIComponent(query)}` : `${API_BASE}/airlines`;
            const response = await fetch(url);
            if (!response.ok) {
                return;
            }
            const data = await response.json();
            const next = data
                .map((entry) => entry.airline)
                .filter(Boolean);
            setSuggestions((prev) => {
                const merged = new Set([...(prev || []), ...next]);
                return Array.from(merged).sort();
            });
        } catch (error) {
            // Ignore suggestion errors to keep the form responsive.
        }
    }, []);

    React.useEffect(() => {
        fetchSuggestions();
        return () => {
            if (debounceRef.current) {
                clearTimeout(debounceRef.current);
            }
        };
    }, [fetchSuggestions]);

    const handleSuggestionQuery = React.useCallback(
        (value) => {
            const trimmed = (value || "").trim();
            if (debounceRef.current) {
                clearTimeout(debounceRef.current);
            }
            if (trimmed.length < 2 || trimmed === lastQueryRef.current) {
                return;
            }
            debounceRef.current = setTimeout(() => {
                fetchSuggestions(trimmed);
                lastQueryRef.current = trimmed;
            }, 250);
        },
        [fetchSuggestions]
    );

    const handleFieldChange = (field) => (event) => {
        const value = event.target.value;
        setFormState((prev) => ({ ...prev, [field]: value }));
    };

    const handleCheckboxChange = (field) => (_, checked) => {
        setFormState((prev) => ({ ...prev, [field]: checked }));
    };

    const handleOptimalFieldChange = (field) => (event) => {
        const value = event.target.value;
        setOptimalConfig((prev) => ({ ...prev, [field]: value }));
    };

    const resetResults = () => {
        setMessages([]);
        setComparison(null);
        setCbsaResults([]);
        setResultsTab("competing");
    };

    const handleSubmit = async (event) => {
        event.preventDefault();
        resetResults();
        setStatus({ message: "Running analysis...", kind: "info" });
        setLoading(true);

        const comparisonAirlines = formState.skip_comparison
            ? []
            : [formState.comparison_airline_1, formState.comparison_airline_2]
                  .map((entry) => entry.trim())
                  .filter(Boolean);

        const cbsaAirlines = (formState.cbsa_airlines || "")
            .split(/\r?\n|,/)
            .map((entry) => entry.trim())
            .filter(Boolean);

        const payload = {
            comparison_airlines: comparisonAirlines,
            skip_comparison: formState.skip_comparison,
            cbsa_airlines: cbsaAirlines,
            cbsa_top_n: Number(formState.cbsa_top_n) || 5,
            cbsa_suggestions: Number(formState.cbsa_suggestions) || 3,
            build_cbsa_cache: formState.build_cbsa_cache,
        };

        const countriesRaw = formState.cbsa_cache_country;
        if (countriesRaw && countriesRaw.trim()) {
            payload.cbsa_cache_country = countriesRaw
                .split(",")
                .map((country) => country.trim())
                .filter(Boolean);
        }

        if (formState.cbsa_cache_limit) {
            payload.cbsa_cache_limit = Number(formState.cbsa_cache_limit);
        }

        if (formState.cbsa_cache_chunk_size) {
            payload.cbsa_cache_chunk_size = Number(formState.cbsa_cache_chunk_size);
        }

        try {
            const response = await fetch(`${API_BASE}/run`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload),
            });

            const result = await response.json();
            if (!response.ok) {
                throw new Error(result.detail || "Request failed.");
            }

            setStatus({ message: "Analysis complete.", kind: "success" });
            setMessages(result.messages || []);
            setComparison(result.comparison || null);
            setCbsaResults(result.cbsa || []);
        } catch (error) {
            setStatus({ message: error.message || "Unable to complete the request.", kind: "error" });
        } finally {
            setLoading(false);
        }
    };

    const handleOptimalSubmit = React.useCallback(
        async (event) => {
            event.preventDefault();
            const airline = (optimalConfig.airline || "").trim();
            if (!airline) {
                setOptimalStatus({ message: "Airline name is required.", kind: "error" });
                return;
            }
            const routeDistance = Number(optimalConfig.route_distance);
            if (!Number.isFinite(routeDistance) || routeDistance <= 0) {
                setOptimalStatus({ message: "Enter a valid route distance above zero.", kind: "error" });
                return;
            }
            const payload = {
                airline,
                route_distance: routeDistance,
                top_n: Number.isFinite(Number(optimalConfig.top_n)) ? Math.max(1, Math.floor(Number(optimalConfig.top_n))) : 3,
            };
            const seatDemandRaw = optimalConfig.seat_demand;
            if (seatDemandRaw) {
                const seatDemand = Number(seatDemandRaw);
                if (Number.isFinite(seatDemand) && seatDemand > 0) {
                    payload.seat_demand = Math.floor(seatDemand);
                }
            }
            setOptimalLoading(true);
            setOptimalStatus({ message: "Finding optimal equipment...", kind: "info" });
            setOptimalResults([]);
            try {
                const response = await fetch(`${API_BASE}/optimal-aircraft`, {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify(payload),
                });
                const result = await response.json();
                if (!response.ok) {
                    throw new Error(result.detail || "Unable to fetch optimal equipment.");
                }
                setOptimalResults(result.optimal_aircraft || []);
                setOptimalStatus({ message: "Recommendations ready.", kind: "success" });
            } catch (error) {
                setOptimalStatus({ message: error.message || "Unable to fetch recommendations.", kind: "error" });
            } finally {
                setOptimalLoading(false);
            }
        },
        [optimalConfig]
    );

    const handleFleetSubmit = React.useCallback(
        async (event) => {
            event.preventDefault();
            const trimmed = (fleetQuery || "").trim();
            if (!trimmed) {
                setFleetStatus({ message: "Enter an airline name or code.", kind: "error" });
                setFleetProfile(null);
                return;
            }
            setFleetLoading(true);
            setFleetStatus({ message: "Fetching fleet details...", kind: "info" });
            try {
                const response = await fetch(`${API_BASE}/fleet?airline=${encodeURIComponent(trimmed)}`);
                const result = await response.json();
                if (!response.ok) {
                    throw new Error(result.detail || "Unable to fetch fleet profile.");
                }
                setFleetProfile(result);
                setFleetStatus({
                    message: `Fleet profile ready for ${result.airline?.name || trimmed}.`,
                    kind: "success",
                });
            } catch (error) {
                setFleetProfile(null);
                setFleetStatus({ message: error.message || "Unable to fetch fleet profile.", kind: "error" });
            } finally {
                setFleetLoading(false);
            }
        },
        [fleetQuery]
    );

    const handleAssignmentFieldChange = (field) => (event) => {
        const value = event.target.value;
        setFleetAssignmentConfig((prev) => ({ ...prev, [field]: value }));
    };

    const handleAssignmentFleetChange = (index, field) => (event) => {
        const value = event.target.value;
        setFleetAssignmentConfig((prev) => {
            const nextFleet = prev.fleet.map((entry, entryIndex) =>
                entryIndex === index ? { ...entry, [field]: value } : entry
            );
            return { ...prev, fleet: nextFleet };
        });
    };

    const handleAddFleetRow = () => {
        setFleetAssignmentConfig((prev) => ({
            ...prev,
            fleet: [...prev.fleet, { equipment: "", count: "1" }],
        }));
    };

    const handleRemoveFleetRow = (index) => {
        setFleetAssignmentConfig((prev) => {
            if (prev.fleet.length <= 1) {
                return prev;
            }
            const nextFleet = prev.fleet.filter((_, entryIndex) => entryIndex !== index);
            return { ...prev, fleet: nextFleet };
        });
    };

    const handleRouteShareRowChange = (index, field) => (event) => {
        const value = (event.target.value || "").toUpperCase();
        setRouteShareRows((prev) =>
            prev.map((row, rowIndex) => (rowIndex === index ? { ...row, [field]: value } : row))
        );
    };

    const handleAddRouteRow = () => {
        setRouteShareRows((prev) => [...prev, { source: "", destination: "" }]);
    };

    const handleRemoveRouteRow = (index) => {
        setRouteShareRows((prev) => {
            if (prev.length <= 1) {
                return prev;
            }
            return prev.filter((_, rowIndex) => rowIndex !== index);
        });
    };

    const handleRouteShareSubmit = async (event) => {
        event.preventDefault();
        const normalizedRoutes = routeShareRows
            .map((row) => ({
                source: (row.source || "").trim().toUpperCase(),
                destination: (row.destination || "").trim().toUpperCase(),
            }))
            .filter((entry) => entry.source && entry.destination);
        if (!normalizedRoutes.length) {
            setRouteShareStatus({ message: "Add at least one valid route pair.", kind: "error" });
            setRouteShareResults([]);
            return;
        }
        const parsedTop = Number(routeShareTopN);
        const topAirlines = Number.isFinite(parsedTop) && parsedTop > 0 ? Math.min(parsedTop, 20) : 5;
        setRouteShareLoading(true);
        setRouteShareStatus({ message: "Fetching route market share...", kind: "info" });
        setRouteShareResults([]);
        try {
            const response = await fetch(`${API_BASE}/route-share`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    routes: normalizedRoutes,
                    top_airlines: topAirlines,
                    include_all_competitors: routeShareIncludeAll,
                }),
            });
            const result = await response.json();
            if (!response.ok) {
                throw new Error(result.detail || "Unable to compute market share for the provided routes.");
            }
            setRouteShareResults(result.routes || []);
            setRouteShareStatus({
                message: `Showing market share for ${normalizedRoutes.length} route${
                    normalizedRoutes.length > 1 ? "s" : ""
                }.`,
                kind: "success",
            });
        } catch (error) {
            setRouteShareStatus({
                message: error.message || "Unable to compute route market share.",
                kind: "error",
            });
            setRouteShareResults([]);
        } finally {
            setRouteShareLoading(false);
        }
    };

    const handleProposalField = (field) => (event) => {
        const value = event.target.value || "";
        setProposal((prev) => ({ ...prev, [field]: value }));
    };

    const handleProposalSubmit = async (event) => {
        event.preventDefault();
        const airline = (proposal.airline || "").trim();
        const source = (proposal.source || "").trim().toUpperCase();
        const destination = (proposal.destination || "").trim().toUpperCase();
        if (!airline || !source || !destination) {
            setProposalStatus({ message: "Airline, source, and destination are required.", kind: "error" });
            setProposalResult(null);
            return;
        }
        const payload = { airline, source, destination };
        const seatDemand = proposal.seat_demand && Number(proposal.seat_demand);
        if (seatDemand && Number.isFinite(seatDemand) && seatDemand > 0) {
            payload.seat_demand = seatDemand;
        }
        setProposalLoading(true);
        setProposalStatus({ message: "Evaluating route...", kind: "info" });
        setProposalResult(null);
        try {
            const response = await fetch(`${API_BASE}/propose-route`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload),
            });
            const result = await response.json();
            if (!response.ok) {
                throw new Error(result.detail || "Unable to evaluate route.");
            }
            setProposalResult(result);
            setProposalStatus({ message: `Recommendation: ${result.recommendation}`, kind: "success" });
        } catch (error) {
            setProposalResult(null);
            setProposalStatus({ message: error.message || "Unable to evaluate route.", kind: "error" });
        } finally {
            setProposalLoading(false);
        }
    };

    const handleAssignmentSubmit = React.useCallback(
        async (event) => {
            event.preventDefault();
            const airline = (fleetAssignmentConfig.airline || "").trim();
            if (!airline) {
                setFleetAssignmentStatus({ message: "Enter an airline before simulating.", kind: "error" });
                setFleetAssignmentResults(null);
                return;
            }
            const fleetEntries = (fleetAssignmentConfig.fleet || [])
                .map((entry) => ({
                    equipment: (entry.equipment || "").trim(),
                    count: Number(entry.count),
                }))
                .filter((entry) => entry.equipment && Number.isFinite(entry.count) && entry.count > 0);
            if (!fleetEntries.length) {
                setFleetAssignmentStatus({ message: "Add at least one valid fleet entry.", kind: "error" });
                setFleetAssignmentResults(null);
                return;
            }
            const payload = {
                airline,
                fleet: fleetEntries,
                route_limit: Number(fleetAssignmentConfig.route_limit) || 60,
                day_hours: Number(fleetAssignmentConfig.day_hours) || 18,
                maintenance_hours: Number(fleetAssignmentConfig.maintenance_hours) || 6,
                crew_max_hours: Number(fleetAssignmentConfig.crew_max_hours) || 14,
            };
            setFleetAssignmentLoading(true);
            setFleetAssignmentStatus({ message: "Simulating fleet assignment...", kind: "info" });
            setFleetAssignmentResults(null);
            try {
                const response = await fetch(`${API_BASE}/fleet-assignment`, {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify(payload),
                });
                const result = await response.json();
                if (!response.ok) {
                    throw new Error(result.detail || "Unable to simulate fleet assignment.");
                }
                setFleetAssignmentResults(result);
                setFleetAssignmentStatus({ message: "Simulation complete.", kind: "success" });
            } catch (error) {
                setFleetAssignmentResults(null);
                setFleetAssignmentStatus({ message: error.message || "Unable to simulate fleet assignment.", kind: "error" });
            } finally {
                setFleetAssignmentLoading(false);
            }
        },
        [fleetAssignmentConfig]
    );

    const renderAirlineField = (label, field) => (
        <Autocomplete
            freeSolo
            options={suggestions}
            filterOptions={(options, { inputValue }) => {
                const normalized = (inputValue || "").trim().toLowerCase();
                const filtered = normalized
                    ? options.filter((option) => option.toLowerCase().includes(normalized))
                    : options;
                return filtered.slice(0, MAX_AIRLINE_SUGGESTIONS);
            }}
            openOnFocus
            autoHighlight
            noOptionsText="No matching airlines"
            value={formState[field]}
            inputValue={formState[field]}
            onChange={(_, value) => {
                setFormState((prev) => ({ ...prev, [field]: value || "" }));
            }}
            onInputChange={(_, value) => {
                const nextValue = value || "";
                setFormState((prev) => ({ ...prev, [field]: nextValue }));
                handleSuggestionQuery(nextValue);
            }}
            renderInput={(params) => (
                <TextField
                    {...params}
                    label={label}
                    variant="outlined"
                />
            )}
        />
    );

    return (
        <Box
            sx={{
                minHeight: "100vh",
                background: "radial-gradient(circle at top, #102040 0%, #050c1a 55%)",
                pb: 6,
            }}
        >
            <AppBar
                position="static"
                elevation={0}
                sx={{
                    background: "transparent",
                    borderBottom: "1px solid rgba(255,255,255,0.08)",
                    backdropFilter: "blur(12px)",
                }}
            >
                <Toolbar
                    sx={{
                        minHeight: 88,
                        flexDirection: { xs: "column", md: "row" },
                        alignItems: { xs: "flex-start", md: "center" },
                        gap: 2,
                    }}
                >
                    <Box sx={{ flexGrow: 1 }}>
                        <Typography variant="h5" fontWeight={700}>
                            Airline Route Optimizer
                        </Typography>
                        <Typography variant="body1" color="text.secondary">
                            Compare airline networks, benchmark specific routes, or inspect a single fleet.
                        </Typography>
                    </Box>
                    <Tabs
                        value={activePage}
                        onChange={(_, value) => setActivePage(value)}
                        textColor="inherit"
                        indicatorColor="secondary"
                        sx={{ minHeight: 48 }}
                    >
                        <Tab label="Route Analysis" value="analysis" />
                        <Tab label="Route Share" value="routes" />
                        <Tab label="Fleet Explorer" value="fleet" />
                        <Tab label="Route Proposal" value="proposal" />
                        <Tab label="Metrics Guide" value="metrics" />
                        <Tab label="Industry Trends" value="trends" />
                    </Tabs>
                </Toolbar>
            </AppBar>

            <Container maxWidth="xl" sx={{ py: { xs: 3, md: 5 } }}>
                <Box sx={{ mb: 3 }}>
                    <PageIntro activePage={activePage} />
                </Box>
                {activePage === "analysis" && (
                    <Grid container spacing={3} alignItems="stretch">
                    <Grid item xs={12} md={5} lg={4}>
                        <Stack spacing={3}>
                            <Paper
                                component="form"
                                onSubmit={handleSubmit}
                                sx={{
                                p: { xs: 2.5, md: 3 },
                                border: "1px solid rgba(255,255,255,0.08)",
                                boxShadow: "0 30px 60px rgba(0,0,0,0.45)",
                                display: "flex",
                                flexDirection: "column",
                                gap: 3,
                                height: "100%",
                            }}
                        >
                            <Box>
                                <Typography variant="h6">Analysis Configuration</Typography>
                                <Typography variant="body2" color="text.secondary">
                                    Choose airlines to compare, then tune CBSA simulation parameters.
                                </Typography>
                            </Box>

                            <Stack spacing={2}>
                                <Typography variant="subtitle2" color="text.secondary">
                                    Airline Comparison
                                </Typography>
                                {renderAirlineField("Airline 1", "comparison_airline_1")}
                                {renderAirlineField("Airline 2", "comparison_airline_2")}
                                <FormControlLabel
                                    control={
                                        <Switch
                                            color="secondary"
                                            checked={formState.skip_comparison}
                                            onChange={handleCheckboxChange("skip_comparison")}
                                        />
                                    }
                                    label="Skip comparison"
                                />
                            </Stack>

                            <Divider flexItem />

                            <Stack spacing={2}>
                                <Typography variant="subtitle2" color="text.secondary">
                                    CBSA Simulation
                                </Typography>
                                <TextField
                                    label="Additional airlines"
                                    placeholder="Enter one airline per line"
                                    multiline
                                    minRows={4}
                                    value={formState.cbsa_airlines}
                                    onChange={handleFieldChange("cbsa_airlines")}
                                    helperText="Useful if you only want CBSA results without a head-to-head comparison."
                                />
                                <Grid container spacing={2}>
                                    <Grid item xs={12} sm={6}>
                                        <TextField
                                            label="Top N routes"
                                            type="number"
                                            inputProps={{ min: 1, max: 20 }}
                                            value={formState.cbsa_top_n}
                                            onChange={handleFieldChange("cbsa_top_n")}
                                            fullWidth
                                            helperText="How many current routes to seed CBSA scoring."
                                        />
                                    </Grid>
                                    <Grid item xs={12} sm={6}>
                                        <TextField
                                            label="Suggestions per route"
                                            type="number"
                                            inputProps={{ min: 1, max: 10 }}
                                            value={formState.cbsa_suggestions}
                                            onChange={handleFieldChange("cbsa_suggestions")}
                                            fullWidth
                                            helperText="How many CBSA-similar routes to propose per seed."
                                        />
                                    </Grid>
                                </Grid>
                            </Stack>

                            <Divider flexItem />

                            <Stack spacing={2}>
                                <Typography variant="subtitle2" color="text.secondary">
                                    CBSA Cache (optional)
                                </Typography>
                                <FormControlLabel
                                    control={
                                        <Checkbox
                                            checked={formState.build_cbsa_cache}
                                            onChange={handleCheckboxChange("build_cbsa_cache")}
                                        />
                                    }
                                    label="Build CBSA cache"
                                />
                                <TextField
                                    label="Countries (comma-separated)"
                                    placeholder="United States, Canada"
                                    value={formState.cbsa_cache_country}
                                    onChange={handleFieldChange("cbsa_cache_country")}
                                />
                                <Grid container spacing={2}>
                                    <Grid item xs={12} sm={6}>
                                        <TextField
                                            label="Limit airports"
                                            type="number"
                                            inputProps={{ min: 1 }}
                                            value={formState.cbsa_cache_limit}
                                            onChange={handleFieldChange("cbsa_cache_limit")}
                                            fullWidth
                                        />
                                    </Grid>
                                    <Grid item xs={12} sm={6}>
                                        <TextField
                                            label="Chunk size"
                                            type="number"
                                            inputProps={{ min: 50 }}
                                            value={formState.cbsa_cache_chunk_size}
                                            onChange={handleFieldChange("cbsa_cache_chunk_size")}
                                            fullWidth
                                        />
                                    </Grid>
                                </Grid>
                            </Stack>

                            <Button
                                type="submit"
                                variant="contained"
                                size="large"
                                disabled={loading}
                                sx={{ alignSelf: "flex-start", mt: 1 }}
                            >
                                {loading ? (
                                    <Stack direction="row" spacing={1} alignItems="center">
                                        <CircularProgress size={20} color="inherit" />
                                        <span>Running...</span>
                                    </Stack>
                                ) : (
                                    "Run Analysis"
                                )}
                            </Button>
                        </Paper>
                        <Paper
                            component="form"
                            onSubmit={handleOptimalSubmit}
                            sx={{
                                p: { xs: 2.5, md: 3 },
                                border: "1px solid rgba(255,255,255,0.08)",
                                boxShadow: "0 30px 60px rgba(0,0,0,0.45)",
                                display: "flex",
                                flexDirection: "column",
                                gap: 2,
                            }}
                        >
                            <Box>
                                <Typography variant="h6">Optimal Aircraft</Typography>
                                <Typography variant="body2" color="text.secondary">
                                    Rank in-fleet equipment by utilization and distance-fit for a single stage.
                                </Typography>
                            </Box>
                            <StatusAlert status={optimalStatus} />
                            <Stack spacing={2}>
                                <Typography variant="subtitle2" color="text.secondary">
                                    Target Flight
                                </Typography>
                                <Autocomplete
                                    freeSolo
                                    options={suggestions}
                                    filterOptions={(options, { inputValue }) => {
                                        const normalizedSearch = (inputValue || "").trim().toLowerCase();
                                        const filtered = normalizedSearch
                                            ? options.filter((option) => option.toLowerCase().includes(normalizedSearch))
                                            : options;
                                        return filtered.slice(0, MAX_AIRLINE_SUGGESTIONS);
                                    }}
                                    openOnFocus
                                    autoHighlight
                                    noOptionsText="No matching airlines"
                                    value={optimalConfig.airline}
                                    inputValue={optimalConfig.airline}
                                    onChange={(_, value) => {
                                        setOptimalConfig((prev) => ({ ...prev, airline: value || "" }));
                                    }}
                                    onInputChange={(_, value) => {
                                        const nextValue = value || "";
                                        setOptimalConfig((prev) => ({ ...prev, airline: nextValue }));
                                        handleSuggestionQuery(nextValue);
                                    }}
                                    renderInput={(params) => (
                                        <TextField
                                            {...params}
                                            label="Airline"
                                            variant="outlined"
                                            helperText="Start typing to trigger suggestions"
                                            fullWidth
                                        />
                                    )}
                                />
                                <TextField
                                    label="Route Distance (miles)"
                                    type="number"
                                    value={optimalConfig.route_distance}
                                    onChange={handleOptimalFieldChange("route_distance")}
                                    fullWidth
                                />
                                <TextField
                                    label="Seat Demand (optional)"
                                    type="number"
                                    value={optimalConfig.seat_demand}
                                    onChange={handleOptimalFieldChange("seat_demand")}
                                    fullWidth
                                />
                                <TextField
                                    label="Results (top N)"
                                    type="number"
                                    inputProps={{ min: 1, max: 10 }}
                                    value={optimalConfig.top_n}
                                    onChange={handleOptimalFieldChange("top_n")}
                                    fullWidth
                                />
                            </Stack>
                            <Button
                                type="submit"
                                variant="contained"
                                size="large"
                                disabled={optimalLoading}
                                sx={{ alignSelf: "flex-start" }}
                            >
                                {optimalLoading ? (
                                    <Stack direction="row" spacing={1} alignItems="center">
                                        <CircularProgress size={20} color="inherit" />
                                        <span>Finding...</span>
                                    </Stack>
                                ) : (
                                    "Find optimal aircraft"
                                )}
                            </Button>
                            <DataTable
                                rows={optimalResults}
                                title="Recommended Equipment"
                                maxHeight={260}
                                enableWrapping
                            />
                        </Paper>
                    </Stack>
                    </Grid>

                    <Grid item xs={12} md={7} lg={8}>
                        <Paper
                            sx={{
                                p: { xs: 2.5, md: 3 },
                                border: "1px solid rgba(255,255,255,0.08)",
                                boxShadow: "0 30px 60px rgba(0,0,0,0.45)",
                                height: "100%",
                                display: "flex",
                                flexDirection: "column",
                                gap: 3,
                            }}
                        >
                            <Box>
                                <Typography variant="h6">Results</Typography>
                                <Typography variant="body2" color="text.secondary">
                                    Explore head-to-head routes plus CBSA opportunity areas.
                                </Typography>
                            </Box>
                            <StatusAlert status={status} />

                            {messages.length > 0 && (
                                <Stack
                                    direction="row"
                                    flexWrap="wrap"
                                    gap={1}
                                    sx={{ mb: 3 }}
                                >
                                    {messages.map((message, index) => (
                                        <Chip
                                            key={`${message}-${index}`}
                                            label={message}
                                            color="primary"
                                            variant="outlined"
                                        />
                                    ))}
                                </Stack>
                            )}

                            <Tabs
                                value={resultsTab}
                                onChange={(_, value) => setResultsTab(value)}
                                textColor="primary"
                                indicatorColor="primary"
                                variant="fullWidth"
                                sx={{ borderBottom: "1px solid rgba(255,255,255,0.12)" }}
                            >
                                <Tab label="Competing Routes" value="competing" />
                                <Tab label="CBSA Opportunities" value="cbsa" />
                            </Tabs>

                            {resultsTab === "competing" && (
                                comparison ? (
                                    <Box sx={{ mt: 2 }}>
                                        <Typography variant="h5" sx={{ mb: 1 }}>
                                            Competing Routes
                                        </Typography>
                                        <NetworkSummary airlines={comparison.airlines} />
                                        <DataTable
                                            rows={comparison.competing_routes}
                                            title="Competing Routes"
                                            maxHeight={420}
                                        />
                                    </Box>
                                ) : (
                                    <Typography color="text.secondary" sx={{ mt: 2 }}>
                                        Choose comparison airlines and rerun the analysis to view head-to-head routes.
                                    </Typography>
                                )
                            )}

                            {resultsTab === "cbsa" && (
                                cbsaResults && cbsaResults.length ? (
                                    <Box sx={{ mt: 2 }}>
                                        <CbsaOpportunities entries={cbsaResults} />
                                    </Box>
                                ) : (
                                    <Typography color="text.secondary" sx={{ mt: 2 }}>
                                        Add CBSA airlines to the form to surface opportunity corridors.
                                    </Typography>
                                )
                            )}
                        </Paper>
                    </Grid>
                    </Grid>
                )}

                {activePage === "metrics" && (
                    <Stack spacing={3} sx={{ pb: 3 }}>
                        <MetricsGuide />
                        <MetricsPlaybook />
                    </Stack>
                )}

                {activePage === "trends" && (
                    <Box sx={{ pb: 3 }}>
                        <IndustryTrends />
                    </Box>
                )}

                {activePage === "proposal" && (
                    <Grid container spacing={3} alignItems="stretch">
                        <Grid item xs={12} md={4}>
                            <Paper
                                component="form"
                                onSubmit={handleProposalSubmit}
                                sx={{
                                    p: { xs: 2.5, md: 3 },
                                    border: "1px solid rgba(255,255,255,0.08)",
                                    boxShadow: "0 30px 60px rgba(0,0,0,0.45)",
                                    display: "flex",
                                    flexDirection: "column",
                                    gap: 2,
                                }}
                            >
                                <Typography variant="h6">New Route Proposal</Typography>
                                <Typography variant="body2" color="text.secondary">
                                    We'll score the route by competition, market depth, and distance fit vs. the airline's network.
                                </Typography>
                                <StatusAlert status={proposalStatus} />
                                <Autocomplete
                                    freeSolo
                                    options={suggestions}
                                    filterOptions={(options, { inputValue }) => {
                                        const normalized = (inputValue || "").trim().toLowerCase();
                                        const filtered = normalized
                                            ? options.filter((option) => option.toLowerCase().includes(normalized))
                                            : options;
                                        return filtered.slice(0, MAX_AIRLINE_SUGGESTIONS);
                                    }}
                                    openOnFocus
                                    autoHighlight
                                    noOptionsText="No matching airlines"
                                    value={proposal.airline}
                                    inputValue={proposal.airline}
                                    onChange={(_, value) => {
                                        setProposal((prev) => ({ ...prev, airline: value || "" }));
                                    }}
                                    onInputChange={(_, value) => {
                                        const nextValue = value || "";
                                        setProposal((prev) => ({ ...prev, airline: nextValue }));
                                        handleSuggestionQuery(nextValue);
                                    }}
                                    renderInput={(params) => (
                                        <TextField
                                            {...params}
                                            label="Airline"
                                            variant="outlined"
                                        />
                                    )}
                                />
                                <Grid container spacing={2}>
                                    <Grid item xs={6}>
                                        <TextField
                                            label="Source"
                                            placeholder="e.g. JFK"
                                            value={proposal.source}
                                            onChange={handleProposalField("source")}
                                            inputProps={{ maxLength: 4 }}
                                            fullWidth
                                        />
                                    </Grid>
                                    <Grid item xs={6}>
                                        <TextField
                                            label="Destination"
                                            placeholder="e.g. LHR"
                                            value={proposal.destination}
                                            onChange={handleProposalField("destination")}
                                            inputProps={{ maxLength: 4 }}
                                            fullWidth
                                        />
                                    </Grid>
                                </Grid>
                                <TextField
                                    label="Seat demand (optional)"
                                    type="number"
                                    value={proposal.seat_demand}
                                    onChange={handleProposalField("seat_demand")}
                                />
                                <Button
                                    type="submit"
                                    variant="contained"
                                    size="large"
                                    disabled={proposalLoading}
                                    sx={{ alignSelf: "flex-start" }}
                                >
                                    {proposalLoading ? (
                                        <Stack direction="row" spacing={1} alignItems="center">
                                            <CircularProgress size={20} color="inherit" />
                                            <span>Evaluating...</span>
                                        </Stack>
                                    ) : (
                                        "Evaluate route"
                                    )}
                                </Button>
                            </Paper>
                        </Grid>
                        <Grid item xs={12} md={8}>
                            {proposalResult ? (
                                <Paper
                                    variant="outlined"
                                    sx={{
                                        p: { xs: 2.5, md: 3 },
                                        border: "1px solid rgba(255,255,255,0.08)",
                                        boxShadow: "0 30px 60px rgba(0,0,0,0.35)",
                                    }}
                                >
                                    <Stack spacing={2}>
                                        <Stack direction="row" spacing={2} alignItems="center" flexWrap="wrap">
                                            <Typography variant="h5">
                                                {proposalResult.source} -> {proposalResult.destination}
                                            </Typography>
                                            <Chip label={`Airline: ${proposalResult.airline}`} />
                                            <Chip
                                                label={`Recommendation: ${proposalResult.recommendation.toUpperCase()} (score ${proposalResult.score.toFixed(2)})`}
                                                color={
                                                    proposalResult.recommendation === "good"
                                                        ? "success"
                                                        : proposalResult.recommendation === "watch"
                                                        ? "warning"
                                                        : "error"
                                                }
                                            />
                                        </Stack>
                                        <Grid container spacing={2}>
                                            <Grid item xs={12} sm={6} md={4}>
                                                <Typography variant="subtitle2" color="text.secondary">
                                                    Competition
                                                </Typography>
                                                <Typography variant="body1">
                                                    {proposalResult.competition_label} ({proposalResult.competition_count} carriers)
                                                </Typography>
                                            </Grid>
                                            <Grid item xs={12} sm={6} md={4}>
                                                <Typography variant="subtitle2" color="text.secondary">
                                                    Market ASM
                                                </Typography>
                                                <Typography variant="body1">
                                                    {proposalResult.market_asm ? integerNumberFormatter.format(proposalResult.market_asm) : "-"}
                                                </Typography>
                                            </Grid>
                                            <Grid item xs={12} sm={6} md={4}>
                                                <Typography variant="subtitle2" color="text.secondary">
                                                    Distance
                                                </Typography>
                                                <Typography variant="body1">
                                                    {proposalResult.distance_miles ? `${integerNumberFormatter.format(proposalResult.distance_miles)} mi` : "-"}
                                                </Typography>
                                            </Grid>
                                        </Grid>
                                        <Stack direction="row" flexWrap="wrap" gap={1}>
                                            <ScoreChip
                                                label={`Competition score: ${proposalResult.competition_score}`}
                                                tooltip="Higher is better; monopoly ~1.0, multi-carrier corridors score lower."
                                            />
                                            <ScoreChip
                                                label={`Distance fit: ${proposalResult.distance_fit}`}
                                                tooltip="How close this stage length is to the airline's median distance; 1.0 means perfect fit."
                                            />
                                            <ScoreChip
                                                label={`Market depth: ${proposalResult.market_depth_score}`}
                                                tooltip="Compares total ASM in this O&D to the airline's median; higher suggests thicker markets."
                                            />
                                            {proposalResult.hub_fit_label ? (
                                                <ScoreChip
                                                    label={`Hub fit: ${proposalResult.hub_fit_label}`}
                                                    tooltip="Hub-to-hub > hub-to-spoke > off-hub. Higher fit usually supports frequency and yield."
                                                />
                                            ) : null}
                                            {proposalResult.load_factor_target ? (
                                                <ScoreChip
                                                    label={`LF target: ≥ ${percentFormatter.format(proposalResult.load_factor_target)}`}
                                                    tooltip="Target load factor derived from recent operational metrics; aim at or above this."
                                                />
                                            ) : null}
                                        </Stack>
                                        {proposalResult.analog_summary ? (
                                            <Typography variant="body2" color="text.secondary">
                                                Analog demand: median ASM {integerNumberFormatter.format(proposalResult.analog_summary.median_asm || 0)}; median competition score{" "}
                                                {proposalResult.analog_summary.median_competition_score ?? "-"}; sample routes{" "}
                                                {proposalResult.analog_summary.sample_routes && proposalResult.analog_summary.sample_routes.length
                                                    ? proposalResult.analog_summary.sample_routes.join(", ")
                                                    : "N/A"}
                                            </Typography>
                                        ) : null}
                                        <Typography variant="subtitle2" color="text.secondary">
                                            Rationale
                                        </Typography>
                                        <Stack component="ul" spacing={0.6} sx={{ listStyle: "disc", pl: 3, color: "text.secondary" }}>
                                            {proposalResult.rationale?.map((item, idx) => (
                                                <Box key={idx} component="li">
                                                    {item}
                                                </Box>
                                            ))}
                                        </Stack>
                                        {playbookNotes ? (
                                            <Box
                                                sx={{
                                                    mt: 1,
                                                    p: 2,
                                                    border: "1px solid rgba(255,255,255,0.12)",
                                                    borderRadius: 2,
                                                    background: "linear-gradient(135deg, rgba(138,180,248,0.05), rgba(138,180,248,0.02))",
                                                }}
                                            >
                                                <Typography variant="subtitle2" sx={{ mb: 1 }}>
                                                    Playbook for this route
                                                </Typography>
                                                <Stack component="ul" spacing={0.6} sx={{ listStyle: "disc", pl: 3, color: "text.secondary" }}>
                                                    {playbookNotes.map((item, idx) => (
                                                        <Box key={`general-note-${idx}`} component="li">
                                                            {item}
                                                        </Box>
                                                    ))}
                                                </Stack>
                                            </Box>
                                        ) : null}
                                        {embeddedInsight ? (
                                            <Box
                                                sx={{
                                                    mt: 1,
                                                    p: 2,
                                                    border: "1px solid rgba(255,255,255,0.12)",
                                                    borderRadius: 2,
                                                    background: "linear-gradient(135deg, rgba(138,180,248,0.08), rgba(242,139,130,0.06))",
                                                }}
                                            >
                                                <Typography variant="subtitle2" sx={{ mb: 1 }}>
                                                    Embedded analyst note (AA JFK-MIA)
                                                </Typography>
                                                <Stack component="ul" spacing={0.6} sx={{ listStyle: "disc", pl: 3, color: "text.secondary" }}>
                                                    {embeddedInsight.map((item, idx) => (
                                                        <Box key={`insight-${idx}`} component="li">
                                                            {item}
                                                        </Box>
                                                    ))}
                                                </Stack>
                                            </Box>
                                        ) : null}
                                    </Stack>
                                </Paper>
                            ) : (
                                <Paper
                                    variant="outlined"
                                    sx={{
                                        p: { xs: 2.5, md: 3 },
                                        border: "1px solid rgba(255,255,255,0.08)",
                                        minHeight: 260,
                                        display: "flex",
                                        alignItems: "center",
                                        justifyContent: "center",
                                        textAlign: "center",
                                    }}
                                >
                                    <Typography color="text.secondary">
                                        Enter an airline and an unserved route to get a quick go/watch/avoid recommendation.
                                    </Typography>
                                </Paper>
                            )}
                        </Grid>
                    </Grid>
                )}

                {activePage === "routes" && (
                    <Grid container spacing={3} alignItems="stretch">
                        <Grid item xs={12} md={4}>
                            <Paper
                                component="form"
                                onSubmit={handleRouteShareSubmit}
                                    sx={{
                                        p: { xs: 2.5, md: 3 },
                                        border: "1px solid rgba(255,255,255,0.08)",
                                    boxShadow: "0 30px 60px rgba(0,0,0,0.45)",
                                    display: "flex",
                                    flexDirection: "column",
                                    gap: 2.5,
                                }}
                            >
                                <Box>
                                    <Typography variant="h6">Route Market Share</Typography>
                                    <Typography variant="body2" color="text.secondary">
                                        Input airport pairs to benchmark carrier share, maturity, and seat deployment.
                                    </Typography>
                                </Box>
                                <Stack spacing={2}>
                                    <Typography variant="subtitle2" color="text.secondary">
                                        Routes
                                    </Typography>
                                    {routeShareRows.map((row, index) => (
                                        <Stack
                                            key={`route-row-${index}`}
                                            direction={{ xs: "column", md: "row" }}
                                            spacing={1}
                                            alignItems={{ xs: "stretch", md: "flex-end" }}
                                        >
                                            <TextField
                                                label="Source"
                                                placeholder="e.g. JFK"
                                                inputProps={{ maxLength: 4 }}
                                                value={row.source}
                                                onChange={handleRouteShareRowChange(index, "source")}
                                                sx={{ flex: 1 }}
                                            />
                                            <TextField
                                                label="Destination"
                                                placeholder="e.g. LAX"
                                                inputProps={{ maxLength: 4 }}
                                                value={row.destination}
                                                onChange={handleRouteShareRowChange(index, "destination")}
                                                sx={{ flex: 1 }}
                                            />
                                            {routeShareRows.length > 1 && (
                                                <Button
                                                    type="button"
                                                    color="secondary"
                                                    onClick={() => handleRemoveRouteRow(index)}
                                                >
                                                    Remove
                                                </Button>
                                            )}
                                        </Stack>
                                    ))}
                                    <Button
                                        type="button"
                                        variant="outlined"
                                        color="secondary"
                                        onClick={handleAddRouteRow}
                                        sx={{ alignSelf: "flex-start" }}
                                    >
                                        Add route
                                    </Button>
                                </Stack>
                                <TextField
                                    label="Airlines to surface"
                                    type="number"
                                    inputProps={{ min: 1, max: 20 }}
                                    value={routeShareTopN}
                                    onChange={(event) => setRouteShareTopN(event.target.value)}
                                />
                                <FormControlLabel
                                    control={
                                        <Switch
                                            checked={routeShareIncludeAll}
                                            onChange={(_, checked) => setRouteShareIncludeAll(checked)}
                                        />
                                    }
                                    label="Include all competitors"
                                />
                                <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
                                    <Button type="submit" variant="contained" size="large" disabled={routeShareLoading}>
                                        {routeShareLoading ? (
                                            <Stack direction="row" spacing={1} alignItems="center">
                                                <CircularProgress size={20} color="inherit" />
                                                <span>Analyzing...</span>
                                            </Stack>
                                        ) : (
                                            "Analyze routes"
                                        )}
                                    </Button>
                                    <Typography variant="caption" color="text.secondary" sx={{ alignSelf: "center" }}>
                                        We map IATA pairs to published schedules, ASM, and proxy maturity metrics.
                                    </Typography>
                                </Stack>
                            </Paper>
                        </Grid>
                        <Grid item xs={12} md={8}>
                            <Stack spacing={3}>
                                <StatusAlert status={routeShareStatus} />
                                {routeShareLoading ? (
                                    <Paper
                                        variant="outlined"
                                        sx={{
                                            p: { xs: 2.5, md: 3 },
                                            minHeight: 240,
                                            display: "flex",
                                            alignItems: "center",
                                            justifyContent: "center",
                                        }}
                                    >
                                        <Stack direction="row" spacing={1.5} alignItems="center">
                                            <CircularProgress size={24} />
                                            <Typography color="text.secondary">Crunching route stats...</Typography>
                                        </Stack>
                                    </Paper>
                                ) : (
                                    <RouteShareResults routes={routeShareResults} />
                                )}
                            </Stack>
                        </Grid>
                    </Grid>
                )}

                {activePage === "fleet" && (
                    <Stack spacing={3}>
                        <Grid container spacing={3} alignItems="stretch">
                            <Grid item xs={12} md={4}>
                                <Paper
                                    component="form"
                                    onSubmit={handleFleetSubmit}
                                    sx={{
                                        p: { xs: 2.5, md: 3 },
                                        border: "1px solid rgba(255,255,255,0.08)",
                                        boxShadow: "0 30px 60px rgba(0,0,0,0.45)",
                                        display: "flex",
                                        flexDirection: "column",
                                        gap: 2.5,
                                    }}
                                >
                                    <Box>
                                        <Typography variant="h6">Fleet Explorer</Typography>
                                        <Typography variant="body2" color="text.secondary">
                                            Pull the fleet mix, top routes, and ASM accuracy for a single airline.
                                        </Typography>
                                    </Box>
                                    <StatusAlert status={fleetStatus} />
                                    <Autocomplete
                                        freeSolo
                                        options={suggestions}
                                        filterOptions={(options, { inputValue }) => {
                                            const normalized = (inputValue || "").trim().toLowerCase();
                                            const filtered = normalized
                                                ? options.filter((option) => option.toLowerCase().includes(normalized))
                                                : options;
                                            return filtered.slice(0, MAX_AIRLINE_SUGGESTIONS);
                                        }}
                                        openOnFocus
                                        autoHighlight
                                        noOptionsText="No matching airlines"
                                        value={fleetQuery}
                                        inputValue={fleetQuery}
                                        onChange={(_, value) => {
                                            setFleetQuery(value || "");
                                        }}
                                        onInputChange={(_, value) => {
                                            const nextValue = value || "";
                                            setFleetQuery(nextValue);
                                            handleSuggestionQuery(nextValue);
                                        }}
                                        renderInput={(params) => (
                                            <TextField
                                                {...params}
                                                label="Airline"
                                                variant="outlined"
                                            />
                                        )}
                                    />
                                    <Button
                                        type="submit"
                                        variant="contained"
                                        size="large"
                                        disabled={fleetLoading}
                                    >
                                        {fleetLoading ? (
                                            <Stack direction="row" spacing={1} alignItems="center">
                                                <CircularProgress size={20} color="inherit" />
                                                <span>Loading...</span>
                                            </Stack>
                                        ) : (
                                            "View Fleet Profile"
                                        )}
                                    </Button>
                                    <Typography variant="caption" color="text.secondary">
                                        Tip: start typing to discover fuzzy-matched airlines.
                                    </Typography>
                                </Paper>
                            </Grid>
                            <Grid item xs={12} md={8}>
                                {fleetProfile ? (
                                    <FleetProfile profile={fleetProfile} />
                                ) : (
                                    <Paper
                                        variant="outlined"
                                        sx={{
                                            p: { xs: 2.5, md: 3 },
                                            border: "1px solid rgba(255,255,255,0.08)",
                                            minHeight: 360,
                                            display: "flex",
                                            alignItems: "center",
                                            justifyContent: "center",
                                            textAlign: "center",
                                        }}
                                    >
                                        <Typography color="text.secondary">
                                            Search for an airline to view its fleet composition and top routes.
                                        </Typography>
                                    </Paper>
                                )}
                            </Grid>
                        </Grid>

        <Paper
            component="form"
            onSubmit={handleAssignmentSubmit}
            sx={{
                p: { xs: 2.5, md: 3 },
                border: "1px solid rgba(255,255,255,0.08)",
                boxShadow: "0 30px 60px rgba(0,0,0,0.45)",
                display: "flex",
                flexDirection: "column",
                gap: 2.5,
            }}
        >
            <Box>
                <Typography variant="h6">Live Fleet Assignment Simulator</Typography>
                <Typography variant="body2" color="text.secondary">
                    Define a fleet, then assign tails to the busiest routes while respecting block times, turn buffers, and maintenance.
                </Typography>
            </Box>
            <StatusAlert status={fleetAssignmentStatus} />
            <Autocomplete
                freeSolo
                options={suggestions}
                filterOptions={(options, { inputValue }) => {
                    const normalized = (inputValue || "").trim().toLowerCase();
                    const filtered = normalized
                        ? options.filter((option) => option.toLowerCase().includes(normalized))
                        : options;
                    return filtered.slice(0, MAX_AIRLINE_SUGGESTIONS);
                }}
                openOnFocus
                autoHighlight
                noOptionsText="No matching airlines"
                value={fleetAssignmentConfig.airline}
                inputValue={fleetAssignmentConfig.airline}
                onChange={(_, value) => {
                    const next = value || "";
                    setFleetAssignmentConfig((prev) => ({ ...prev, airline: next }));
                }}
                onInputChange={(_, value) => {
                    const nextValue = value || "";
                    setFleetAssignmentConfig((prev) => ({ ...prev, airline: nextValue }));
                    handleSuggestionQuery(nextValue);
                }}
                renderInput={(params) => (
                    <TextField
                        {...params}
                        label="Airline"
                        variant="outlined"
                        helperText="Use any public carrier or search for a code"
                    />
                )}
            />
            <Stack spacing={1.5}>
                <Typography variant="subtitle2" color="text.secondary">
                    Fleet mix
                </Typography>
                {fleetAssignmentConfig.fleet.map((entry, index) => (
                    <Stack
                        key={`fleet-row-${index}`}
                        direction={{ xs: "column", md: "row" }}
                        spacing={1}
                        alignItems={{ xs: "stretch", md: "flex-end" }}
                    >
                        <TextField
                            label="Equipment"
                            value={entry.equipment}
                            onChange={handleAssignmentFleetChange(index, "equipment")}
                            fullWidth
                        />
                        <TextField
                            label="Tails"
                            type="number"
                            inputProps={{ min: 1 }}
                            value={entry.count}
                            onChange={handleAssignmentFleetChange(index, "count")}
                            sx={{ width: { xs: "100%", md: 140 } }}
                        />
                        {fleetAssignmentConfig.fleet.length > 1 && (
                            <Button color="secondary" onClick={() => handleRemoveFleetRow(index)}>
                                Remove
                            </Button>
                        )}
                    </Stack>
                ))}
                <Button variant="outlined" color="secondary" onClick={handleAddFleetRow} sx={{ alignSelf: "flex-start" }}>
                    Add equipment type
                </Button>
            </Stack>
            <Grid container spacing={2}>
                <Grid item xs={12} sm={6} md={3}>
                    <TextField
                        label="Sample routes"
                        type="number"
                        helperText="Top O&D segments to schedule"
                        value={fleetAssignmentConfig.route_limit}
                        onChange={handleAssignmentFieldChange("route_limit")}
                        fullWidth
                    />
                </Grid>
                <Grid item xs={12} sm={6} md={3}>
                    <TextField
                        label="Operating day (hrs)"
                        type="number"
                        value={fleetAssignmentConfig.day_hours}
                        onChange={handleAssignmentFieldChange("day_hours")}
                        fullWidth
                    />
                </Grid>
                <Grid item xs={12} sm={6} md={3}>
                    <TextField
                        label="Maintenance window (hrs)"
                        type="number"
                        value={fleetAssignmentConfig.maintenance_hours}
                        onChange={handleAssignmentFieldChange("maintenance_hours")}
                        fullWidth
                    />
                </Grid>
                <Grid item xs={12} sm={6} md={3}>
                    <TextField
                        label="Crew max block (hrs)"
                        type="number"
                        value={fleetAssignmentConfig.crew_max_hours}
                        onChange={handleAssignmentFieldChange("crew_max_hours")}
                        fullWidth
                    />
                </Grid>
            </Grid>
            <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
                <Button
                    type="submit"
                    variant="contained"
                    size="large"
                    disabled={fleetAssignmentLoading}
                    sx={{ alignSelf: "flex-start" }}
                >
                    {fleetAssignmentLoading ? (
                        <Stack direction="row" spacing={1} alignItems="center">
                            <CircularProgress size={20} color="inherit" />
                            <span>Simulating...</span>
                        </Stack>
                    ) : (
                        "Simulate assignments"
                    )}
                </Button>
                <Typography variant="caption" color="text.secondary" sx={{ alignSelf: "center" }}>
                    Heuristic assigns aircraft by seat class, stage length, duty limits, and nightly maintenance buffer.
                </Typography>
            </Stack>
        </Paper>

        {fleetAssignmentResults ? (
            <FleetAssignmentResults result={fleetAssignmentResults} />
        ) : (
            <Paper
                variant="outlined"
                sx={{
                    p: { xs: 2.5, md: 3 },
                    border: "1px solid rgba(255,255,255,0.08)",
                    textAlign: "center",
                }}
            >
                <Typography color="text.secondary">
                    Configure a fleet above to generate a duty-day schedule with block utilization and hours per tail.
                </Typography>
            </Paper>
                        )}
                    </Stack>
                )}
            </Container>
        </Box>
    );
}

const root = ReactDOM.createRoot(document.getElementById("root"));
root.render(
    <ThemeProvider theme={darkTheme}>
        <CssBaseline />
        <App />
    </ThemeProvider>
);
