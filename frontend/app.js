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

function formatValue(value) {
    if (value === null || value === undefined || value === "") {
        return "—";
    }
    if (Array.isArray(value)) {
        return value.map((entry) => formatValue(entry)).join(", ");
    }
    if (typeof value === "number") {
        if (!Number.isFinite(value)) {
            return "—";
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
        return "—";
    }
    if (key.toLowerCase().includes("hub") && Array.isArray(value)) {
        return value
            .map((hub) => (Array.isArray(hub) ? `${hub[0]} — ${hub[1]}` : formatValue(hub)))
            .join(", ");
    }
    return formatValue(value);
}

function formatPercent(value) {
    if (typeof value !== "number" || Number.isNaN(value)) {
        return "—";
    }
    return percentFormatter.format(value);
}

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
                Route Scorecard
            </Typography>
            <Grid container spacing={2}>
                {competitionEntries.length > 0 && (
                    <Grid item xs={12} md={6}>
                        <Typography variant="caption" color="text.secondary">
                            Competition Mix
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
                            Network Maturity
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
                            Yield Proxy (lower values lean cost-leader)
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
                            <TableCell>{`${row.Source || "?"} → ${row.Destination || "?"}`}</TableCell>
                            <TableCell align="right">{formatPercent(row["Market Share"])}</TableCell>
                            <TableCell>{row["Competition Level"] || "—"}</TableCell>
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
                Fleet Utilization Snapshot
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
        Route: entry.route || `${entry.source || "?"} → ${entry.destination || "?"}`,
        Tail: entry.tail_id || "—",
        Equipment: entry.assigned_equipment || "—",
        "Requested Equip": entry.equipment_requested || "—",
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
                const title = `${route.source || "?"} → ${route.destination || "?"}`;
                const summaryChips = [
                    {
                        label: "Distance",
                        value: route.distance_miles ? `${integerNumberFormatter.format(route.distance_miles)} mi` : "—",
                    },
                    {
                        label: "Market ASM",
                        value: route.market_asm ? `${integerNumberFormatter.format(route.market_asm)} ASM` : "—",
                    },
                    {
                        label: "Competitors",
                        value: route.competitor_count != null ? integerNumberFormatter.format(route.competitor_count) : "—",
                    },
                    { label: "Competition", value: route.competition_level || "—" },
                    { label: "Maturity", value: route.route_maturity_label || "—" },
                    {
                        label: "Yield Proxy",
                        value:
                            route.yield_proxy_score != null
                                ? decimalNumberFormatter.format(route.yield_proxy_score)
                                : "—",
                    },
                ];
                const tableRows = (route.airlines || []).map((entry) => ({
                    Airline: entry.airline || entry.airline_normalized || "Airline",
                    ASM: entry.asm,
                    "Market Share":
                        typeof entry.market_share === "number" ? formatPercent(entry.market_share) : "—",
                    Seats: entry.seats,
                    "Seats / Mile": entry.seats_per_mile,
                    Equipment: entry.equipment && entry.equipment.length ? entry.equipment.join(", ") : "—",
                    "Strategy Score": entry.route_strategy_baseline,
                    "Yield Proxy": entry.yield_proxy_score,
                    "Route Maturity": entry.route_maturity_label || route.route_maturity_label || "—",
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
    const [routeShareStatus, setRouteShareStatus] = React.useState({ message: "", kind: "" });
    const [routeShareLoading, setRouteShareLoading] = React.useState(false);
    const [routeShareResults, setRouteShareResults] = React.useState([]);

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
        setStatus({ message: "Running analysis…", kind: "info" });
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
            setOptimalStatus({ message: "Finding optimal equipment…", kind: "info" });
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
            setFleetStatus({ message: "Fetching fleet details…", kind: "info" });
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
        setRouteShareStatus({ message: "Fetching route market share…", kind: "info" });
        setRouteShareResults([]);
        try {
            const response = await fetch(`${API_BASE}/route-share`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ routes: normalizedRoutes, top_airlines: topAirlines }),
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
            setFleetAssignmentStatus({ message: "Simulating fleet assignment…", kind: "info" });
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
                    </Tabs>
                </Toolbar>
            </AppBar>

            <Container maxWidth="xl" sx={{ py: { xs: 3, md: 5 } }}>
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
                                <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
                                    <Button type="submit" variant="contained" size="large" disabled={routeShareLoading}>
                                        {routeShareLoading ? (
                                            <Stack direction="row" spacing={1} alignItems="center">
                                                <CircularProgress size={20} color="inherit" />
                                                <span>Analyzing…</span>
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
                                            <Typography color="text.secondary">Crunching route stats…</Typography>
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
