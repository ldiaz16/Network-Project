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

const DataTable = ({ rows, title, maxHeight }) => {
    if (!rows || !rows.length) {
        return (
            <Paper variant="outlined" sx={{ p: 2, mb: 3 }}>
                <Typography color="text.secondary">No {title.toLowerCase()} available.</Typography>
            </Paper>
        );
    }

    const headers = Object.keys(rows[0]);

    return (
        <Paper
            variant="outlined"
            sx={{
                mb: 3,
                overflowX: "auto",
            }}
        >
            <TableContainer sx={maxHeight ? { maxHeight } : undefined}>
                <Table size="small" stickyHeader={Boolean(maxHeight)}>
                    <TableHead>
                        <TableRow>
                            {headers.map((header) => (
                                <TableCell key={header}>{header}</TableCell>
                            ))}
                        </TableRow>
                    </TableHead>
                    <TableBody>
                        {rows.map((row, rowIndex) => (
                            <TableRow key={rowIndex}>
                                {headers.map((header) => (
                                    <TableCell key={`${rowIndex}-${header}`}>{formatValue(row[header])}</TableCell>
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
                {airlines.map((airline, index) => (
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
                                title={airline.name || "Unknown Airline"}
                                subheader={airline.iata ? `IATA: ${airline.iata}` : null}
                            />
                            <CardContent>
                                <Stack component="ul" spacing={1} sx={{ listStyle: "none", p: 0, m: 0 }}>
                                    {Object.entries(airline.network_stats || {}).map(([key, value]) => (
                                        <Box
                                            key={`${airline.name}-${key}`}
                                            component="li"
                                            sx={{ color: "text.secondary", fontSize: "0.95rem" }}
                                        >
                                            <strong>{key}:</strong> {formatNetworkStat(key, value)}
                                        </Box>
                                    ))}
                                </Stack>
                            </CardContent>
                        </Card>
                    </Grid>
                ))}
            </Grid>
        </Stack>
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
                                <Typography variant="h6" sx={{ fontWeight: 600 }}>
                                    {entry.airline || "Airline"}
                                </Typography>
                                <Typography variant="body2" color="text.secondary">
                                    Based on the airline&apos;s active U.S. network and CBSA coverage.
                                </Typography>
                            </Box>
                            <Grid container spacing={2.5}>
                                <Grid item xs={12} md={6}>
                                    <Typography variant="subtitle2" color="text.secondary" sx={{ mb: 1 }}>
                                        Top CBSA Routes
                                    </Typography>
                                    <DataTable rows={bestRoutes} title="Top CBSA Routes" maxHeight={320} />
                                </Grid>
                                <Grid item xs={12} md={6}>
                                    <Typography variant="subtitle2" color="text.secondary" sx={{ mb: 1 }}>
                                        Potential CBSA Routes
                                    </Typography>
                                    <DataTable rows={potentialRoutes} title="Potential CBSA Routes" maxHeight={420} />
                                </Grid>
                            </Grid>
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
    const debounceRef = React.useRef(null);
    const lastQueryRef = React.useRef("");

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

    const resetResults = () => {
        setMessages([]);
        setComparison(null);
        setCbsaResults([]);
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
                <Toolbar sx={{ minHeight: 88 }}>
                    <Box>
                        <Typography variant="h5" fontWeight={700}>
                            Airline Route Optimizer
                        </Typography>
                        <Typography variant="body1" color="text.secondary">
                            Compare airline networks and surface CBSA-aligned opportunities.
                        </Typography>
                    </Box>
                </Toolbar>
            </AppBar>

            <Container maxWidth="xl" sx={{ py: { xs: 3, md: 5 } }}>
                <Grid container spacing={3} alignItems="stretch">
                    <Grid item xs={12} md={5} lg={4}>
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

                            {comparison && (
                                <Box sx={{ mb: 1 }}>
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
                            )}

                            <CbsaOpportunities entries={cbsaResults} />
                        </Paper>
                    </Grid>
                </Grid>
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
