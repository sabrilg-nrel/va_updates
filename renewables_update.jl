
using XLSX, DataFrames, StringDistances, CSV

# ── 1. Load data ──────────────────────────────────────────────────────────────
eia_2_pf_mapping = DataFrame(XLSX.readtable("/Users/sabrilg/Documents/GitHub/va_updates/EIA2PF.xlsx", "EIA2PF"))
mmwg_data_full   = DataFrame(XLSX.readtable("/Users/sabrilg/Documents/GitHub/va_updates/mmwg-2023-series-data-dictionary.xlsx", "ERAG"))
eia_plants       = DataFrame(XLSX.readtable("/Users/sabrilg/Documents/GitHub/va_updates/2___Plant_Y2024.xlsx", "Plant", first_row = 2))

VA_ei_gens = CSV.read(joinpath(homedir(), "Downloads", "virginia_resources.csv"), DataFrame)
MD_ei_gens = CSV.read(joinpath(homedir(), "Downloads", "maryland_resources.csv"), DataFrame)
WV_ei_gens = CSV.read(joinpath(homedir(), "Downloads", "west_virginia_resources.csv"), DataFrame)

# ── 2. Prepare EIA slim (done once, shared across states) ────────────────────
eia_locations = DataFrames.select(eia_plants, [
    "Plant Code", "Plant Name", "Street Address", "City",
    "State", "Zip", "County", "Latitude", "Longitude",
])
# Normalize Plant Code to Int for joining
eia_locations[!, "Plant Code"] = [ismissing(x) ? missing : Int(x) for x in eia_locations[!, "Plant Code"]]

cap_col = filter(c -> occursin("apacit", c), names(eia_2_pf_mapping))[1]
eia_slim = DataFrames.select(eia_2_pf_mapping, [
    "Plant ID", "Plant Name", "State", "County",
    "Generator ID", "Technology", "Prime Mover Code",
    "BusID", "BusName", "kV", cap_col,
])
DataFrames.rename!(eia_slim, cap_col => "eia_capacity_mw")

# Normalize Plant ID to Int for joining
eia_slim[!, "Plant ID"] = [ismissing(x) ? missing : Int(x) for x in eia_slim[!, "Plant ID"]]

# Join lat/lon from eia_locations into eia_slim on Plant ID = Plant Code
eia_slim = leftjoin(
    eia_slim,
    DataFrames.select(eia_locations, ["Plant Code", "Latitude", "Longitude"]),
    on = "Plant ID" => "Plant Code",
    matchmissing = :notequal,
)

DataFrames.rename!(eia_slim, "Latitude" => "eia_lat", "Longitude" => "eia_lon")

# ── Check match quality ───────────────────────────────────────────────────────
n_with_loc    = count(!ismissing, eia_slim[!, "eia_lat"])
n_without_loc = count(ismissing,  eia_slim[!, "eia_lat"])
println("\nEIA slim rows:          ", nrow(eia_slim))
println("  ✅ With lat/lon:       ", n_with_loc)
println("  ⚠️  Missing lat/lon:   ", n_without_loc)


# ── 3. Prepare MMWG slim (done once, shared across states) ───────────────────
DataFrames.rename!(mmwg_data_full, [c => strip(c) for c in names(mmwg_data_full)])
mmwg_slim = DataFrames.select(mmwg_data_full, [
    "Bus Number", "Load Flow  Bus Name", "English Name", "EIA Plant Code", "Bus kV",
    "Region/PC",   # ← add this for filtering
    "AreaName",    # ← optional: useful for debugging
])
mmwg_slim[!, "Bus Number"] = [ismissing(x) ? missing : Int(x) for x in mmwg_slim[!, "Bus Number"]]

# ── State → Region/PC mapping ─────────────────────────────────────────────────
const STATE_REGION = Dict(
    "VA" => "PJM",
    "MD" => "PJM",
    "WV" => "PJM",
)
# ── 4. Parse BusID — unique() collapses repeated circuits to same bus ─────────
function parse_all_busids(x)
    ismissing(x) && return Int[]
    isnothing(x) && return Int[]
    s = strip(string(x))
    isempty(s) && return Int[]
    matches = [tryparse(Int, m.match) for m in eachmatch(r"\d+", s)]
    filtered = filter(!isnothing, matches)
    return unique(filtered)
end

# ── 5. Expand EIA so each BusID gets its own row (done once) ─────────────────
eia_expanded = DataFrame()
for row in eachrow(eia_slim)
    bus_ids = parse_all_busids(row["BusID"])
    if isempty(bus_ids)
        new_row = copy(DataFrame(row))
        new_row[!, "BusID_int"] = [missing]
        append!(eia_expanded, new_row, promote=true)
    else
        for bid in bus_ids
            new_row = copy(DataFrame(row))
            new_row[!, "BusID_int"] = [bid]
            append!(eia_expanded, new_row, promote=true)
        end
    end
end
println("EIA slim rows: ", nrow(eia_slim), " → expanded: ", nrow(eia_expanded))

# ── 6. Column sets ────────────────────────────────────────────────────────────
const KEY_COLS = [
    "name", "bus_number", "bus", "generator_type", "prime_mover_type",
    "rating", "eia_capacity_mw", "ts_column_name",
    "lat", "lon", "plant_name",
    "Plant ID", "Plant Name", "Technology", "Prime Mover Code", "BusName", "kV",
]
const UNMATCHED_COLS = [
    "name", "bus_number", "bus", "generator_type", "prime_mover_type",
    "rating", "ts_column_name", "lat", "lon", "plant_name",
]
const MMWG_COLS = [
    "name", "bus_number", "bus", "rating", "ts_column_name",
    "Load Flow  Bus Name", "English Name", "EIA Plant Code", "Bus kV",
]

# ── 7. Match EI → EIA2PF ─────────────────────────────────────────────────────
function match_ei_to_eia(ei_gens::DataFrame, eia_expanded::DataFrame, state::String)
    ei = copy(ei_gens)
    ei.bus_number = [ismissing(x) ? missing : Int(x) for x in ei.bus_number]

    eia_state = filter(row -> coalesce(row["State"] == state, false) &&
                              coalesce(row["Prime Mover Code"] == "PV", false), eia_expanded)
    DataFrames.rename!(eia_state, "BusID_int" => "BusID_int_join")

    println("\n$state: EIA PV rows after expansion: ", nrow(eia_state))

    bus_counts = combine(groupby(eia_state, "BusID_int_join"), nrow => :n_eia_entries)
    multi = filter(row -> coalesce(row[:n_eia_entries] > 1, false), bus_counts)
    if nrow(multi) > 0
        println("  ⚠️  Buses with multiple EIA entries (many-to-one):")
        show(multi, allrows=true)
    end

    result = leftjoin(ei, eia_state, on = "bus_number" => "BusID_int_join", matchmissing = :notequal)

    matched   = filter(row -> !ismissing(row["Plant ID"]), result)
    unmatched = filter(row ->  ismissing(row["Plant ID"]), result)

    println("\n", "="^60)
    println("$state EI Generators — EIA2PF Bus Match Summary")
    println("="^60)
    println("Total $state EI generators:    ", nrow(ei))
    println("  ✅ Matched to EIA2PF bus:    ", nrow(matched))
    println("  ⚠️  No EIA2PF bus match:      ", nrow(unmatched))
    println("─"^60)
    println("Solar generators in $state EI:")
    println("  Total:        ", count(==("Solar"), ei.generator_type))
    println("  ✅ Matched:   ", count(==("Solar"), matched.generator_type))
    println("  ⚠️  Unmatched: ", count(==("Solar"), unmatched.generator_type))
    println("="^60)

    return result
end


# ── 8. Summary + display matched/unmatched solar ──────────────────────────────
function summarize_solar(label::String, result::DataFrame)
    # Matched = any EI gen that found an EIA PV entry (regardless of EI classification)
    matched   = filter(row -> !ismissing(row["Plant ID"]), result)
    # Unmatched = Solar-classified EI gens with no EIA match (may miss misclassified ones)
    unmatched = filter(row -> ismissing(row["Plant ID"]) && row["generator_type"] == "Solar", result)

    println("\n", "="^60)
    println("$label — Solar Matched vs Unmatched")
    println("="^60)
    println("  ✅ Matched (any EI type → EIA PV):  ", nrow(matched))
    println("  ⚠️  Unmatched Solar EI gens:          ", nrow(unmatched))
    println("─"^60)
    println("  Matched by EI generator_type (misclassification check):")
    for gt in sort(unique(skipmissing(matched.generator_type)))
        n = count(==(gt), skipmissing(matched.generator_type))
        println("    $gt: $n")
    end
    println("="^60)

    println("\n📋 $label matched generators (", nrow(matched), " rows):")
    show(sort(DataFrames.select(matched, KEY_COLS), "bus_number"), allrows=true)

    println("\n⚠️  $label unmatched Solar EI generators (", nrow(unmatched), " rows):")
    show(sort(DataFrames.select(unmatched, UNMATCHED_COLS), "bus_number"), allrows=true)

    return matched, unmatched
end

# ── 9. MMWG fallback lookup for unmatched solar ───────────────────────────────
function mmwg_lookup(label::String, unmatched::DataFrame, mmwg_slim::DataFrame)
    result = leftjoin(unmatched, mmwg_slim,
                      on = "bus_number" => "Bus Number", matchmissing = :notequal)

    mmwg_matched   = filter(row -> !ismissing(row["EIA Plant Code"]), result)
    mmwg_unmatched = filter(row ->  ismissing(row["EIA Plant Code"]), result)

    println("\n", "="^60)
    println("$label — Unmatched Solar → MMWG Fallback")
    println("="^60)
    println("  ✅ Found in MMWG:    ", nrow(mmwg_matched))
    println("  ⚠️  Still unmatched: ", nrow(mmwg_unmatched))
    println("="^60)

    println("\n📋 $label found in MMWG (", nrow(mmwg_matched), " rows):")
    show(sort(DataFrames.select(mmwg_matched, MMWG_COLS), "bus_number"), allrows=true)

    println("\n⚠️  $label still unmatched after MMWG (", nrow(mmwg_unmatched), " rows):")
    show(sort(DataFrames.select(mmwg_unmatched, UNMATCHED_COLS), "bus_number"), allrows=true)

    return mmwg_matched, mmwg_unmatched
end

# Final output columns
const OUTPUT_COLS = [
    "state",
    "name",            # EI generator name
    "lat", "lon",      # coordinates
    "bus_number",      # bus ID
    "bus",             # bus name (EI)
    "rating",          # EI capacity (MW)
    "eia_capacity_mw", # EIA nameplate capacity (MW)
    "kV",              # bus voltage
    "Plant ID", "Plant Name",
    "source",          # "EIA2PF" or "MMWG"
]

# ── 10. Run for each state and store results ──────────────────────────────────
va_ei_eia_result   = match_ei_to_eia(VA_ei_gens, eia_expanded, "VA")
md_ei_eia_result   = match_ei_to_eia(MD_ei_gens, eia_expanded, "MD")
wv_ei_eia_result   = match_ei_to_eia(WV_ei_gens, eia_expanded, "WV")

# ── EIA2PF matched/unmatched solar ───────────────────────────────────────────
va_solar_matched,   va_solar_unmatched   = summarize_solar("VA", va_ei_eia_result)
md_solar_matched,   md_solar_unmatched   = summarize_solar("MD", md_ei_eia_result)
wv_solar_matched,   wv_solar_unmatched   = summarize_solar("WV", wv_ei_eia_result)

# ── MMWG fallback for unmatched solar ────────────────────────────────────────
va_mmwg_matched,   va_still_unmatched   = mmwg_lookup("VA", va_solar_unmatched, mmwg_slim)
md_mmwg_matched,   md_still_unmatched   = mmwg_lookup("MD", md_solar_unmatched, mmwg_slim)
wv_mmwg_matched,   wv_still_unmatched   = mmwg_lookup("WV", wv_solar_unmatched, mmwg_slim)

println("\n", "="^60)
println("📊 Final Summary by State")
println("="^60)
for (label, matched, mmwg, unmatched) in [
    ("VA", va_solar_matched, va_mmwg_matched, va_still_unmatched),
    ("MD", md_solar_matched, md_mmwg_matched, md_still_unmatched),
    ("WV", wv_solar_matched, wv_mmwg_matched, wv_still_unmatched),
]
    println("\n$label:")
    println("  ✅ EIA2PF matched:      ", nrow(matched))
    println("  ✅ MMWG matched:        ", nrow(mmwg))
    println("  ⚠️  Still unmatched:    ", nrow(unmatched))
end
println("="^60)

# ── 11. Find EIA solar plants not matched to any EI generator ─────────────────
function find_eia_unmatched(label::String, ei_eia_result::DataFrame, eia_expanded::DataFrame, state::String)

    matched_buses = Set(skipmissing(ei_eia_result[!, "bus_number"]))

    eia_state_pv = filter(row -> coalesce(row["State"] == state, false) &&
                                 coalesce(row["Prime Mover Code"] == "PV", false), eia_expanded)

    eia_unmatched = filter(row -> !in(coalesce(row["BusID_int"], -1), matched_buses), eia_state_pv)

    has_both    = filter(row -> !ismissing(row["BusID_int"]) && !ismissing(row["BusName"]) && row["BusName"] != "", eia_unmatched)
    has_neither = filter(row -> ismissing(row["BusID_int"]) && (ismissing(row["BusName"]) || row["BusName"] == ""), eia_unmatched)

    println("\n", "="^60)
    println("$label — EIA Solar Plants NOT in EI model")
    println("="^60)
    println("  Total EIA PV entries for $label:      ", nrow(eia_state_pv))
    println("  ⚠️  Not matched to any EI generator:  ", nrow(eia_unmatched))
    println("─"^60)
    println("  Classification:")
    println("    ✅ Has BusID + BusName:  ", nrow(has_both))
    println("    ❌ Has neither:          ", nrow(has_neither))
    println("="^60)

    EIA_UNMATCHED_COLS = [
        "Plant ID", "Plant Name", "Generator ID",
        "BusID_int", "BusName", "kV",
        "eia_capacity_mw", "Technology", "Prime Mover Code",
    ]

    println("\n  ✅ Has BusID + BusName (", nrow(has_both), " rows):")
    show(DataFrames.select(has_both, EIA_UNMATCHED_COLS), allrows=true)

    println("\n  ❌ Has neither BusID nor BusName (", nrow(has_neither), " rows):")
    show(DataFrames.select(has_neither, EIA_UNMATCHED_COLS), allrows=true)

    return eia_unmatched, has_both, has_neither
end

# ── 12. Run for each state ────────────────────────────────────────────────────
va_eia_unmatched, va_eia_has_both, va_eia_neither =
    find_eia_unmatched("VA", va_ei_eia_result, eia_expanded, "VA");

md_eia_unmatched, md_eia_has_both, md_eia_neither =
    find_eia_unmatched("MD", md_ei_eia_result, eia_expanded, "MD");

wv_eia_unmatched, wv_eia_has_both, wv_eia_neither =
    find_eia_unmatched("WV", wv_ei_eia_result, eia_expanded, "WV");


# ── 13. Match EIA "neither" plants to MMWG by Plant Name ≈ English Name ───────
function match_neither_to_mmwg(label::String, neither::DataFrame, mmwg_slim::DataFrame)

    println("\n", "="^60)
    println("$label — EIA 'neither' plants → MMWG name match")
    println("="^60)

    # Filter MMWG to the state's region first
    region = get(STATE_REGION, label, missing)
    mmwg_region = if !ismissing(region)
        println("  Filtering MMWG to Region/PC = $region ($(nrow(filter(r -> coalesce(r["Region/PC"] == region, false), mmwg_slim))) rows)")
        filter(r -> coalesce(r["Region/PC"] == region, false), mmwg_slim)
    else
        mmwg_slim
    end

    normalize(s) = ismissing(s) ? "" : lowercase(strip(string(s)))

    matches  = DataFrame()
    no_match = DataFrame()

    for row in eachrow(neither)
        plant_name = normalize(row["Plant Name"])
        mmwg_match = filter(r -> normalize(r["English Name"]) == plant_name, mmwg_region)

        if nrow(mmwg_match) > 0
            new_row = copy(DataFrame(row))
            new_row[!, "Bus Number"]          .= mmwg_match[1, "Bus Number"]
            new_row[!, "Load Flow  Bus Name"] .= mmwg_match[1, "Load Flow  Bus Name"]
            new_row[!, "English Name"]        .= mmwg_match[1, "English Name"]
            new_row[!, "EIA Plant Code"]      .= mmwg_match[1, "EIA Plant Code"]
            new_row[!, "Bus kV"]              .= mmwg_match[1, "Bus kV"]
            new_row[!, "Region/PC"]           .= mmwg_match[1, "Region/PC"]
            append!(matches, new_row, promote=true)
        else
            append!(no_match, DataFrame(row), promote=true)
        end
    end

    println("  Total 'neither':          ", nrow(neither))
    println("  ✅ Matched via name:      ", nrow(matches))
    println("  ⚠️  Still no match:       ", nrow(no_match))
    println("="^60)

    MATCH_COLS = [
        "Plant ID", "Plant Name", "Generator ID", "eia_capacity_mw",
        "Bus Number", "Load Flow  Bus Name", "English Name", "EIA Plant Code", "Bus kV", "Region/PC",
    ]
    NO_MATCH_COLS = [
        "Plant ID", "Plant Name", "Generator ID", "eia_capacity_mw",
    ]

    println("\n  ✅ Matched (", nrow(matches), " rows):")
    show(DataFrames.select(matches, MATCH_COLS), allrows=true)

    println("\n  ⚠️  Still no match (", nrow(no_match), " rows):")
    show(DataFrames.select(no_match, NO_MATCH_COLS), allrows=true)

    return matches, no_match
end

# ── 14. Run for each state ────────────────────────────────────────────────────
va_neither_mmwg_matched, va_neither_still_unmatched =
    match_neither_to_mmwg("VA", va_eia_neither, mmwg_slim);

md_neither_mmwg_matched, md_neither_still_unmatched =
    match_neither_to_mmwg("MD", md_eia_neither, mmwg_slim);

wv_neither_mmwg_matched, wv_neither_still_unmatched =
    match_neither_to_mmwg("WV", wv_eia_neither, mmwg_slim);

# ── 15. Fuzzy name match for still-unmatched plants ───────────────────────────
function fuzzy_match_to_mmwg(label::String, still_unmatched::DataFrame, mmwg_slim::DataFrame;
    threshold::Float64 = 0.7)

println("\n", "="^60)
println("$label — Fuzzy name match → MMWG (threshold = $threshold)")
println("="^60)

# Filter MMWG to state region
region = get(STATE_REGION, label, missing)
mmwg_region = if !ismissing(region)
filter(r -> coalesce(r["Region/PC"] == region, false), mmwg_slim)
else
mmwg_slim
end

normalize(s) = ismissing(s) ? "" : lowercase(strip(string(s)))

mmwg_names    = [normalize(r["English Name"]) for r in eachrow(mmwg_region)]
mmwg_bus_nums = mmwg_region[!, "Bus Number"]
mmwg_bus_kv   = mmwg_region[!, "Bus kV"]
mmwg_lf_names = mmwg_region[!, "Load Flow  Bus Name"]
mmwg_eia_code = mmwg_region[!, "EIA Plant Code"]

fuzzy_matched   = DataFrame()
fuzzy_unmatched = DataFrame()

for row in eachrow(still_unmatched)
plant_name = normalize(row["Plant Name"])

# Score all MMWG English Names against this plant name
scores = [compare(plant_name, mn, Jaro()) for mn in mmwg_names]
best_idx = argmax(scores)
best_score = scores[best_idx]

if best_score >= threshold
new_row = copy(DataFrame(row))
new_row[!, "Bus Number"]          .= mmwg_bus_nums[best_idx]
new_row[!, "Load Flow  Bus Name"] .= mmwg_lf_names[best_idx]
new_row[!, "English Name"]        .= mmwg_region[best_idx, "English Name"]
new_row[!, "EIA Plant Code"]      .= mmwg_eia_code[best_idx]
new_row[!, "Bus kV"]              .= mmwg_bus_kv[best_idx]
new_row[!, "match_score"]         .= round(best_score, digits=3)
new_row[!, "Region/PC"]           .= mmwg_region[best_idx, "Region/PC"]
append!(fuzzy_matched, new_row, promote=true)
else
new_row = copy(DataFrame(row))
new_row[!, "best_mmwg_name"]  .= mmwg_region[best_idx, "English Name"]
new_row[!, "match_score"]     .= round(best_score, digits=3)
append!(fuzzy_unmatched, new_row, promote=true)
end
end

println("  Total still unmatched:        ", nrow(still_unmatched))
    println("  ✅ Fuzzy matched (≥$threshold): ", nrow(fuzzy_matched))
    println("  ⚠️  No fuzzy match:             ", nrow(fuzzy_unmatched))
    println("="^60)

    FUZZY_MATCH_COLS = [
        "Plant ID", "Plant Name", "Generator ID", "eia_capacity_mw",
        "Bus Number", "English Name", "Bus kV", "Region/PC", "match_score",
    ]
    FUZZY_NO_MATCH_COLS = [
        "Plant ID", "Plant Name", "Generator ID", "eia_capacity_mw",
        "best_mmwg_name", "match_score",
    ]

    println("\n  ✅ Fuzzy matched (", nrow(fuzzy_matched), " rows):")
    if nrow(fuzzy_matched) > 0
        show(DataFrames.select(fuzzy_matched, FUZZY_MATCH_COLS), allrows=true)
    else
        println("  (none)")
    end

    println("\n  ⚠️  No fuzzy match (", nrow(fuzzy_unmatched), " rows):")
    if nrow(fuzzy_unmatched) > 0
        show(DataFrames.select(fuzzy_unmatched, FUZZY_NO_MATCH_COLS), allrows=true)
    else
        println("  (none)")
    end

    return fuzzy_matched, fuzzy_unmatched
end

# ── 16. Run fuzzy match for each state ───────────────────────────────────────
va_fuzzy_matched, va_fuzzy_unmatched =
fuzzy_match_to_mmwg("VA", va_neither_still_unmatched, mmwg_slim);

md_fuzzy_matched, md_fuzzy_unmatched =
fuzzy_match_to_mmwg("MD", md_neither_still_unmatched, mmwg_slim);

wv_fuzzy_matched, wv_fuzzy_unmatched =
fuzzy_match_to_mmwg("WV", wv_neither_still_unmatched, mmwg_slim);


# ── 17. Build final per-state solar DataFrames ────────────────────────────────
function build_state_solar_df(label::String,
    solar_matched::DataFrame,   # EI → EIA2PF matched
    mmwg_matched::DataFrame)    # EI unmatched → MMWG matched

# ── Source 1: EI → EIA2PF matched ────────────────────────────────────────
s1 = DataFrame(
source          = fill("EI_EIA2PF", nrow(solar_matched)),
gen_name        = solar_matched.name,
ts_column_name  = solar_matched.ts_column_name,
ei_lat          = solar_matched.lat,
ei_lon          = solar_matched.lon,
eia_lat         = solar_matched[!, "eia_lat"],
eia_lon         = solar_matched[!, "eia_lon"],
bus_id          = solar_matched.bus_number,
bus_name        = solar_matched.bus,
ei_capacity_mw  = solar_matched.rating,
eia_capacity_mw = solar_matched.eia_capacity_mw,
bus_voltage_kv  = solar_matched.kV,
)

# ── Source 2: EI unmatched → MMWG matched ────────────────────────────────
s2 = DataFrame(
source          = fill("EI_MMWG", nrow(mmwg_matched)),
gen_name        = mmwg_matched.name,
ts_column_name  = mmwg_matched.ts_column_name,
ei_lat          = mmwg_matched.lat,
ei_lon          = mmwg_matched.lon,
eia_lat         = fill(missing, nrow(mmwg_matched)),
eia_lon         = fill(missing, nrow(mmwg_matched)),
bus_id          = mmwg_matched.bus_number,
bus_name        = mmwg_matched.bus,
ei_capacity_mw  = mmwg_matched.rating,
eia_capacity_mw = fill(missing, nrow(mmwg_matched)),
bus_voltage_kv  = mmwg_matched[!, "Bus kV"],
)

final_df = vcat(s1, s2, cols=:union)
final_df[!, "state"] .= label

println("\n", "="^60)
println("$label — Final Solar DataFrame (EI generators only)")
println("="^60)
println("  EI_EIA2PF:  ", nrow(s1), " generators")
println("  EI_MMWG:    ", nrow(s2), " generators")
println("  Total:      ", nrow(final_df), " generators")
println("="^60)
show(sort(final_df, :bus_id), allrows=true)

return final_df
end

# ── 18. Build per-state DataFrames ───────────────────────────────────────────
va_solar = build_state_solar_df("VA", va_solar_matched, va_mmwg_matched);
md_solar = build_state_solar_df("MD", md_solar_matched, md_mmwg_matched);
wv_solar = build_state_solar_df("WV", wv_solar_matched, wv_mmwg_matched);

function clean_bus_voltage(x)
    ismissing(x) && return missing
    s = strip(string(x))
    # Extract first number found
    m = match(r"[\d.]+", s)
    isnothing(m) && return missing
    val = tryparse(Float64, m.match)
    return val
end

va_solar[!, "bus_voltage_kv"] = [clean_bus_voltage(x) for x in va_solar[!, "bus_voltage_kv"]];
md_solar[!, "kV"] = [clean_bus_voltage(x) for x in md_solar[!, "kV"]];
wv_solar[!, "kV"] = [clean_bus_voltage(x) for x in wv_solar[!, "kV"]];

# ── Helper: generate synthetic EI-style generator name ───────────────────────
function make_gen_name(bus_id::Any, idx::Int)
    bus    = ismissing(bus_id) ? string(rand(1000000000:9999999999)) : string(Int(bus_id))
    suffix = string(rand(1000000000:9999999999))
    return "generator-$(bus)-$(suffix)"
end

# ── Helpers: clean bus_name and bus_voltage from EIA raw strings ──────────────
function clean_bus_name(x)
    ismissing(x) && return missing
    s = strip(string(x))
    # Remove outer brackets and quotes: ['A', 'B'] → A, B
    s = replace(s, r"^\[|\]$" => "")          # remove [ ]
    s = replace(s, r"'" => "")                 # remove '
    s = strip(s)
    return s
end


function build_eia_only_solar_df(label::String,
                                  eia_has_both::DataFrame,
                                  neither_mmwg_matched::DataFrame,
                                  fuzzy_matched::DataFrame)

    # ── Source 3: EIA has BusID+BusName, not in EI ───────────────────────────
    s3 = DataFrame(
        source          = fill("EIA_ONLY", nrow(eia_has_both)),
        gen_name        = [make_gen_name(eia_has_both[i, "BusID_int"], i) for i in 1:nrow(eia_has_both)],
        ts_column_name  = fill(missing, nrow(eia_has_both)),
        ei_lat          = fill(missing, nrow(eia_has_both)),
        ei_lon          = fill(missing, nrow(eia_has_both)),
        eia_lat         = eia_has_both[!, "eia_lat"],
        eia_lon         = eia_has_both[!, "eia_lon"],
        bus_id          = eia_has_both[!, "BusID_int"],
        bus_name        = [clean_bus_name(x) for x in eia_has_both[!, "BusName"]],
        ei_capacity_mw  = fill(missing, nrow(eia_has_both)),
        eia_capacity_mw = eia_has_both[!, "eia_capacity_mw"],
        bus_voltage_kv  = [clean_bus_voltage(x) for x in eia_has_both[!, "kV"]],
    )

    # ── Source 4: EIA neither → MMWG exact name match ────────────────────────
    s4 = DataFrame(
        source          = fill("EIA_MMWG_EXACT", nrow(neither_mmwg_matched)),
        gen_name        = [make_gen_name(neither_mmwg_matched[i, "Bus Number"], i)
                           for i in 1:nrow(neither_mmwg_matched)],
        ts_column_name  = fill(missing, nrow(neither_mmwg_matched)),
        ei_lat          = fill(missing, nrow(neither_mmwg_matched)),
        ei_lon          = fill(missing, nrow(neither_mmwg_matched)),
        eia_lat         = neither_mmwg_matched[!, "eia_lat"],
        eia_lon         = neither_mmwg_matched[!, "eia_lon"],
        bus_id          = neither_mmwg_matched[!, "Bus Number"],
        bus_name        = neither_mmwg_matched[!, "Load Flow  Bus Name"],
        ei_capacity_mw  = fill(missing, nrow(neither_mmwg_matched)),
        eia_capacity_mw = neither_mmwg_matched[!, "eia_capacity_mw"],
        bus_voltage_kv  = neither_mmwg_matched[!, "Bus kV"],
    )

    # ── Source 5: EIA neither → MMWG fuzzy match ─────────────────────────────
    s5 = DataFrame(
        source          = fill("EIA_MMWG_FUZZY", nrow(fuzzy_matched)),
        gen_name        = [make_gen_name(fuzzy_matched[i, "Bus Number"], i)
                           for i in 1:nrow(fuzzy_matched)],
        ts_column_name  = fill(missing, nrow(fuzzy_matched)),
        ei_lat          = fill(missing, nrow(fuzzy_matched)),
        ei_lon          = fill(missing, nrow(fuzzy_matched)),
        eia_lat         = fuzzy_matched[!, "eia_lat"],
        eia_lon         = fuzzy_matched[!, "eia_lon"],
        bus_id          = fuzzy_matched[!, "Bus Number"],
        bus_name        = fuzzy_matched[!, "English Name"],
        ei_capacity_mw  = fill(missing, nrow(fuzzy_matched)),
        eia_capacity_mw = fuzzy_matched[!, "eia_capacity_mw"],
        bus_voltage_kv  = fuzzy_matched[!, "Bus kV"],
    )

    final_df = vcat(s3, s4, s5, cols=:union)
    final_df[!, "state"] .= label

    # Sort by source priority: EIA_ONLY → EIA_MMWG_EXACT → EIA_MMWG_FUZZY
    source_order = Dict("EIA_ONLY" => 1, "EIA_MMWG_EXACT" => 2, "EIA_MMWG_FUZZY" => 3)
    final_df[!, "source_order"] = [source_order[s] for s in final_df.source]
    sort!(final_df, [:source_order, :bus_id])
    select!(final_df, Not(:source_order))

    println("\n", "="^60)
    println("$label — EIA-only Solar DataFrame (not in EI model)")
    println("="^60)
    println("  EIA_ONLY:       ", nrow(s3), " generators")
    println("  EIA_MMWG_EXACT: ", nrow(s4), " generators")
    println("  EIA_MMWG_FUZZY: ", nrow(s5), " generators")
    println("  Total:          ", nrow(final_df), " generators")
    println("="^60)
    show(final_df, allrows=true)

    return final_df
end

# ── Run for each state ────────────────────────────────────────────────────────
va_solar_eia_only = build_eia_only_solar_df("VA", va_eia_has_both, va_neither_mmwg_matched, va_fuzzy_matched);
md_solar_eia_only = build_eia_only_solar_df("MD", md_eia_has_both, md_neither_mmwg_matched, md_fuzzy_matched);
wv_solar_eia_only = build_eia_only_solar_df("WV", wv_eia_has_both, wv_neither_mmwg_matched, wv_fuzzy_matched);

function build_export_df(ei_df::DataFrame, eia_only_df::DataFrame, label::String)

    println("DEBUG $label ei_df cols: ", names(ei_df))

    # Detect which column naming convention is used
    has_gen_name = "gen_name" in names(ei_df)

    ei_out = if nrow(ei_df) == 0
        DataFrame(gen_name=String[], bus_id=Int[], bus_name=String[],
                  lat=Float64[], lon=Float64[], capacity_mw=Any[],
                  bus_voltage_kv=Float64[], source=String[], state=String[])
    elseif has_gen_name
        DataFrame(
            gen_name       = ei_df[!, "gen_name"],
            bus_id         = ei_df[!, "bus_id"],
            bus_name       = ei_df[!, "bus_name"],
            lat            = coalesce.(ei_df[!, "eia_lat"], ei_df[!, "ei_lat"]),
            lon            = coalesce.(ei_df[!, "eia_lon"], ei_df[!, "ei_lon"]),
            capacity_mw    = coalesce.(ei_df[!, "eia_capacity_mw"], ei_df[!, "ei_capacity_mw"]),
            bus_voltage_kv = ei_df[!, "bus_voltage_kv"],
            source         = ei_df[!, "source"],
            state          = fill(label, nrow(ei_df)),
        )
    else
        # Original EI column names (md_solar, wv_solar)
        DataFrame(
            gen_name       = ei_df[!, "name"],
            bus_id         = ei_df[!, "bus_number"],
            bus_name       = ei_df[!, "bus"],
            lat            = "eia_lat" in names(ei_df) ?
                             coalesce.(ei_df[!, "eia_lat"], ei_df[!, "lat"]) :
                             ei_df[!, "lat"],
            lon            = "eia_lon" in names(ei_df) ?
                             coalesce.(ei_df[!, "eia_lon"], ei_df[!, "lon"]) :
                             ei_df[!, "lon"],
            capacity_mw    = "eia_capacity_mw" in names(ei_df) ?
                             coalesce.(ei_df[!, "eia_capacity_mw"], ei_df[!, "rating"]) :
                             ei_df[!, "rating"],
            bus_voltage_kv = [clean_bus_voltage(x) for x in ei_df[!, "kV"]],
            source         = ei_df[!, "source"],
            state          = fill(label, nrow(ei_df)),
        )
    end

    # ── EIA-only df columns ───────────────────────────────────────────────────
    eia_out = DataFrame(
        gen_name       = eia_only_df[!, "gen_name"],
        bus_id         = eia_only_df[!, "bus_id"],
        bus_name       = eia_only_df[!, "bus_name"],
        lat            = eia_only_df[!, "eia_lat"],
        lon            = eia_only_df[!, "eia_lon"],
        capacity_mw    = eia_only_df[!, "eia_capacity_mw"],
        bus_voltage_kv = eia_only_df[!, "bus_voltage_kv"],
        source         = eia_only_df[!, "source"],
        state          = fill(label, nrow(eia_only_df)),
    )

    combined = vcat(ei_out, eia_out, cols=:union)
    combined = sort(combined, :bus_id)

    println("\n", "="^60)
    println("$label — Export Summary")
    println("="^60)
    println("  EI generators:    ", nrow(ei_out))
    println("  EIA-only:         ", nrow(eia_out))
    println("  Total:            ", nrow(combined))
    for src in unique(skipmissing(combined.source))
        println("    $(src): ", count(==(src), combined.source))
    end
    println("="^60)

    return combined
end

# ── Build export DataFrames ───────────────────────────────────────────────────
va_export = build_export_df(va_solar, va_solar_eia_only, "VA")
md_export = build_export_df(md_solar, md_solar_eia_only, "MD")
wv_export = build_export_df(wv_solar, wv_solar_eia_only, "WV")

# ── Write CSVs ────────────────────────────────────────────────────────────────
const OUTPUT_DIR = "/Users/sabrilg/Documents/GitHub/va_updates/"

CSV.write(joinpath(OUTPUT_DIR, "solar_RE_VA.csv"), va_export)
CSV.write(joinpath(OUTPUT_DIR, "solar_RE_MD.csv"), md_export)
CSV.write(joinpath(OUTPUT_DIR, "solar_RE_WV.csv"), wv_export)

println("\n✅ CSVs written:")
println("  → solar_RE_VA.csv (", nrow(va_export), " rows)")
println("  → solar_RE_MD.csv (", nrow(md_export), " rows)")
println("  → solar_RE_WV.csv (", nrow(wv_export), " rows)")