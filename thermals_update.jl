# ══════════════════════════════════════════════════════════════════════════════
#                       THERMAL GENERATORS UPDATE SCRIPT
# ══════════════════════════════════════════════════════════════════════════════
# Purpose: Update EI model thermal generators with EIA Form 860 data
# Approach:
#   1. Load EI thermal generators (Coal, Gas, Nuclear, etc.)
#   2. Match to EIA2PF mapping via BusID
#   3. Fuzzy match unmatched to MMWG by plant name
#   4. Apply manual overrides for known corrections
#   5. Export update instructions for each state
# ══════════════════════════════════════════════════════════════════════════════

using XLSX, DataFrames, StringDistances, CSV, PlotlyJS, Printf

# ── Constants ─────────────────────────────────────────────────────────────────
const OUTPUT_DIR = joinpath(homedir(), "Documents/GitHub/va_updates/output")
isdir(OUTPUT_DIR) || mkdir(OUTPUT_DIR)

# ── Manual overrides: Plant ID → Bus Number ───────────────────────────────────
const MANUAL_OVERRIDES = Dict{Int, Union{Int, Missing}}(
    # Add thermal-specific overrides here
    # Example: 12345 => 314078  # Plant Name → Correct Bus
    52089 => 242587 # Celanese Acetate LLC to 5CELAN

)

# ── Thermal generator type mapping ────────────────────────────────────────────
const THERMAL_TYPES = [
    "ThermalStandard",
    "ThermalMultiStart",
    
]

# EIA Prime Mover → PowerSystems.jl type
const PM_THERMAL_MAP = Dict(
    "ST"  => "ThermalStandard",  # Steam turbine
    "GT"  => "ThermalStandard",  # Gas turbine
    "CT"  => "ThermalStandard",  # Combined cycle
    "CA"  => "ThermalStandard",  # Combined cycle steam
    "IC"  => "ThermalStandard",  # Internal combustion
    "CS"  => "ThermalStandard",  # Combined cycle single shaft
    "CC"  => "ThermalStandard",  # Combined cycle
    "CE"  => "ThermalStandard",  # Compressed air
)


# ── 1. Load data ──────────────────────────────────────────────────────────────
println("Loading data...")
eia_2_pf_mapping = DataFrame(XLSX.readtable("EIA2PF.xlsx", "EIA2PF"))
eia_2_pf_mapping = unique(eia_2_pf_mapping)

mmwg_data_full = DataFrame(XLSX.readtable("mmwg-2023-series-data-dictionary.xlsx", "ERAG"))
mmwg_data_full = unique(mmwg_data_full)

eia_plants = DataFrame(XLSX.readtable("2___Plant_Y2024.xlsx", "Plant", first_row=2))

# EI generator exports
VA_ei_gens = CSV.read(joinpath(homedir(), "Downloads", "virginia_resources.csv"), DataFrame)
MD_ei_gens = CSV.read(joinpath(homedir(), "Downloads", "maryland_resources.csv"), DataFrame)
WV_ei_gens = CSV.read(joinpath(homedir(), "Downloads", "west_virginia_resources.csv"), DataFrame)

# ── 2. Filter for thermal generators ──────────────────────────────────────────
function filter_thermal(df::DataFrame)
    filter(r -> coalesce(r.generator_type, "") in THERMAL_TYPES, df)
end

va_thermal = filter_thermal(VA_ei_gens)
md_thermal = filter_thermal(MD_ei_gens)
wv_thermal = filter_thermal(WV_ei_gens)

println("EI thermal generators loaded:")
println("  VA: ", nrow(va_thermal))
println("  MD: ", nrow(md_thermal))
println("  WV: ", nrow(wv_thermal))

# ── 3. Prepare EIA slim (thermal-specific columns) ────────────────────────────
eia_slim = DataFrames.select(
    eia_2_pf_mapping,
    ["uid",                                          # ← critical for deduplication
     "Utility ID", "Utility Name", "Plant ID", "Plant Name", "State", "County",
     "Generator ID", "Technology", "Prime Mover Code", "Nameplate Capacity (MW)",
     "BusID", "BusName", "kV", "Status"]
)
DataFrames.rename!(eia_slim, "Nameplate Capacity (MW)" => "eia_capacity_mw")
eia_slim[!, "Plant ID"]   = [ismissing(x) ? missing : Int(x) for x in eia_slim[!, "Plant ID"]]
eia_slim[!, "Utility ID"] = [ismissing(x) ? missing : Int(x) for x in eia_slim[!, "Utility ID"]]

# ── 4. Join lat/lon from Plant Y2024 ──────────────────────────────────────────
eia_locations = DataFrames.select(eia_plants, ["Plant Code", "Latitude", "Longitude"])
eia_locations[!, "Plant Code"] = [ismissing(x) ? missing : Int(x) for x in eia_locations[!, "Plant Code"]]

eia_slim = leftjoin(
    eia_slim,
    eia_locations,
    on = "Plant ID" => "Plant Code",
    matchmissing = :notequal
)

DataFrames.rename!(eia_slim, "Latitude" => "eia_lat", "Longitude" => "eia_lon")

println("\nEIA thermal data prepared: ", nrow(eia_slim), " rows")
println("  With lat/lon: ", count(!ismissing, eia_slim[!, "eia_lat"]))

# ── 5. Parse BusID column (same as renewables) ────────────────────────────────
function parse_all_busids(x)
    ismissing(x) && return Int[]
    isnothing(x) && return Int[]
    s = strip(string(x))
    isempty(s) && return Int[]
    matches = [tryparse(Int, m.match) for m in eachmatch(r"\d+", s)]
    filtered = filter(!isnothing, matches)
    return unique(filtered)
end

# ── 6. Expand EIA → take FIRST BusID ──────────────────────────────────────────
eia_expanded = DataFrame()
for row in eachrow(eia_slim)
    busids = parse_all_busids(row["BusID"])
    if isempty(busids)
        new_row = copy(DataFrame(row))
        new_row[!, "BusID_int"] .= missing
        append!(eia_expanded, new_row, promote=true)
    else
        new_row = copy(DataFrame(row))
        new_row[!, "BusID_int"] .= first(busids)
        append!(eia_expanded, new_row, promote=true)
    end
end

println("EIA expanded (full): ", nrow(eia_expanded), " rows")

# ── Remove rows already consumed by solar + wind ──────────────────────────────
n_before = nrow(eia_expanded)
eia_expanded = filter(row ->
    ismissing(row["uid"]) || !(string(row["uid"]) in consumed_uids),
    eia_expanded)
println("EIA expanded (thermal only): ", nrow(eia_expanded),
        " rows  (removed ", n_before - nrow(eia_expanded), " renewable rows)")

include("/Users/sabrilg/Documents/GitHub/va_updates/EIA860_comparison.jl")

# ── Verify Status vs eia860_status mapping consistency ────────────────────────
# The purpose of this comparison is to check if the mapping from EIA2PF "Status" codes to our derived "eia860_status" categories is consistent.
println("\n", "="^70)
println("  Status Mapping Verification")
println("="^70)

# Define expected mappings
OPERABLE_CODES = ["OP", "SB", "OA", "OS"]
PROPOSED_CODES = ["L", "V", "U", "P", "T", "TS"]
RETIRED_CODES  = ["RE", "CN", "IP"]

# Create cross-tabulation
status_comparison = combine(
    groupby(eia_expanded, [:Status, :eia860_status]),
    nrow => :count
)

println("\nCross-tabulation (Status vs eia860_status):")
show(sort(status_comparison, [:eia860_status, :Status]), allrows=true)

# Check for mismatches
println("\n\nMismatch Analysis:")

# Operable mismatches
operable_mismatch = filter(r -> 
    coalesce(r.Status, "") in OPERABLE_CODES && 
    r.eia860_status != "Operable",
    status_comparison
)
if nrow(operable_mismatch) > 0
    println("  ⚠️  Status=OP/SB/OA/OS but eia860_status≠Operable:")
    show(operable_mismatch, allrows=true)
else
    println("  ✅ All OP/SB/OA/OS correctly mapped to 'Operable'")
end

# Proposed mismatches
proposed_mismatch = filter(r -> 
    coalesce(r.Status, "") in PROPOSED_CODES && 
    r.eia860_status != "Proposed",
    status_comparison
)
if nrow(proposed_mismatch) > 0
    println("\n  ⚠️  Status=L/V/U/P/T/TS but eia860_status≠Proposed:")
    show(proposed_mismatch, allrows=true)
else
    println("  ✅ All L/V/U/P/T/TS correctly mapped to 'Proposed'")
end

# Retired mismatches
retired_mismatch = filter(r -> 
    coalesce(r.Status, "") in RETIRED_CODES && 
    r.eia860_status != "Retired/Canceled",
    status_comparison
)
if nrow(retired_mismatch) > 0
    println("\n  ⚠️  Status=RE/CN/IP but eia860_status≠Retired/Canceled:")
    show(retired_mismatch, allrows=true)
else
    println("  ✅ All RE/CN/IP correctly mapped to 'Retired/Canceled'")
end

# Check NA cases
na_cases = filter(r -> r.eia860_status == "NA", status_comparison)
println("\n  ⚠️  eia860_status='NA' breakdown:")
show(na_cases, allrows=true)

println("\n", "="^70)

# ── Summary counts ─────────────────────────────────────────────────────────────
println("\nExpected vs Actual counts:")
println("  Operable (should be OP+SB+OA+OS = ", 16348+1234+110+282, "):")
println("    eia860_status='Operable': ", count(==("Operable"), eia_expanded.eia860_status))
println("  Proposed (should be L+V+U+P+T+TS = ", 184+245+246+311+217+53, "):")
println("    eia860_status='Proposed': ", count(==("Proposed"), eia_expanded.eia860_status))
println("  Retired (should be RE+CN+IP = ", 3165+432+56, "):")
println("    eia860_status='Retired/Canceled': ", count(==("Retired/Canceled"), eia_expanded.eia860_status))
println("  NA (should be missing = 2079):")
println("    eia860_status='NA': ", count(==("NA"), eia_expanded.eia860_status))

# ── Diagnostic: Count thermal generators in EI by state ───────────────────────
println("\n", "="^70)
println("  EI Thermal Generator Counts (Original Data)")
println("="^70)

for (label, df) in [("VA", va_thermal), ("MD", md_thermal), ("WV", wv_thermal)]
    total_mw = round(sum(skipmissing(df.rating)), digits=1)
    
    println("\n$label:")
    println("  Total thermal generators: ", nrow(df), " ($(total_mw) MW)")
    
    # Breakdown by generator_type if available
    if hasproperty(df, :generator_type)
        println("  By generator_type:")
        type_counts = combine(groupby(df, :generator_type), 
                             nrow => :count,
                             :rating => (x -> round(sum(skipmissing(x)), digits=1)) => :total_mw)
        for row in eachrow(sort(type_counts, :count, rev=true))
            println("    $(row.generator_type): $(row.count) gens | $(row.total_mw) MW")
        end
    end
    
    # Breakdown by fuel if available
    if hasproperty(df, :fuel)
        println("  By fuel:")
        fuel_counts = combine(groupby(df, :fuel), 
                             nrow => :count,
                             :rating => (x -> round(sum(skipmissing(x)), digits=1)) => :total_mw)
        for row in eachrow(sort(fuel_counts, :count, rev=true))
            println("    $(row.fuel): $(row.count) gens | $(row.total_mw) MW")
        end
    end
end
println("="^70)

# ── Comprehensive EI vs EIA status comparison ──────────────────────────────────
println("\n", "="^70)
println("  Comprehensive EI vs EIA Thermal Comparison")
println("="^70)

# Define status groups
const STATUS_GROUPS = [
    ("Operable",          ["Operable"]),
    ("Proposed",          ["Proposed"]),
    ("Retired/Canceled",  ["Retired/Canceled"]),
    ("Unknown/NA",        ["NA"])
]

for (label, ei_df) in [("VA", va_thermal), ("MD", md_thermal), ("WV", wv_thermal)]
    println("\n$label:")
    println("  " * "─"^80)
    println("  Category          | EI Avail | EI Avail MW | EI Unavail | EI Unavail MW | EIA Count | EIA MW")
    println("  " * "─"^80)
    
    # EI breakdown by availability
    ei_avail = filter(r -> coalesce(r.available, false) == true, ei_df)
    ei_unavail = filter(r -> coalesce(r.available, false) == false, ei_df)
    
    ei_avail_mw = round(sum(skipmissing(ei_avail.rating)), digits=1)
    ei_unavail_mw = round(sum(skipmissing(ei_unavail.rating)), digits=1)
    
    for (status_label, status_codes) in STATUS_GROUPS
        # Filter EIA by state, thermal tech, and status
        eia_subset = filter(r -> 
            coalesce(r["State"], "") == label &&
            coalesce(r["Technology"], "") in THERMAL_TECHNOLOGIES &&
            coalesce(r.eia860_status, "") in status_codes,
            eia_expanded
        )
        
        eia_count = nrow(eia_subset)
        # Handle empty DataFrame or missing values
        eia_mw = if eia_count > 0
            round(sum(x -> coalesce(Float64(x), 0.0), eia_subset.eia_capacity_mw), digits=1)
        else
            0.0
        end
        
        # For EI, we don't have status breakdown, so show N/A for unavailable/proposed
        if status_label == "Operable"
            @printf("  %-17s | %8d | %11.1f | %10d | %13.1f | %9d | %8.1f\n",
                    status_label, nrow(ei_avail), ei_avail_mw, 
                    nrow(ei_unavail), ei_unavail_mw,
                    eia_count, eia_mw)
        else
            @printf("  %-17s | %8s | %11s | %10s | %13s | %9d | %8.1f\n",
                    status_label, "-", "-", "-", "-",
                    eia_count, eia_mw)
        end
    end
    
    # Totals
    println("  " * "─"^80)
    ei_av_total = nrow(ei_avail)
    ei_unav_total = nrow(ei_unavail)
    ei_total_mw = round(sum(skipmissing(ei_df.rating)), digits=1)
    
    eia_state_thermal = filter(r -> 
        coalesce(r["State"], "") == label &&
        coalesce(r["Technology"], "") in THERMAL_TECHNOLOGIES,
        eia_expanded
    )
    eia_total = nrow(eia_state_thermal)
    eia_total_mw = round(sum(x -> coalesce(Float64(x), 0.0), eia_state_thermal.eia_capacity_mw), digits=1)
    
    @printf("  TOTAL (EI)        | %8d | %11.1f | %10d | %13.1f | %9s | %8s\n",
            ei_av_total, ei_avail_mw, ei_unav_total, ei_unavail_mw, "-", "-")
    @printf("  TOTAL (EIA)       | %8s | %11s | %10s | %13s | %9d | %8.1f\n",
            "-", "-", "-", "-", eia_total, eia_total_mw)
end
println("="^70)


# ── 7. Match EI thermal → EIA2PF ───────────────────────────────────────────────
function match_thermal_to_eia2pf(label::String, thermal_df::DataFrame, eia_expanded::DataFrame)
    matched   = DataFrame()
    unmatched = DataFrame()

    for row in eachrow(thermal_df)
        bus_num = row["bus_number"]
        eia_rows = filter(r -> coalesce(r["BusID_int"] == bus_num, false), eia_expanded)

        if nrow(eia_rows) > 0
            best = eia_rows[1, :]
            new_row = copy(DataFrame(row))
            for col in names(eia_rows)
                new_row[!, col] .= best[col]
            end
            append!(matched, new_row, promote=true)
        else
            append!(unmatched, copy(DataFrame(row)), promote=true)
        end
    end

    # ── Reorder and rename columns in matched ──────────────────────────────
    if nrow(matched) > 0
        # Rename first, then reorder
        DataFrames.rename!(matched, "Technology" => "eia_technology")

        cols = names(matched)
        function move_after!(cols, target, anchor)
            filter!(x -> x != target, cols)
            idx = findfirst(==(anchor), cols)
            insert!(cols, idx + 1, target)
        end
        move_after!(cols, "eia_capacity_mw", "rating")
        move_after!(cols, "eia_technology", "eia_capacity_mw")
        move_after!(cols, "fuel", "eia_technology")
        move_after!(cols, "eia860_status", "fuel")
        select!(matched, cols)
    end
    # ────────────────────────────────────────────────────────────────────────
    println("\n", "="^60)
    println("$label — Thermal EI → EIA2PF Match")
    println("="^60)
    println("  ✅ Matched:   ", nrow(matched))
    println("  ⚠️  Unmatched: ", nrow(unmatched))
    println("="^60)

    return matched, unmatched
end

va_thermal_matched, va_thermal_unmatched = match_thermal_to_eia2pf("VA", va_thermal, eia_expanded)
md_thermal_matched, md_thermal_unmatched = match_thermal_to_eia2pf("MD", md_thermal, eia_expanded)
wv_thermal_matched, wv_thermal_unmatched = match_thermal_to_eia2pf("WV", wv_thermal, eia_expanded)

# # ── 7. Match EI thermal → EIA2PF with technology validation ────────────────────
# function match_thermal_to_eia2pf(label::String, thermal_df::DataFrame, eia_expanded::DataFrame)
#     matched   = DataFrame()
#     unmatched = DataFrame()

#     for row in eachrow(thermal_df)
#         bus_num = row["bus_number"]
        
#         # Filter EIA to same bus AND thermal technology
#         eia_rows = filter(r -> 
#             coalesce(r["BusID_int"], -1) == bus_num &&
#             coalesce(r["Technology"], "") in THERMAL_TECHNOLOGIES,
#             eia_expanded
#         )

#         if nrow(eia_rows) > 0
#             # If multiple matches, try to pick best by fuel/prime mover
#             best = if nrow(eia_rows) == 1
#                 eia_rows[1, :]
#             else
#                 # Try to match by fuel/prime mover similarity
#                 ei_fuel = lowercase(string(coalesce(row["fuel"], "")))
#                 ei_pm = uppercase(string(coalesce(row["prime_mover_type"], "")))
                
#                 # Score each EIA row
#                 scored = DataFrame()
#                 for eia_row in eachrow(eia_rows)
#                     eia_tech = lowercase(string(coalesce(eia_row["Technology"], "")))
#                     eia_pm_code = uppercase(string(coalesce(eia_row["Prime Mover Code"], "")))
                    
#                     score = 0
#                     # Fuel matching
#                     if occursin("gas", ei_fuel) && occursin("gas", eia_tech)
#                         score += 2
#                     elseif occursin("coal", ei_fuel) && occursin("coal", eia_tech)
#                         score += 2
#                     elseif occursin("oil", ei_fuel) && occursin("petroleum", eia_tech)
#                         score += 2
#                     elseif occursin("nuclear", ei_fuel) && occursin("nuclear", eia_tech)
#                         score += 2
#                     end
                    
#                     # Prime mover matching
#                     if ei_pm == eia_pm_code
#                         score += 3
#                     elseif (ei_pm in ["CC", "CT"] && eia_pm_code in ["CC", "CT", "CA", "CS"])
#                         score += 1
#                     end
                    
#                     new_row = copy(DataFrame(eia_row))
#                     new_row[!, "match_score"] .= score
#                     append!(scored, new_row, promote=true)
#                 end
                
#                 # Take highest scoring match
#                 sort!(scored, :match_score, rev=true)
#                 scored[1, :]
#             end
            
#             new_row = copy(DataFrame(row))
#             for col in names(eia_rows)
#                 if col != "match_score"  # Don't overwrite if it exists
#                     new_row[!, col] .= best[col]
#                 end
#             end
#             append!(matched, new_row, promote=true)
#         else
#             append!(unmatched, copy(DataFrame(row)), promote=true)
#         end
#     end

#     println("\n", "="^60)
#     println("$label — Thermal EI → EIA2PF Match")
#     println("="^60)
#     println("  ✅ Matched:   ", nrow(matched))
#     println("  ⚠️  Unmatched: ", nrow(unmatched))
    
#     # Technology breakdown of matches
#     if nrow(matched) > 0
#         println("\n  Technology breakdown of matches:")
#         tech_counts = combine(groupby(matched, :Technology), nrow => :count)
#         for row in eachrow(sort(tech_counts, :count, rev=true))
#             println("    $(coalesce(row.Technology, "missing")): $(row.count)")
#         end
#     end
#     println("="^60)

#     return matched, unmatched
# end

# Re-run matching with technology filter
va_thermal_matched, va_thermal_unmatched = match_thermal_to_eia2pf("VA", va_thermal, eia_expanded)
md_thermal_matched, md_thermal_unmatched = match_thermal_to_eia2pf("MD", md_thermal, eia_expanded)
wv_thermal_matched, wv_thermal_unmatched = match_thermal_to_eia2pf("WV", wv_thermal, eia_expanded)

# ── Split thermal by fuel type — VA ───────────────────────────────────────────
va_thermal_ng       = filter(r -> coalesce(r.fuel, "") == "NATURAL_GAS",         va_thermal)
va_thermal_oil_dist = filter(r -> coalesce(r.fuel, "") == "DISTILLATE_FUEL_OIL", va_thermal)
va_thermal_oil_res  = filter(r -> coalesce(r.fuel, "") == "RESIDUAL_FUEL_OIL",   va_thermal)
va_thermal_coal     = filter(r -> coalesce(r.fuel, "") == "COAL",                 va_thermal)
va_thermal_nuclear  = filter(r -> coalesce(r.fuel, "") == "NUCLEAR",              va_thermal)
va_thermal_wood     = filter(r -> coalesce(r.fuel, "") == "WOOD_WASTE_SOLIDS",    va_thermal)
va_thermal_other    = filter(r -> coalesce(r.fuel, "") == "OTHER",                va_thermal)

println("\n", "="^70)
println("  VA Thermal Breakdown by Fuel")
println("="^70)
println("  Natural Gas:    $(nrow(va_thermal_ng))       gens | $(round(sum(skipmissing(va_thermal_ng.rating)),       digits=1)) MW")
println("  Distillate Oil: $(nrow(va_thermal_oil_dist)) gens | $(round(sum(skipmissing(va_thermal_oil_dist.rating)), digits=1)) MW")
println("  Residual Oil:   $(nrow(va_thermal_oil_res))  gens | $(round(sum(skipmissing(va_thermal_oil_res.rating)),  digits=1)) MW")
println("  Coal:           $(nrow(va_thermal_coal))     gens | $(round(sum(skipmissing(va_thermal_coal.rating)),     digits=1)) MW")
println("  Nuclear:        $(nrow(va_thermal_nuclear))  gens | $(round(sum(skipmissing(va_thermal_nuclear.rating)),  digits=1)) MW")
println("  Wood Waste:     $(nrow(va_thermal_wood))     gens | $(round(sum(skipmissing(va_thermal_wood.rating)),     digits=1)) MW")
println("  Other:          $(nrow(va_thermal_other))    gens | $(round(sum(skipmissing(va_thermal_other.rating)),    digits=1)) MW")
println("="^70)

# ── Sanity check ──────────────────────────────────────────────────────────────
va_fuel_total = nrow(va_thermal_ng) + nrow(va_thermal_oil_dist) + nrow(va_thermal_oil_res) +
                nrow(va_thermal_coal) + nrow(va_thermal_nuclear) + nrow(va_thermal_wood) +
                nrow(va_thermal_other)

@assert va_fuel_total == nrow(va_thermal) """
    VA fuel split mismatch: splits sum to $va_fuel_total but va_thermal has $(nrow(va_thermal)).
    Check for missing or unexpected fuel codes: $(setdiff(unique(va_thermal.fuel),
    ["NATURAL_GAS","DISTILLATE_FUEL_OIL","RESIDUAL_FUEL_OIL","COAL","NUCLEAR","WOOD_WASTE_SOLIDS","OTHER"]))
"""

va_ng_matched,       va_ng_unmatched       = match_thermal_to_eia2pf("VA", va_thermal_ng,       eia_expanded)
va_oil_dist_matched, va_oil_dist_unmatched = match_thermal_to_eia2pf("VA", va_thermal_oil_dist, eia_expanded)
va_oil_res_matched,  va_oil_res_unmatched  = match_thermal_to_eia2pf("VA", va_thermal_oil_res,  eia_expanded)
va_coal_matched,     va_coal_unmatched     = match_thermal_to_eia2pf("VA", va_thermal_coal,     eia_expanded)
va_nuclear_matched,  va_nuclear_unmatched  = match_thermal_to_eia2pf("VA", va_thermal_nuclear,  eia_expanded)
va_wood_matched,     va_wood_unmatched     = match_thermal_to_eia2pf("VA", va_thermal_wood,     eia_expanded)
va_other_matched,    va_other_unmatched    = match_thermal_to_eia2pf("VA", va_thermal_other,    eia_expanded)

# ── Split thermal by fuel type — MD ───────────────────────────────────────────
md_thermal_ng       = filter(r -> coalesce(r.fuel, "") == "NATURAL_GAS",         md_thermal)
md_thermal_oil_dist = filter(r -> coalesce(r.fuel, "") == "DISTILLATE_FUEL_OIL", md_thermal)
md_thermal_oil_res  = filter(r -> coalesce(r.fuel, "") == "RESIDUAL_FUEL_OIL",   md_thermal)
md_thermal_coal     = filter(r -> coalesce(r.fuel, "") == "COAL",                 md_thermal)
md_thermal_nuclear  = filter(r -> coalesce(r.fuel, "") == "NUCLEAR",              md_thermal)
md_thermal_other    = filter(r -> coalesce(r.fuel, "") == "OTHER",                md_thermal)

println("\n", "="^70)
println("  MD Thermal Breakdown by Fuel")
println("="^70)
println("  Natural Gas:    $(nrow(md_thermal_ng))       gens | $(round(sum(skipmissing(md_thermal_ng.rating)),       digits=1)) MW")
println("  Distillate Oil: $(nrow(md_thermal_oil_dist)) gens | $(round(sum(skipmissing(md_thermal_oil_dist.rating)), digits=1)) MW")
println("  Residual Oil:   $(nrow(md_thermal_oil_res))  gens | $(round(sum(skipmissing(md_thermal_oil_res.rating)),  digits=1)) MW")
println("  Coal:           $(nrow(md_thermal_coal))     gens | $(round(sum(skipmissing(md_thermal_coal.rating)),     digits=1)) MW")
println("  Nuclear:        $(nrow(md_thermal_nuclear))  gens | $(round(sum(skipmissing(md_thermal_nuclear.rating)),  digits=1)) MW")
println("  Other:          $(nrow(md_thermal_other))    gens | $(round(sum(skipmissing(md_thermal_other.rating)),    digits=1)) MW")
println("="^70)

# ── Sanity check ──────────────────────────────────────────────────────────────
md_fuel_total = nrow(md_thermal_ng) + nrow(md_thermal_oil_dist) + nrow(md_thermal_oil_res) +
                nrow(md_thermal_coal) + nrow(md_thermal_nuclear) +
                nrow(md_thermal_other)

@assert md_fuel_total == nrow(md_thermal) """
    md fuel split mismatch: splits sum to $md_fuel_total but md_thermal has $(nrow(md_thermal)).
    Check for missing or unexpected fuel codes: $(setdiff(unique(md_thermal.fuel),
    ["NATURAL_GAS","DISTILLATE_FUEL_OIL","RESIDUAL_FUEL_OIL","COAL","NUCLEAR","WOOD_WASTE_SOLIDS","OTHER"]))
"""

md_ng_matched,       md_ng_unmatched       = match_thermal_to_eia2pf("MD", md_thermal_ng,       eia_expanded)
md_oil_dist_matched, md_oil_dist_unmatched = match_thermal_to_eia2pf("MD", md_thermal_oil_dist, eia_expanded)
md_oil_res_matched,  md_oil_res_unmatched  = match_thermal_to_eia2pf("MD", md_thermal_oil_res,  eia_expanded)
md_coal_matched,     md_coal_unmatched     = match_thermal_to_eia2pf("MD", md_thermal_coal,     eia_expanded)
md_nuclear_matched,  md_nuclear_unmatched  = match_thermal_to_eia2pf("MD", md_thermal_nuclear,  eia_expanded)
md_other_matched,    md_other_unmatched    = match_thermal_to_eia2pf("MD", md_thermal_other,    eia_expanded)

# ── Split thermal by fuel type — WV ───────────────────────────────────────────
wv_thermal_ng       = filter(r -> coalesce(r.fuel, "") == "NATURAL_GAS",         wv_thermal)
wv_thermal_coal     = filter(r -> coalesce(r.fuel, "") == "COAL",                 wv_thermal)
wv_thermal_jet      = filter(r -> coalesce(r.fuel, "") == "JET_FUEL",             wv_thermal)
wv_thermal_other    = filter(r -> coalesce(r.fuel, "") == "OTHER",                wv_thermal)

println("\n", "="^70)
println("  WV Thermal Breakdown by Fuel")
println("="^70)
println("  Natural Gas:    $(nrow(wv_thermal_ng))   gens | $(round(sum(skipmissing(wv_thermal_ng.rating)),   digits=1)) MW")
println("  Coal:           $(nrow(wv_thermal_coal)) gens | $(round(sum(skipmissing(wv_thermal_coal.rating)), digits=1)) MW")
println("  Jet Fuel:       $(nrow(wv_thermal_jet))  gens | $(round(sum(skipmissing(wv_thermal_jet.rating)),  digits=1)) MW")
println("  Other:          $(nrow(wv_thermal_other))gens | $(round(sum(skipmissing(wv_thermal_other.rating)),digits=1)) MW")
println("="^70)

wv_ng_matched,   wv_ng_unmatched   = match_thermal_to_eia2pf("WV", wv_thermal_ng,    eia_expanded)
wv_coal_matched, wv_coal_unmatched = match_thermal_to_eia2pf("WV", wv_thermal_coal,  eia_expanded)
wv_jet_matched,  wv_jet_unmatched  = match_thermal_to_eia2pf("WV", wv_thermal_jet,   eia_expanded)
wv_other_matched,wv_other_unmatched= match_thermal_to_eia2pf("WV", wv_thermal_other, eia_expanded)

# ── Technology-Fuel consistency check ─────────────────────────────────────────

# Expected EIA technology keywords per EI fuel
const FUEL_TECH_KEYWORDS = Dict(
    "NATURAL_GAS"          => ["gas", "combustion", "combined cycle", "steam"],
    "COAL"                 => ["coal", "steam"],
    "NUCLEAR"              => ["nuclear"],
    "DISTILLATE_FUEL_OIL"  => ["petroleum", "oil", "combustion", "steam"],
    "RESIDUAL_FUEL_OIL"    => ["petroleum", "oil", "combustion", "steam"],
    "WOOD_WASTE_SOLIDS"    => ["wood", "biomass", "steam"],
    "OTHER"                => [],  # too broad to validate
)

"""
    check_tech_fuel_mismatch(matched::DataFrame, fuel::String) -> DataFrame

Returns rows where eia_technology does not contain any expected keyword for the given fuel.
"""
function check_tech_fuel_mismatch(matched::DataFrame, fuel::String)
    keywords = get(FUEL_TECH_KEYWORDS, fuel, [])
    isempty(keywords) && return matched[1:0, :]  # skip OTHER

    return filter(r ->
        !any(occursin(kw, lowercase(coalesce(r.eia_technology, ""))) for kw in keywords),
        eachrow(matched)) |> DataFrame
end

"""
    check_capacity_diff(matched::DataFrame; threshold_pct::Float64 = 20.0) -> DataFrame

Returns rows where the difference between EI rating and EIA capacity exceeds threshold_pct %.
Also adds a `capacity_diff_mw` and `capacity_diff_pct` column to the full matched DataFrame.
"""
function check_capacity_diff(matched::DataFrame; threshold_pct::Float64 = 20.0)
    df = copy(matched)
    df[!, :capacity_diff_mw]  = df.rating .- coalesce.(df.eia_capacity_mw, df.rating)
    df[!, :capacity_diff_pct] = abs.(df.capacity_diff_mw) ./ coalesce.(df.eia_capacity_mw, df.rating) .* 100.0

    outliers = filter(r -> coalesce(r.capacity_diff_pct, 0.0) > threshold_pct, eachrow(df)) |> DataFrame
    return df, outliers
end

"""
    diagnose_matched(label::String, fuel::String, matched::DataFrame; threshold_pct::Float64 = 20.0)

Runs both checks and prints a summary. Returns (tech_mismatches, capacity_outliers, matched_with_diff).
"""
function diagnose_matched(label::String, fuel::String, matched::DataFrame; threshold_pct::Float64 = 20.0)
    println("\n", "="^70)
    println("  $label — $fuel — Diagnostics")
    println("="^70)

    # Tech-fuel mismatch
    tech_mismatches = check_tech_fuel_mismatch(matched, fuel)
    println("\n  Technology-Fuel mismatches: ", nrow(tech_mismatches))
    if nrow(tech_mismatches) > 0
        select(tech_mismatches, [:name, :rating, :eia_capacity_mw, :eia_technology, :fuel, :eia860_status]) |> display
    end

    # Capacity diff
    matched_with_diff, capacity_outliers = check_capacity_diff(matched; threshold_pct)
    println("\n  Capacity outliers (>$(threshold_pct)% diff): ", nrow(capacity_outliers))
    if nrow(capacity_outliers) > 0
        select(capacity_outliers, [:name, :rating, :eia_capacity_mw, :capacity_diff_mw, :capacity_diff_pct, :eia_technology, :eia860_status]) |> display
    end

    println("="^70)
    return tech_mismatches, capacity_outliers, matched_with_diff
end

va_ng_tech_mismatch, va_ng_cap_outliers, va_ng_matched = diagnose_matched("VA", "NATURAL_GAS", va_ng_matched) #DONE. #TODO use the eia capacities
va_ng_to_remove = Set([
    "generator-316283-6731062071",  # matched to Solar PV - actually solar misclassification
    "generator-315018-4794555208",  # matched to Petroleum Liquids
    "generator-315076-2907818267",  # matched to Wood/Wood Waste Biomass
    "generator-315015-9086928895",  # matched to Petroleum Liquids
    "generator-315015-7635854601",  # matched to Petroleum Liquids
    "generator-315018-9241787318",  # matched to Petroleum Liquids
    "generator-316303-3928489885",  # matched to Batteries
    "generator-315015-3044849791",  # matched to Petroleum Liquids
    "generator-315018-1226589183",  # matched to Petroleum Liquids
])

va_ng_matched = filter(r -> !(r.name in va_ng_to_remove), eachrow(va_ng_matched)) |> DataFrame

println("va_ng_matched after removal: ", nrow(va_ng_matched), " rows")
va_coal_tech_mismatch, va_coal_cap_outliers, va_coal_matched = diagnose_matched("VA", "COAL", va_coal_matched)#DONE. #TODO use the eia capacities
va_nuclear_tech_mismatch, va_nuclear_cap_outliers, va_nuclear_matched = diagnose_matched("VA", "NUCLEAR", va_nuclear_matched) #DONE no problem
va_oil_dist_tech_mismatch, va_oil_dist_cap_outliers, va_oil_dist_matched = diagnose_matched("VA", "DISTILLATE_FUEL_OIL", va_oil_dist_matched)
va_oil_res_tech_mismatch, va_oil_res_cap_outliers, va_oil_res_matched = diagnose_matched("VA", "RESIDUAL_FUEL_OIL", va_oil_res_matched)#DONE no problem
va_wood_tech_mismatch, va_wood_cap_outliers, va_wood_matched = diagnose_matched("VA", "WOOD_WASTE_SOLIDS", va_wood_matched) ##DONE no problem

# ── MD Diagnostics ────────────────────────────────────────────────────────────
md_ng_tech_mismatch,       md_ng_cap_outliers,       md_ng_matched       = diagnose_matched("MD", "NATURAL_GAS",         md_ng_matched)
md_oil_dist_tech_mismatch, md_oil_dist_cap_outliers, md_oil_dist_matched = diagnose_matched("MD", "DISTILLATE_FUEL_OIL", md_oil_dist_matched)
md_oil_res_tech_mismatch,  md_oil_res_cap_outliers,  md_oil_res_matched  = diagnose_matched("MD", "RESIDUAL_FUEL_OIL",   md_oil_res_matched) #DONE
md_coal_tech_mismatch,     md_coal_cap_outliers,     md_coal_matched     = diagnose_matched("MD", "COAL",                md_coal_matched)
md_nuclear_tech_mismatch,  md_nuclear_cap_outliers,  md_nuclear_matched  = diagnose_matched("MD", "NUCLEAR",             md_nuclear_matched) #DONE
md_other_tech_mismatch,    md_other_cap_outliers,    md_other_matched    = diagnose_matched("MD", "OTHER",               md_other_matched) #DONE

# ── WV Diagnostics ────────────────────────────────────────────────────────────
wv_ng_tech_mismatch,    wv_ng_cap_outliers,    wv_ng_matched    = diagnose_matched("WV", "NATURAL_GAS", wv_ng_matched)
wv_coal_tech_mismatch,  wv_coal_cap_outliers,  wv_coal_matched  = diagnose_matched("WV", "COAL",        wv_coal_matched) #DONE
wv_jet_tech_mismatch,   wv_jet_cap_outliers,   wv_jet_matched   = diagnose_matched("WV", "JET_FUEL",    wv_jet_matched)  #DONE
wv_other_tech_mismatch, wv_other_cap_outliers, wv_other_matched = diagnose_matched("WV", "OTHER",       wv_other_matched)

#TODO implement to automitize for all states and fuels. Remains commented for reviewing data.
# ── Helper: split thermal df by fuel, print summary, run matching ─────────────
# function split_match_by_fuel(label::String, thermal_df::DataFrame, 
#             eia_expanded::DataFrame)
#     fuel_codes = sort(unique(skipmissing(thermal_df.fuel)))

#     fuel_dfs = Dict(fuel => filter(r -> coalesce(r.fuel, "") == fuel, thermal_df)
#     for fuel in fuel_codes)

#     println("\n", "="^70)
#     println("  $label Thermal Breakdown by Fuel")
#     println("="^70)
#     for fuel in fuel_codes
#     df  = fuel_dfs[fuel]
#     mw  = round(sum(skipmissing(df.rating)), digits=1)
#     println("  $(rpad(fuel*":", 25)) $(lpad(nrow(df), 4)) gens | $(lpad(mw, 8)) MW")
#     end
#     println("─"^70)
#     total_mw = round(sum(skipmissing(thermal_df.rating)), digits=1)
#     println("  $(rpad("TOTAL:", 25)) $(lpad(nrow(thermal_df), 4)) gens | $(lpad(total_mw, 8)) MW")
#     println("="^70)

#     split_total = sum(nrow(df) for df in values(fuel_dfs))
#     @assert split_total == nrow(thermal_df) """
#     $label fuel split mismatch: splits sum to $split_total but thermal has $(nrow(thermal_df)).
#     Unexpected fuel codes: $(setdiff(unique(skipmissing(thermal_df.fuel)), fuel_codes))
#     """

#     matched_dfs   = Dict{String, DataFrame}()
#     unmatched_dfs = Dict{String, DataFrame}()

#     for fuel in fuel_codes
#     m, u = match_thermal_to_eia2pf(label, fuel_dfs[fuel], eia_expanded)
#     matched_dfs[fuel]   = m
#     unmatched_dfs[fuel] = u
#     end

#     # ── Safe MW extractor — handles empty DataFrames ──────────────────────────
#     safe_mw(df) = (nrow(df) == 0 || !("rating" in names(df))) ? 0.0 :
#     round(sum(Float64.(coalesce.(df.rating, 0.0))), digits=1)

#     println("\n  $label — Match Summary by Fuel")
#     println("  ", "─"^66)
#     @printf("  %-25s %8s %10s %10s %10s\n",
#     "Fuel", "Matched", "Match MW", "Unmatched", "Unmatch MW")
#     println("  ", "─"^66)
#     for fuel in fuel_codes
#     mm = nrow(matched_dfs[fuel])
#     mw = safe_mw(matched_dfs[fuel])
#     um = nrow(unmatched_dfs[fuel])
#     uw = safe_mw(unmatched_dfs[fuel])
#     @printf("  %-25s %8d %10.1f %10d %10.1f\n", fuel*":", mm, mw, um, uw)
#     end
#     println("  ", "─"^66)

# return fuel_dfs, matched_dfs, unmatched_dfs
# end
# # ── Run for each state ────────────────────────────────────────────────────────
# va_fuel_dfs, va_fuel_matched, va_fuel_unmatched = 
# split_match_by_fuel("VA", va_thermal, eia_expanded)
# va_fuel_matched["NATURAL_GAS"]

# md_fuel_dfs, md_fuel_matched, md_fuel_unmatched = 
# split_match_by_fuel("MD", md_thermal, eia_expanded)

# wv_fuel_dfs, wv_fuel_matched, wv_fuel_unmatched = 
# split_match_by_fuel("WV", wv_thermal, eia_expanded)

# # ── Helper: print matched/unmatched detail for all fuels in a state ───────────
# function print_fuel_results(label::String,
#     fuel_codes,
#     matched_dfs::Dict{String, DataFrame},
#     unmatched_dfs::Dict{String, DataFrame})

# DISPLAY_COLS = [
# "name", "bus_number", "bus", "prime_mover_type", "fuel",
# "rating", "eia_capacity_mw", "eia_technology",
# "Plant ID", "Plant Name", "BusName", "kV",
# ]

# for fuel in fuel_codes
# println("\n", "█"^70)
# println("  $label — $fuel")
# println("█"^70)

# m = matched_dfs[fuel]
# u = unmatched_dfs[fuel]

# println("\n  ✅ Matched ($(nrow(m)) rows):")
# if nrow(m) > 0
# show(sort(DataFrames.select(m, intersect(DISPLAY_COLS, names(m))),
# "bus_number"), allrows=true)
# else
# println("  (none)")
# end

# println("\n  ⚠️  Unmatched ($(nrow(u)) rows):")
# if nrow(u) > 0
# UNMATCH_COLS = ["name", "bus_number", "bus",
#    "prime_mover_type", "fuel", "rating"]
# show(sort(DataFrames.select(u, intersect(UNMATCH_COLS, names(u))),
# "bus_number"), allrows=true)
# else
# println("  (none)")
# end
# end
# end

# # ── Run for each state ────────────────────────────────────────────────────────
# # print_fuel_results("VA", sort(keys(va_fuel_matched)),  va_fuel_matched, va_fuel_unmatched)
# # print_fuel_results("MD", sort(keys(md_fuel_matched)),  md_fuel_matched, md_fuel_unmatched)
# # print_fuel_results("WV", sort(keys(wv_fuel_matched)),  wv_fuel_matched, wv_fuel_unmatched)

# # ── Single fuel inspection shortcuts ─────────────────────────────────────────

# # VA
# print_fuel_results("VA", ["NATURAL_GAS"],         va_fuel_matched, va_fuel_unmatched)
# print_fuel_results("VA", ["DISTILLATE_FUEL_OIL"], va_fuel_matched, va_fuel_unmatched)
# print_fuel_results("VA", ["RESIDUAL_FUEL_OIL"],   va_fuel_matched, va_fuel_unmatched)
# print_fuel_results("VA", ["COAL"],                va_fuel_matched, va_fuel_unmatched)
# print_fuel_results("VA", ["NUCLEAR"],             va_fuel_matched, va_fuel_unmatched)
# print_fuel_results("VA", ["WOOD_WASTE_SOLIDS"],   va_fuel_matched, va_fuel_unmatched)
# print_fuel_results("VA", ["OTHER"],               va_fuel_matched, va_fuel_unmatched)

# # MD
# print_fuel_results("MD", ["NATURAL_GAS"],         md_fuel_matched, md_fuel_unmatched)
# print_fuel_results("MD", ["DISTILLATE_FUEL_OIL"], md_fuel_matched, md_fuel_unmatched)
# print_fuel_results("MD", ["RESIDUAL_FUEL_OIL"],   md_fuel_matched, md_fuel_unmatched)
# print_fuel_results("MD", ["COAL"],                md_fuel_matched, md_fuel_unmatched)
# print_fuel_results("MD", ["NUCLEAR"],             md_fuel_matched, md_fuel_unmatched)
# print_fuel_results("MD", ["OTHER"],               md_fuel_matched, md_fuel_unmatched)

# # WV
# print_fuel_results("WV", ["NATURAL_GAS"],         wv_fuel_matched, wv_fuel_unmatched)
# print_fuel_results("WV", ["COAL"],                wv_fuel_matched, wv_fuel_unmatched)
# print_fuel_results("WV", ["JET_FUEL"],            wv_fuel_matched, wv_fuel_unmatched)
# print_fuel_results("WV", ["OTHER"],               wv_fuel_matched, wv_fuel_unmatched)

# # ── 8. Build export DataFrames ────────────────────────────────────────────────
# function build_thermal_export(label::String, matched::DataFrame)
#     DataFrame(
#         gen_name        = matched[!, "name"],
#         bus_id          = matched[!, "bus_number"],
#         bus_name        = matched[!, "bus"],
#         generator_type  = matched[!, "generator_type"],
#         prime_mover     = matched[!, "prime_mover_type"],
#         fuel            = matched[!, "fuel"],
#         ei_capacity_mw  = matched[!, "rating"],
#         eia_capacity_mw = matched[!, "eia_capacity_mw"],
#         eia_lat         = matched[!, "eia_lat"],
#         eia_lon         = matched[!, "eia_lon"],
#         eia860_status   = matched[!, "eia860_status"],
#         source          = fill("EI_EIA2PF", nrow(matched)),
#         state           = fill(label, nrow(matched)),
#     )
# end

# """
#     thermal_initial_comparison(label, ei_df, eia_expanded)

# High-level summary comparing EI thermal generators vs EIA by fuel type and status.
# No matching needed — pure aggregate comparison.
# """
# function thermal_initial_comparison(label::String, ei_df::DataFrame, eia_expanded::DataFrame)
#     println("\n", "="^110)
#     println("  $label — EI vs EIA Thermal Initial Comparison")
#     println("="^110)
#     @printf("  %-22s | %6s | %10s | %6s | %10s | %6s | %10s | %6s | %10s | %6s | %10s | %8s\n",
#             "Fuel", 
#             "EI N", "EI MW",
#             "EI Av N", "EI Av MW",
#             "EI Un N", "EI Un MW",
#             "EIA Op+Pr N", "EIA Op+Pr MW",
#             "EIA Ret N", "EIA Ret MW",
#             "Diff MW")
#     println("  ", "─"^110)

#     fuels = sort(unique(skipmissing(string.(ei_df.fuel))))

#     # Storage for plots
#     plot_fuels    = String[]
#     plot_ei_av    = Float64[]
#     plot_ei_unav  = Float64[]
#     plot_eia_op   = Float64[]
#     plot_eia_ret  = Float64[]

#     for fuel in fuels
#         # EI side
#         ei_subset  = filter(r -> coalesce(string(r.fuel), "") == fuel, eachrow(ei_df)) |> DataFrame
#         ei_avail   = filter(r -> coalesce(r.available, false) == true,  eachrow(ei_subset)) |> DataFrame
#         ei_unavail = filter(r -> coalesce(r.available, false) == false, eachrow(ei_subset)) |> DataFrame

#         ei_n       = nrow(ei_subset)
#         ei_mw      = round(sum(skipmissing(ei_subset.rating),  init=0.0), digits=1)
#         ei_av_n    = nrow(ei_avail)
#         ei_av_mw   = round(sum(skipmissing(ei_avail.rating),   init=0.0), digits=1)
#         ei_unav_n  = nrow(ei_unavail)
#         ei_unav_mw = round(sum(skipmissing(ei_unavail.rating), init=0.0), digits=1)

#         # EIA side
#         fuel_keywords = get(FUEL_TECH_KEYWORDS, fuel, String[])
#         eia_state = filter(r ->
#             coalesce(r["State"], "") == label &&
#             coalesce(r["Technology"], "") in THERMAL_TECHNOLOGIES,
#             eia_expanded)

#         if !isempty(fuel_keywords)
#             eia_state = filter(r ->
#                 any(occursin(kw, lowercase(coalesce(r["Technology"], ""))) for kw in fuel_keywords),
#                 eachrow(eia_state)) |> DataFrame
#         end

#         eia_op  = filter(r -> coalesce(r.eia860_status, "") in ["Operable", "Proposed"],
#                         eachrow(eia_state)) |> DataFrame
#         eia_ret = filter(r -> coalesce(r.eia860_status, "") == "Retired/Canceled",
#                         eachrow(eia_state)) |> DataFrame

#         eia_op_n   = nrow(eia_op)
#         eia_op_mw  = round(sum(x -> coalesce(Float64(x), 0.0), eia_op.eia_capacity_mw,  init=0.0), digits=1)
#         eia_ret_n  = nrow(eia_ret)
#         eia_ret_mw = round(sum(x -> coalesce(Float64(x), 0.0), eia_ret.eia_capacity_mw, init=0.0), digits=1)

#         diff_mw = round(ei_av_mw - eia_op_mw, digits=1)
#         flag = abs(diff_mw) < 100 ? "✅" : "⚠️ "

#         @printf("  %-22s | %6d | %10.1f | %7d | %10.1f | %7d | %10.1f | %11d | %12.1f | %9d | %10.1f | %s %7.1f\n",
#                 fuel,
#                 ei_n, ei_mw,
#                 ei_av_n, ei_av_mw,
#                 ei_unav_n, ei_unav_mw,
#                 eia_op_n, eia_op_mw,
#                 eia_ret_n, eia_ret_mw,
#                 flag, diff_mw)

#         # Store for plots
#         push!(plot_fuels,   fuel)
#         push!(plot_ei_av,   ei_av_mw)
#         push!(plot_ei_unav, ei_unav_mw)
#         push!(plot_eia_op,  eia_op_mw)
#         push!(plot_eia_ret, eia_ret_mw)
#     end

#     # Totals row
#     println("  ", "─"^110)
#     ei_total      = nrow(ei_df)
#     ei_total_mw   = round(sum(skipmissing(ei_df.rating), init=0.0), digits=1)
#     ei_av_total   = filter(r -> coalesce(r.available, false) == true,  eachrow(ei_df)) |> DataFrame
#     ei_unav_total = filter(r -> coalesce(r.available, false) == false, eachrow(ei_df)) |> DataFrame
#     ei_av_mw_tot  = round(sum(skipmissing(ei_av_total.rating),   init=0.0), digits=1)
#     ei_unav_mw_tot= round(sum(skipmissing(ei_unav_total.rating), init=0.0), digits=1)

#     eia_all     = filter(r -> coalesce(r["State"], "") == label &&
#                               coalesce(r["Technology"], "") in THERMAL_TECHNOLOGIES, eia_expanded)
#     eia_op_tot  = filter(r -> coalesce(r.eia860_status, "") in ["Operable", "Proposed"],
#                         eachrow(eia_all)) |> DataFrame
#     eia_ret_tot = filter(r -> coalesce(r.eia860_status, "") == "Retired/Canceled",
#                         eachrow(eia_all)) |> DataFrame
#     eia_op_mw_tot  = round(sum(x -> coalesce(Float64(x), 0.0), eia_op_tot.eia_capacity_mw,  init=0.0), digits=1)
#     eia_ret_mw_tot = round(sum(x -> coalesce(Float64(x), 0.0), eia_ret_tot.eia_capacity_mw, init=0.0), digits=1)
#     diff_tot = round(ei_av_mw_tot - eia_op_mw_tot, digits=1)

#     @printf("  %-22s | %6d | %10.1f | %7d | %10.1f | %7d | %10.1f | %11d | %12.1f | %9d | %10.1f | %s %7.1f\n",
#             "TOTAL",
#             ei_total, ei_total_mw,
#             nrow(ei_av_total), ei_av_mw_tot,
#             nrow(ei_unav_total), ei_unav_mw_tot,
#             nrow(eia_op_tot), eia_op_mw_tot,
#             nrow(eia_ret_tot), eia_ret_mw_tot,
#             abs(diff_tot) < 100 ? "✅" : "⚠️ ", diff_tot)
#     println("="^110)

#     # ── Plots ─────────────────────────────────────────────────────────────────
#     short_fuels = replace.(plot_fuels, 
#         "NATURAL_GAS"        => "NG",
#         "DISTILLATE_FUEL_OIL"=> "Dist Oil",
#         "RESIDUAL_FUEL_OIL"  => "Res Oil",
#         "WOOD_WASTE_SOLIDS"  => "Wood",
#         "COAL"               => "Coal",
#         "NUCLEAR"            => "Nuclear",
#         "JET_FUEL"           => "Jet",
#         "OTHER"              => "Other")

#     p1 = PlotlyJS.plot([
#         PlotlyJS.bar(x=short_fuels, y=plot_ei_av,   name="EI Available",   marker_color="green"),
#         PlotlyJS.bar(x=short_fuels, y=plot_ei_unav, name="EI Unavailable",  marker_color="orange"),
#         PlotlyJS.bar(x=short_fuels, y=plot_eia_op,  name="EIA Op+Proposed", marker_color="steelblue"),
#         PlotlyJS.bar(x=short_fuels, y=plot_eia_ret, name="EIA Retired",     marker_color="red"),
#     ], PlotlyJS.Layout(
#         title       = "$label — Thermal Capacity by Fuel",
#         xaxis_title = "Fuel Type",
#         yaxis_title = "MW",
#         barmode     = "group",
#         width       = 900,
#         height      = 500,
#     ))

#     display(p1)

# end

# # ── Run for all states ─────────────────────────────────────────────────────────
# thermal_initial_comparison("VA", va_thermal, eia_expanded)
# thermal_initial_comparison("MD", md_thermal, eia_expanded)
# thermal_initial_comparison("WV", wv_thermal, eia_expanded)

# # ── Run for all states ─────────────────────────────────────────────────────────
# thermal_initial_comparison("VA", va_thermal, eia_expanded)
# thermal_initial_comparison("MD", md_thermal, eia_expanded)
# thermal_initial_comparison("WV", wv_thermal, eia_expanded)

# # ── Prepare MMWG slim for thermal lookup ──────────────────────────────────────
# mmwg_slim = DataFrames.select(
#     mmwg_data_full,
#     [" Bus Number", "English Name", " Bus kV", "Load Flow  Bus Name",
#      "EIA Plant Code", "Region/PC"]
# )

# # Rename to remove leading spaces
# DataFrames.rename!(mmwg_slim, 
#     " Bus Number" => "Bus Number",
#     " Bus kV"     => "Bus kV"
# )

# mmwg_slim = unique(mmwg_slim)
# println("\nMMWG slim prepared: ", nrow(mmwg_slim), " unique buses")
# # ── MMWG lookup columns ────────────────────────────────────────────────────────
# const MMWG_COLS_THERMAL = [
#     "name", "bus_number", "bus", "generator_type", "prime_mover_type",
#     "rating", "fuel", "Bus Number", "English Name", "Bus kV",
#     "EIA Plant Code", "Region/PC"
# ]

# const UNMATCHED_COLS_THERMAL = [
#     "name", "bus_number", "bus", "generator_type", "prime_mover_type",
#     "rating", "fuel"
# ]

# # ── MMWG fallback for unmatched thermals ───────────────────────────────────────
# function mmwg_lookup_thermal(label::String, unmatched::DataFrame, mmwg_slim::DataFrame)
#     result = leftjoin(
#         unmatched,
#         mmwg_slim,
#         on = "bus_number" => "Bus Number",
#         matchmissing = :notequal
#     )

#     mmwg_matched   = filter(row -> !ismissing(row["EIA Plant Code"]), result)
#     mmwg_unmatched = filter(row ->  ismissing(row["EIA Plant Code"]), result)

#     println("\n", "="^60)
#     println("$label — Unmatched Thermal → MMWG Fallback")
#     println("="^60)
#     println("  ✅ Found in MMWG:    ", nrow(mmwg_matched))
#     println("  ⚠️  Still unmatched: ", nrow(mmwg_unmatched))
#     println("="^60)

#     if nrow(mmwg_matched) > 0
#         println("\n📋 $label found in MMWG (", nrow(mmwg_matched), " rows):")
#         # Use only columns that exist in the result
#         avail_cols = intersect(MMWG_COLS_THERMAL, names(mmwg_matched))
#         show(sort(DataFrames.select(mmwg_matched, avail_cols), "bus_number"),
#              allrows=true)
#     else
#         println("\n📋 $label found in MMWG: (none)")
#     end

#     if nrow(mmwg_unmatched) > 0
#         println("\n⚠️  $label still unmatched after MMWG (", nrow(mmwg_unmatched), " rows):")
#         avail_cols = intersect(UNMATCHED_COLS_THERMAL, names(mmwg_unmatched))
#         show(sort(DataFrames.select(mmwg_unmatched, avail_cols), "bus_number"),
#              allrows=true)
#     else
#         println("\n⚠️  $label still unmatched: (none)")
#     end

#     return mmwg_matched, mmwg_unmatched
# end

# # ── Run MMWG lookup for each state ─────────────────────────────────────────────
# va_thermal_mmwg, va_thermal_still_unmatched =
#     mmwg_lookup_thermal("VA", va_thermal_unmatched, mmwg_slim)

# md_thermal_mmwg, md_thermal_still_unmatched =
#     mmwg_lookup_thermal("MD", md_thermal_unmatched, mmwg_slim)

# wv_thermal_mmwg, wv_thermal_still_unmatched =
#     mmwg_lookup_thermal("WV", wv_thermal_unmatched, mmwg_slim)

# # ── Final summary ───────────────────────────────────────────────────────────────
# println("\n", "="^60)
# println("📊 Final Thermal Summary by State")
# println("="^60)
# for (label, matched, mmwg, unmatched) in [
#     ("VA", va_thermal_matched, va_thermal_mmwg, va_thermal_still_unmatched),
#     ("MD", md_thermal_matched, md_thermal_mmwg, md_thermal_still_unmatched),
#     ("WV", wv_thermal_matched, wv_thermal_mmwg, wv_thermal_still_unmatched),
# ]
#     println("\n$label:")
#     println("  ✅ EIA2PF matched:      ", nrow(matched))
#     println("  ✅ MMWG matched:        ", nrow(mmwg))
#     println("  ⚠️  Still unmatched:    ", nrow(unmatched))
# end
# println("="^60)

# # ── MMWG fallback per fuel — VA ───────────────────────────────────────────────
# va_ng_mmwg,       va_ng_still_unmatched       = mmwg_lookup_thermal("VA", va_ng_unmatched,       mmwg_slim)
# # va_oil_dist_mmwg, va_oil_dist_still_unmatched = mmwg_lookup_thermal("VA", va_oil_dist_unmatched, mmwg_slim)
# va_oil_res_mmwg,  va_oil_res_still_unmatched  = mmwg_lookup_thermal("VA", va_oil_res_unmatched,  mmwg_slim)
# va_coal_mmwg,     va_coal_still_unmatched     = mmwg_lookup_thermal("VA", va_coal_unmatched,     mmwg_slim)
# # va_nuclear_mmwg,  va_nuclear_still_unmatched  = mmwg_lookup_thermal("VA", va_nuclear_unmatched,  mmwg_slim)
# # va_wood_mmwg,     va_wood_still_unmatched     = mmwg_lookup_thermal("VA", va_wood_unmatched,     mmwg_slim)
# va_other_mmwg,    va_other_still_unmatched    = mmwg_lookup_thermal("VA", va_other_unmatched,    mmwg_slim)

# # ── MMWG fallback per fuel — MD ───────────────────────────────────────────────
# md_ng_mmwg,       md_ng_still_unmatched       = mmwg_lookup_thermal("MD", md_ng_unmatched,       mmwg_slim)
# #md_oil_dist_mmwg, md_oil_dist_still_unmatched = mmwg_lookup_thermal("MD", md_oil_dist_unmatched, mmwg_slim)
# #md_oil_res_mmwg,  md_oil_res_still_unmatched  = mmwg_lookup_thermal("MD", md_oil_res_unmatched,  mmwg_slim)
# #md_coal_mmwg,     md_coal_still_unmatched     = mmwg_lookup_thermal("MD", md_coal_unmatched,     mmwg_slim)
# #md_nuclear_mmwg,  md_nuclear_still_unmatched  = mmwg_lookup_thermal("MD", md_nuclear_unmatched,  mmwg_slim)
# md_other_mmwg,    md_other_still_unmatched    = mmwg_lookup_thermal("MD", md_other_unmatched,    mmwg_slim)

# # ── MMWG fallback per fuel — WV ───────────────────────────────────────────────
# wv_ng_mmwg,    wv_ng_still_unmatched    = mmwg_lookup_thermal("WV", wv_ng_unmatched,    mmwg_slim)
# #wv_coal_mmwg,  wv_coal_still_unmatched  = mmwg_lookup_thermal("WV", wv_coal_unmatched,  mmwg_slim)
# #wv_jet_mmwg,   wv_jet_still_unmatched   = mmwg_lookup_thermal("WV", wv_jet_unmatched,   mmwg_slim)
# #wv_other_mmwg, wv_other_still_unmatched = mmwg_lookup_thermal("WV", wv_other_unmatched, mmwg_slim)

# # ── Step 3: Find EIA_ONLY thermal generators ──────────────────────────────────

# # ── Thermal technology → fuel category mapping ────────────────────────────────
# const TECH_TO_FUEL_CAT = Dict(
#     "Natural Gas Fired Combined Cycle"            => "NATURAL_GAS",
#     "Natural Gas Fired Combustion Turbine"        => "NATURAL_GAS",
#     "Natural Gas Steam Turbine"                   => "NATURAL_GAS",
#     "Natural Gas Internal Combustion Engine"      => "NATURAL_GAS",
#     "Natural Gas with Compressed Air Storage"     => "NATURAL_GAS",
#     "Other Natural Gas"                           => "NATURAL_GAS",
#     "Other Gases"                                 => "NATURAL_GAS",
#     "Conventional Steam Coal"                     => "COAL",
#     "Coal Integrated Gasification Combined Cycle" => "COAL",
#     "Petroleum Liquids"                           => "OIL",
#     "Petroleum Coke"                              => "OIL",
#     "Nuclear"                                     => "NUCLEAR",
#     "Wood/Wood Waste Biomass"                     => "WOOD_WASTE_SOLIDS",
#     "Landfill Gas"                                => "OTHER",
#     "Municipal Solid Waste"                       => "OTHER",
#     "Other Waste Biomass"                         => "OTHER",
#     "All Other"                                   => "OTHER",
# )

# # Technologies to include in thermal EIA-only search
# const THERMAL_TECH_SET = Set(keys(TECH_TO_FUEL_CAT))

# # ── Find EIA thermal not in EI, by state and fuel category ───────────────────
# function find_eia_thermal_not_in_ei_by_fuel(
#     label::String,
#     ei_matched_dfs::Dict{String, DataFrame},   # fuel => matched df (from EIA2PF)
#     ei_mmwg_dfs::Dict{String, DataFrame},      # fuel => mmwg matched df
#     eia_expanded::DataFrame,
#     state::String,
# )
#     # ── Collect all uids already matched to EI ────────────────────────────────
#     consumed = Set{String}()
#     for df in values(ei_matched_dfs)
#         "uid" in names(df) || continue
#         union!(consumed, (string(u) for u in skipmissing(df[!, "uid"])))
#     end
#     for df in values(ei_mmwg_dfs)
#         "uid" in names(df) || continue
#         union!(consumed, (string(u) for u in skipmissing(df[!, "uid"])))
#     end

#     # ── Filter EIA to this state + thermal technologies only ──────────────────
#     eia_state = filter(row ->
#         coalesce(row["State"] == state, false) &&
#         coalesce(row["Technology"], "") in THERMAL_TECH_SET,
#         eia_expanded)

#     # ── Remove rows already matched ───────────────────────────────────────────
#     eia_unmatched = filter(row ->
#         ismissing(row["uid"]) || !(string(row["uid"]) in consumed),
#         eia_state)

#     # ── Add fuel category column ──────────────────────────────────────────────
#     eia_unmatched[!, "fuel_cat"] = [
#         get(TECH_TO_FUEL_CAT, coalesce(string(r["Technology"]), ""), "OTHER")
#         for r in eachrow(eia_unmatched)
#     ]

#     # ── Split by has bus / has neither ────────────────────────────────────────
#     has_both    = filter(row -> !ismissing(row["BusID_int"]) &&
#                                 !ismissing(row["BusName"]) &&
#                                 row["BusName"] != "", eia_unmatched)
#     has_neither = filter(row ->  ismissing(row["BusID_int"]) &&
#                                 (ismissing(row["BusName"]) || row["BusName"] == ""),
#                          eia_unmatched)

#     # ── Summary ───────────────────────────────────────────────────────────────
#     println("\n", "█"^70)
#     println("  $label — EIA Thermal NOT in EI model")
#     println("█"^70)
#     println("  Total EIA thermal ($state):         ", nrow(eia_state))
#     println("  Already matched to EI:              ", nrow(eia_state) - nrow(eia_unmatched))
#     println("  ⚠️  Not matched (to add):            ", nrow(eia_unmatched))
#     println("─"^70)
#     println("    ✅ Has BusID + BusName:            ", nrow(has_both))
#     println("    ❌ Has neither:                    ", nrow(has_neither))
#     println("─"^70)

#     # ── Breakdown by fuel category ────────────────────────────────────────────
#     println("  By fuel category:")
#     for fuel_cat in sort(unique(eia_unmatched[!, "fuel_cat"]))
#         n_both = count(r -> r.fuel_cat == fuel_cat, eachrow(has_both))
#         n_neit = count(r -> r.fuel_cat == fuel_cat, eachrow(has_neither))
#         mw     = round(sum(Float64.(coalesce.(
#                     filter(r -> r.fuel_cat == fuel_cat, eia_unmatched).eia_capacity_mw,
#                     0.0))), digits=1)
#         println("    $(rpad(fuel_cat*":", 22)) $(lpad(n_both+n_neit, 4)) total",
#                 " | $(lpad(n_both, 4)) w/bus | $(lpad(n_neit, 4)) no bus | $(lpad(mw, 8)) MW")
#     end
#     println("█"^70)

#     # ── Split has_both by fuel category ──────────────────────────────────────
#     has_both_by_fuel    = Dict{String, DataFrame}()
#     has_neither_by_fuel = Dict{String, DataFrame}()

#     for fuel_cat in sort(unique(eia_unmatched[!, "fuel_cat"]))
#         has_both_by_fuel[fuel_cat]    = filter(r -> r.fuel_cat == fuel_cat, has_both)
#         has_neither_by_fuel[fuel_cat] = filter(r -> r.fuel_cat == fuel_cat, has_neither)
#     end

#     # ── Detail display ────────────────────────────────────────────────────────
#     KEY_COLS_BOTH = [
#         "Plant ID", "Plant Name",
#         "fuel_cat", "Prime Mover Code", "Technology",
#         "eia_capacity_mw", "eia860_status",
#         "BusID_int", "BusName", "kV",
#         "eia_lat", "eia_lon",
#     ]
#     KEY_COLS_NEITHER = [
#         "Plant ID", "Plant Name",
#         "fuel_cat", "Prime Mover Code", "Technology",
#         "eia_capacity_mw", "eia860_status",
#         "eia_lat", "eia_lon",
#     ]

#     for fuel_cat in sort(unique(eia_unmatched[!, "fuel_cat"]))
#         b = has_both_by_fuel[fuel_cat]
#         n = has_neither_by_fuel[fuel_cat]

#         println("\n  ── $fuel_cat ──────────────────────────────────────────")
#         println("  ✅ Has BusID + BusName ($(nrow(b)) rows):")
#         if nrow(b) > 0
#             show(DataFrames.select(b, intersect(KEY_COLS_BOTH, names(b))), allrows=true)
#         else
#             println("    (none)")
#         end

#         println("\n  ❌ Has neither ($(nrow(n)) rows):")
#         if nrow(n) > 0
#             show(DataFrames.select(n, intersect(KEY_COLS_NEITHER, names(n))), allrows=true)
#         else
#             println("    (none)")
#         end
#     end
#     return eia_unmatched, has_both, has_neither, has_both_by_fuel, has_neither_by_fuel
# end

# # ── Build mmwg dicts from explicit named variables ────────────────────────────
# va_ei_matched_by_fuel = Dict(
#     "NATURAL_GAS"        => va_ng_matched,
#     "DISTILLATE_FUEL_OIL"=> va_oil_dist_matched,
#     "RESIDUAL_FUEL_OIL"  => va_oil_res_matched,
#     "COAL"               => va_coal_matched,
#     "NUCLEAR"            => va_nuclear_matched,
#     "WOOD_WASTE_SOLIDS"  => va_wood_matched,
#     "OTHER"              => va_other_matched,
# )
# va_ei_mmwg_by_fuel = Dict(
#     "NATURAL_GAS"        => va_ng_mmwg,
#     "COAL"               => va_coal_mmwg,
#     "RESIDUAL_FUEL_OIL"  => va_oil_res_mmwg,
#     "OTHER"              => va_other_mmwg,
# )

# md_ei_matched_by_fuel = Dict(
#     "NATURAL_GAS"        => md_ng_matched,
#     "DISTILLATE_FUEL_OIL"=> md_oil_dist_matched,
#     "RESIDUAL_FUEL_OIL"  => md_oil_res_matched,
#     "COAL"               => md_coal_matched,
#     "NUCLEAR"            => md_nuclear_matched,
#     "OTHER"              => md_other_matched,
# )
# md_ei_mmwg_by_fuel = Dict(
#     "NATURAL_GAS"        => md_ng_mmwg,
#     "OTHER"              => md_other_mmwg,
# )

# wv_ei_matched_by_fuel = Dict(
#     "NATURAL_GAS"        => wv_ng_matched,
#     "COAL"               => wv_coal_matched,
#     "JET_FUEL"           => wv_jet_matched,
#     "OTHER"              => wv_other_matched,
# )
# wv_ei_mmwg_by_fuel = Dict(
#     "NATURAL_GAS"        => wv_ng_mmwg,
# )

# # ── Run for each state ────────────────────────────────────────────────────────
# va_thermal_eia_unmatched, va_thermal_has_both, va_thermal_has_neither,
#     va_thermal_has_both_by_fuel, va_thermal_has_neither_by_fuel =
#     find_eia_thermal_not_in_ei_by_fuel("VA", va_ei_matched_by_fuel,
#                                         va_ei_mmwg_by_fuel, eia_expanded, "VA")

# md_thermal_eia_unmatched, md_thermal_has_both, md_thermal_has_neither,
#     md_thermal_has_both_by_fuel, md_thermal_has_neither_by_fuel =
#     find_eia_thermal_not_in_ei_by_fuel("MD", md_ei_matched_by_fuel,
#                                         md_ei_mmwg_by_fuel, eia_expanded, "MD")

# wv_thermal_eia_unmatched, wv_thermal_has_both, wv_thermal_has_neither,
#     wv_thermal_has_both_by_fuel, wv_thermal_has_neither_by_fuel =
#     find_eia_thermal_not_in_ei_by_fuel("WV", wv_ei_matched_by_fuel,
#                                         wv_ei_mmwg_by_fuel, eia_expanded, "WV")


# # ── Unpack into individual named DataFrames — VA ──────────────────────────────
# va_ng_eia_has_both       = get(va_thermal_has_both_by_fuel,    "NATURAL_GAS",         DataFrame())
# va_ng_eia_has_neither    = get(va_thermal_has_neither_by_fuel, "NATURAL_GAS",         DataFrame())
# va_oil_eia_has_both      = get(va_thermal_has_both_by_fuel,    "OIL",                 DataFrame())
# va_oil_eia_has_neither   = get(va_thermal_has_neither_by_fuel, "OIL",                 DataFrame())
# va_coal_eia_has_both     = get(va_thermal_has_both_by_fuel,    "COAL",                DataFrame())
# va_coal_eia_has_neither  = get(va_thermal_has_neither_by_fuel, "COAL",                DataFrame())
# va_nuclear_eia_has_both  = get(va_thermal_has_both_by_fuel,    "NUCLEAR",             DataFrame())
# va_nuclear_eia_has_neither = get(va_thermal_has_neither_by_fuel, "NUCLEAR",           DataFrame())
# va_wood_eia_has_both     = get(va_thermal_has_both_by_fuel,    "WOOD_WASTE_SOLIDS",   DataFrame())
# va_wood_eia_has_neither  = get(va_thermal_has_neither_by_fuel, "WOOD_WASTE_SOLIDS",   DataFrame())
# va_other_eia_has_both    = get(va_thermal_has_both_by_fuel,    "OTHER",               DataFrame())
# va_other_eia_has_neither = get(va_thermal_has_neither_by_fuel, "OTHER",               DataFrame())

# # ── Unpack into individual named DataFrames — MD ──────────────────────────────
# md_ng_eia_has_both       = get(md_thermal_has_both_by_fuel,    "NATURAL_GAS",         DataFrame())
# md_ng_eia_has_neither    = get(md_thermal_has_neither_by_fuel, "NATURAL_GAS",         DataFrame())
# md_oil_eia_has_both      = get(md_thermal_has_both_by_fuel,    "OIL",                 DataFrame())
# md_oil_eia_has_neither   = get(md_thermal_has_neither_by_fuel, "OIL",                 DataFrame())
# md_coal_eia_has_both     = get(md_thermal_has_both_by_fuel,    "COAL",                DataFrame())
# md_coal_eia_has_neither  = get(md_thermal_has_neither_by_fuel, "COAL",                DataFrame())
# md_nuclear_eia_has_both  = get(md_thermal_has_both_by_fuel,    "NUCLEAR",             DataFrame())
# md_nuclear_eia_has_neither = get(md_thermal_has_neither_by_fuel, "NUCLEAR",           DataFrame())
# md_other_eia_has_both    = get(md_thermal_has_both_by_fuel,    "OTHER",               DataFrame())
# md_other_eia_has_neither = get(md_thermal_has_neither_by_fuel, "OTHER",               DataFrame())

# # ── Unpack into individual named DataFrames — WV ──────────────────────────────
# wv_ng_eia_has_both       = get(wv_thermal_has_both_by_fuel,    "NATURAL_GAS",         DataFrame())
# wv_ng_eia_has_neither    = get(wv_thermal_has_neither_by_fuel, "NATURAL_GAS",         DataFrame())
# wv_coal_eia_has_both     = get(wv_thermal_has_both_by_fuel,    "COAL",                DataFrame())
# wv_coal_eia_has_neither  = get(wv_thermal_has_neither_by_fuel, "COAL",                DataFrame())
# wv_other_eia_has_both    = get(wv_thermal_has_both_by_fuel,    "OTHER",               DataFrame())
# wv_other_eia_has_neither = get(wv_thermal_has_neither_by_fuel, "OTHER",               DataFrame())

# # ── Helper to reorder columns for display ─────────────────────────────────────
# function reorder_eia_cols(df::DataFrame)
#     nrow(df) == 0 && return df
#     priority = [
#         "Plant ID", "Plant Name",
#         "fuel_cat", "Prime Mover Code", "Technology",
#         "eia_capacity_mw", "eia860_status",
#         "BusID_int", "BusName", "kV",
#         "eia_lat", "eia_lon",
#     ]
#     ordered = vcat(intersect(priority, names(df)), setdiff(names(df), priority))
#     return DataFrames.select(df, ordered)
# end

# # ── Unpack into individual named DataFrames — VA ──────────────────────────────
# va_ng_eia_has_both         = reorder_eia_cols(get(va_thermal_has_both_by_fuel,    "NATURAL_GAS",       DataFrame()))
# va_ng_eia_has_neither      = reorder_eia_cols(get(va_thermal_has_neither_by_fuel, "NATURAL_GAS",       DataFrame()))
# va_oil_eia_has_both        = reorder_eia_cols(get(va_thermal_has_both_by_fuel,    "OIL",               DataFrame()))
# va_oil_eia_has_neither     = reorder_eia_cols(get(va_thermal_has_neither_by_fuel, "OIL",               DataFrame()))
# va_coal_eia_has_both       = reorder_eia_cols(get(va_thermal_has_both_by_fuel,    "COAL",              DataFrame()))
# va_coal_eia_has_neither    = reorder_eia_cols(get(va_thermal_has_neither_by_fuel, "COAL",              DataFrame()))
# va_nuclear_eia_has_both    = reorder_eia_cols(get(va_thermal_has_both_by_fuel,    "NUCLEAR",           DataFrame()))
# va_nuclear_eia_has_neither = reorder_eia_cols(get(va_thermal_has_neither_by_fuel, "NUCLEAR",           DataFrame()))
# va_wood_eia_has_both       = reorder_eia_cols(get(va_thermal_has_both_by_fuel,    "WOOD_WASTE_SOLIDS", DataFrame()))
# va_wood_eia_has_neither    = reorder_eia_cols(get(va_thermal_has_neither_by_fuel, "WOOD_WASTE_SOLIDS", DataFrame()))
# va_other_eia_has_both      = reorder_eia_cols(get(va_thermal_has_both_by_fuel,    "OTHER",             DataFrame()))
# va_other_eia_has_neither   = reorder_eia_cols(get(va_thermal_has_neither_by_fuel, "OTHER",             DataFrame()))

# # ── Unpack into individual named DataFrames — MD ──────────────────────────────
# md_ng_eia_has_both         = reorder_eia_cols(get(md_thermal_has_both_by_fuel,    "NATURAL_GAS",       DataFrame()))
# md_ng_eia_has_neither      = reorder_eia_cols(get(md_thermal_has_neither_by_fuel, "NATURAL_GAS",       DataFrame()))
# md_oil_eia_has_both        = reorder_eia_cols(get(md_thermal_has_both_by_fuel,    "OIL",               DataFrame()))
# md_oil_eia_has_neither     = reorder_eia_cols(get(md_thermal_has_neither_by_fuel, "OIL",               DataFrame()))
# md_coal_eia_has_both       = reorder_eia_cols(get(md_thermal_has_both_by_fuel,    "COAL",              DataFrame()))
# md_coal_eia_has_neither    = reorder_eia_cols(get(md_thermal_has_neither_by_fuel, "COAL",              DataFrame()))
# md_nuclear_eia_has_both    = reorder_eia_cols(get(md_thermal_has_both_by_fuel,    "NUCLEAR",           DataFrame()))
# md_nuclear_eia_has_neither = reorder_eia_cols(get(md_thermal_has_neither_by_fuel, "NUCLEAR",           DataFrame()))
# md_other_eia_has_both      = reorder_eia_cols(get(md_thermal_has_both_by_fuel,    "OTHER",             DataFrame()))
# md_other_eia_has_neither   = reorder_eia_cols(get(md_thermal_has_neither_by_fuel, "OTHER",             DataFrame()))

# # ── Unpack into individual named DataFrames — WV ──────────────────────────────
# wv_ng_eia_has_both         = reorder_eia_cols(get(wv_thermal_has_both_by_fuel,    "NATURAL_GAS",       DataFrame()))
# wv_ng_eia_has_neither      = reorder_eia_cols(get(wv_thermal_has_neither_by_fuel, "NATURAL_GAS",       DataFrame()))
# wv_coal_eia_has_both       = reorder_eia_cols(get(wv_thermal_has_both_by_fuel,    "COAL",              DataFrame()))
# wv_coal_eia_has_neither    = reorder_eia_cols(get(wv_thermal_has_neither_by_fuel, "COAL",              DataFrame()))
# wv_other_eia_has_both      = reorder_eia_cols(get(wv_thermal_has_both_by_fuel,    "OTHER",             DataFrame()))
# wv_other_eia_has_neither   = reorder_eia_cols(get(wv_thermal_has_neither_by_fuel, "OTHER",             DataFrame()))

# # ── Quick inventory ───────────────────────────────────────────────────────────
# println("\n", "="^70)
# println("  EIA-only Thermal Inventory (has_both / has_neither)")
# println("="^70)
# for (name, b, n) in [
#     ("va_ng",      va_ng_eia_has_both,      va_ng_eia_has_neither),
#     ("va_oil",     va_oil_eia_has_both,     va_oil_eia_has_neither),
#     ("va_coal",    va_coal_eia_has_both,    va_coal_eia_has_neither),
#     ("va_nuclear", va_nuclear_eia_has_both, va_nuclear_eia_has_neither),
#     ("va_wood",    va_wood_eia_has_both,    va_wood_eia_has_neither),
#     ("va_other",   va_other_eia_has_both,   va_other_eia_has_neither),
#     ("md_ng",      md_ng_eia_has_both,      md_ng_eia_has_neither),
#     ("md_oil",     md_oil_eia_has_both,     md_oil_eia_has_neither),
#     ("md_coal",    md_coal_eia_has_both,    md_coal_eia_has_neither),
#     ("md_nuclear", md_nuclear_eia_has_both, md_nuclear_eia_has_neither),
#     ("md_other",   md_other_eia_has_both,   md_other_eia_has_neither),
#     ("wv_ng",      wv_ng_eia_has_both,      wv_ng_eia_has_neither),
#     ("wv_coal",    wv_coal_eia_has_both,    wv_coal_eia_has_neither),
#     ("wv_other",   wv_other_eia_has_both,   wv_other_eia_has_neither),
# ]
#     safe_mw(df) = (nrow(df) == 0 || !("eia_capacity_mw" in names(df))) ? 0.0 :
#                   round(sum(Float64.(coalesce.(df.eia_capacity_mw, 0.0))), digits=1)

#     b_mw   = safe_mw(b)
#     n_mw   = safe_mw(n)
#     tot    = nrow(b) + nrow(n)
#     tot_mw = b_mw + n_mw

#     @printf("  %-12s  has_both: %3d (%8.1f MW)  |  has_neither: %3d (%8.1f MW)  |  total: %3d (%8.1f MW)\n",
#             name*":", nrow(b), b_mw, nrow(n), n_mw, tot, tot_mw)
# end
# println("="^70)

# # ── Match thermal 'neither' to MMWG by plant name — by state and fuel ─────────
# function match_thermal_neither_to_mmwg(label::String, fuel::String,
#     neither::DataFrame, mmwg_slim::DataFrame)
# println("\n", "="^60)
# println("$label [$fuel] — EIA thermal 'neither' → MMWG name match")
# println("="^60)

# nrow(neither) == 0 && (println("  ℹ️  No 'neither' rows — skipping"); 
# println("="^60); 
# return DataFrame(), DataFrame())

# region = get(STATE_REGION, label, missing)
# mmwg_available = !ismissing(region) ?
# filter(r -> coalesce(r["Region/PC"] == region, false), mmwg_slim) : mmwg_slim

# println("  MMWG available: ", nrow(mmwg_available), " (region filtered)")

# normalize(s) = ismissing(s) ? "" : lowercase(strip(string(s)))
# matches  = DataFrame()
# no_match = DataFrame()

# for row in eachrow(neither)
# plant_name = normalize(row["Plant Name"])
# mmwg_match = filter(r -> normalize(r["English Name"]) == plant_name, mmwg_available)
# if nrow(mmwg_match) > 1
# println("  ⚠️  Multiple MMWG matches for '$(row["Plant Name"])' ($(nrow(mmwg_match)) rows) — taking first")
# end
# if nrow(mmwg_match) > 0
# new_row = copy(DataFrame(row))
# new_row[!, "Bus Number"]          .= mmwg_match[1, "Bus Number"]
# new_row[!, "Load Flow  Bus Name"] .= mmwg_match[1, "Load Flow  Bus Name"]
# new_row[!, "English Name"]        .= mmwg_match[1, "English Name"]
# new_row[!, "EIA Plant Code"]      .= mmwg_match[1, "EIA Plant Code"]
# new_row[!, "Bus kV"]              .= mmwg_match[1, "Bus kV"]
# new_row[!, "Region/PC"]           .= mmwg_match[1, "Region/PC"]
# append!(matches, new_row, promote=true)
# else
# append!(no_match, DataFrame(row), promote=true)
# end
# end

# println("  Total 'neither':     ", nrow(neither))
# println("  ✅ Matched via name: ", nrow(matches))
# println("  ⚠️  Still no match:  ", nrow(no_match))
# println("="^60)

# MATCH_COLS = ["Plant ID", "Plant Name", "Generator ID", "fuel_cat",
# "eia_capacity_mw", "Technology", "Prime Mover Code",
# "Bus Number", "Load Flow  Bus Name", "English Name",
# "EIA Plant Code", "Bus kV", "Region/PC"]
# NO_MATCH_COLS = ["Plant ID", "Plant Name", "Generator ID", "fuel_cat",
# "eia_capacity_mw", "Technology", "Prime Mover Code"]

# println("\n  ✅ Matched (", nrow(matches), " rows):")
# nrow(matches) > 0 ?
# show(DataFrames.select(matches, intersect(MATCH_COLS, names(matches))), allrows=true) :
# println("    (none)")

# println("\n  ⚠️  Still no match (", nrow(no_match), " rows):")
# nrow(no_match) > 0 ?
# show(DataFrames.select(no_match, intersect(NO_MATCH_COLS, names(no_match))), allrows=true) :
# println("    (none)")

# return matches, no_match
# end

# # ── VA ────────────────────────────────────────────────────────────────────────
# va_ng_neither_mmwg,      va_ng_neither_unmatched      = match_thermal_neither_to_mmwg("VA", "NATURAL_GAS",       va_ng_eia_has_neither,      mmwg_slim)
# va_oil_neither_mmwg,     va_oil_neither_unmatched     = match_thermal_neither_to_mmwg("VA", "OIL",               va_oil_eia_has_neither,     mmwg_slim)
# va_coal_neither_mmwg,    va_coal_neither_unmatched    = match_thermal_neither_to_mmwg("VA", "COAL",              va_coal_eia_has_neither,    mmwg_slim)
# va_nuclear_neither_mmwg, va_nuclear_neither_unmatched = match_thermal_neither_to_mmwg("VA", "NUCLEAR",           va_nuclear_eia_has_neither, mmwg_slim)
# va_wood_neither_mmwg,    va_wood_neither_unmatched    = match_thermal_neither_to_mmwg("VA", "WOOD_WASTE_SOLIDS", va_wood_eia_has_neither,    mmwg_slim)
# va_other_neither_mmwg,   va_other_neither_unmatched   = match_thermal_neither_to_mmwg("VA", "OTHER",             va_other_eia_has_neither,   mmwg_slim)

# # ── MD ────────────────────────────────────────────────────────────────────────
# md_ng_neither_mmwg,      md_ng_neither_unmatched      = match_thermal_neither_to_mmwg("MD", "NATURAL_GAS",       md_ng_eia_has_neither,      mmwg_slim)
# md_oil_neither_mmwg,     md_oil_neither_unmatched     = match_thermal_neither_to_mmwg("MD", "OIL",               md_oil_eia_has_neither,     mmwg_slim)
# md_coal_neither_mmwg,    md_coal_neither_unmatched    = match_thermal_neither_to_mmwg("MD", "COAL",              md_coal_eia_has_neither,    mmwg_slim)
# md_nuclear_neither_mmwg, md_nuclear_neither_unmatched = match_thermal_neither_to_mmwg("MD", "NUCLEAR",           md_nuclear_eia_has_neither, mmwg_slim)
# md_other_neither_mmwg,   md_other_neither_unmatched   = match_thermal_neither_to_mmwg("MD", "OTHER",             md_other_eia_has_neither,   mmwg_slim)

# # ── WV ────────────────────────────────────────────────────────────────────────
# wv_ng_neither_mmwg,    wv_ng_neither_unmatched    = match_thermal_neither_to_mmwg("WV", "NATURAL_GAS", wv_ng_eia_has_neither,    mmwg_slim)
# wv_coal_neither_mmwg,  wv_coal_neither_unmatched  = match_thermal_neither_to_mmwg("WV", "COAL",        wv_coal_eia_has_neither,  mmwg_slim)
# wv_other_neither_mmwg, wv_other_neither_unmatched = match_thermal_neither_to_mmwg("WV", "OTHER",       wv_other_eia_has_neither, mmwg_slim)

# # ── Summary across all states and fuels ───────────────────────────────────────
# println("\n", "="^70)
# println("  Thermal 'neither' → MMWG Name Match Summary")
# println("="^70)
# @printf("  %-25s %6s %9s %6s %9s %6s %9s\n",
#         "State/Fuel", "In", "In MW", "Match", "Match MW", "Still", "Still MW")
# println("  ", "─"^75)

# safe_mw(df) = (nrow(df) == 0 || !("eia_capacity_mw" in names(df))) ? 0.0 :
#               round(sum(Float64.(coalesce.(df.eia_capacity_mw, 0.0))), digits=1)

# for (name, neither, matched, still) in [
#     ("VA / NATURAL_GAS",       va_ng_eia_has_neither,      va_ng_neither_mmwg,      va_ng_neither_unmatched),
#     ("VA / OIL",               va_oil_eia_has_neither,     va_oil_neither_mmwg,     va_oil_neither_unmatched),
#     ("VA / COAL",              va_coal_eia_has_neither,    va_coal_neither_mmwg,    va_coal_neither_unmatched),
#     ("VA / NUCLEAR",           va_nuclear_eia_has_neither, va_nuclear_neither_mmwg, va_nuclear_neither_unmatched),
#     ("VA / WOOD_WASTE_SOLIDS", va_wood_eia_has_neither,    va_wood_neither_mmwg,    va_wood_neither_unmatched),
#     ("VA / OTHER",             va_other_eia_has_neither,   va_other_neither_mmwg,   va_other_neither_unmatched),
#     ("MD / NATURAL_GAS",       md_ng_eia_has_neither,      md_ng_neither_mmwg,      md_ng_neither_unmatched),
#     ("MD / OIL",               md_oil_eia_has_neither,     md_oil_neither_mmwg,     md_oil_neither_unmatched),
#     ("MD / COAL",              md_coal_eia_has_neither,    md_coal_neither_mmwg,    md_coal_neither_unmatched),
#     ("MD / NUCLEAR",           md_nuclear_eia_has_neither, md_nuclear_neither_mmwg, md_nuclear_neither_unmatched),
#     ("MD / OTHER",             md_other_eia_has_neither,   md_other_neither_mmwg,   md_other_neither_unmatched),
#     ("WV / NATURAL_GAS",       wv_ng_eia_has_neither,      wv_ng_neither_mmwg,      wv_ng_neither_unmatched),
#     ("WV / COAL",              wv_coal_eia_has_neither,    wv_coal_neither_mmwg,    wv_coal_neither_unmatched),
#     ("WV / OTHER",             wv_other_eia_has_neither,   wv_other_neither_mmwg,   wv_other_neither_unmatched),
# ]
#     nrow(neither) == 0 && continue
#     @printf("  %-25s %6d %9.1f %6d %9.1f %6d %9.1f\n",
#             name,
#             nrow(neither), safe_mw(neither),
#             nrow(matched),  safe_mw(matched),
#             nrow(still),    safe_mw(still))
# end
# println("  ", "─"^75)
# println("="^70)





















# # ── Step 3: Find EIA_ONLY thermal generators ──────────────────────────────────
# println("\n", "="^70)
# println("  STEP 3: EIA_ONLY — New Thermal Generators Not in EI")
# println("="^70)

# # Filter eia_expanded to thermal technologies only
# # ── Thermal technology filter (exclude renewables/storage) ────────────────────
# const THERMAL_TECHNOLOGIES = [
#     "Natural Gas Fired Combined Cycle",
#     "Natural Gas Fired Combustion Turbine",
#     "Natural Gas Steam Turbine",
#     "Natural Gas Internal Combustion Engine",
#     "Natural Gas with Compressed Air Storage",
#     "Other Natural Gas",                          # Added
#     "Coal Integrated Gasification Combined Cycle",
#     "Conventional Steam Coal",
#     "Petroleum Liquids",
#     "Petroleum Coke",
#     "Nuclear",
#     "Wood/Wood Waste Biomass",
#     "Landfill Gas",
#     "Municipal Solid Waste",
#     "Other Waste Biomass",
#     "Other Gases",
#     "All Other",                                  # Added (check if actually thermal)
#    # "Conventional Hydroelectric",                 # Hydro — decide if you want this
#    # "Hydroelectric Pumped Storage",              # Pumped storage — decide
# ]

# function find_eia_only_thermal(label::String, 
#     eia_expanded::DataFrame,
#     ei_matched::DataFrame,
#     mmwg_matched::DataFrame)

#     # Filter EIA to state + thermal tech + has bus assignment
#     state_eia = filter(r -> 
#     coalesce(r["State"], "") == label &&
#     coalesce(r["Technology"], "") in THERMAL_TECHNOLOGIES &&
#     !ismissing(r["BusID_int"]) &&
#     !ismissing(r["BusName"]),
#     eia_expanded
#     )

#     println("\n$label — EIA thermal generators with bus assignment: ", nrow(state_eia))

#     # Collect BusID_int values from EIA-matched rows (not bus_number from EI)
#     ei_buses = Set{Int}()

#     # From EIA2PF matched: get BusID_int column if it exists
#     if hasproperty(ei_matched, :BusID_int)
#         union!(ei_buses, skipmissing(ei_matched[!, :BusID_int]))
#     elseif hasproperty(ei_matched, Symbol("Bus Number"))
#         union!(ei_buses, skipmissing(ei_matched[!, Symbol("Bus Number")]))
#     end

#     # From MMWG matched: get Bus Number column
#     if hasproperty(mmwg_matched, Symbol("Bus Number"))
#         union!(ei_buses, skipmissing(mmwg_matched[!, Symbol("Bus Number")]))
#     elseif hasproperty(mmwg_matched, :bus_number)
#         union!(ei_buses, skipmissing(mmwg_matched[!, :bus_number]))
#     end

#     println("  EI buses already matched: ", length(ei_buses))

#     # Find EIA rows whose bus is NOT in EI
#     eia_only = filter(r -> !(r["BusID_int"] in ei_buses), state_eia)

#     println("  ✅ EIA_ONLY (new thermal gens): ", nrow(eia_only))

#     if nrow(eia_only) > 0
#         println("\n📋 $label — EIA_ONLY thermal generators:")
#         show_cols = intersect(
#         ["Plant ID", "Plant Name", "Generator ID", "Technology",
#         "Prime Mover Code", "eia_capacity_mw", "BusID_int", "BusName",
#         "kV", "eia860_status", "eia_lat", "eia_lon"],
#         names(eia_only)
#         )
#         show(sort(DataFrames.select(eia_only, show_cols), "BusID_int"), allrows=true)
#     end

# return eia_only

# end

# va_thermal_eia_only = find_eia_only_thermal("VA", eia_expanded, va_thermal_matched, va_thermal_mmwg)
# md_thermal_eia_only = find_eia_only_thermal("MD", eia_expanded, md_thermal_matched, md_thermal_mmwg)
# wv_thermal_eia_only = find_eia_only_thermal("WV", eia_expanded, wv_thermal_matched, wv_thermal_mmwg)

# println("\n", "="^70)
# println("  EIA_ONLY Summary")
# println("="^70)
# for (label, df) in [("VA", va_thermal_eia_only),
#                     ("MD", md_thermal_eia_only),
#                     ("WV", wv_thermal_eia_only)]
#     total_mw = nrow(df) > 0 ? round(sum(skipmissing(df.eia_capacity_mw)), digits=1) : 0.0
#     println("  $label: $(nrow(df)) generators | $(total_mw) MW")
# end
# println("="^70)


# # ── Find EIA thermal plants NOT in EI model ───────────────────────────────────
# function find_eia_thermal_unmatched(label::String, 
#     ei_matched::DataFrame,
#     mmwg_matched::DataFrame,
#     eia_expanded::DataFrame)

# # Collect all EIA Plant ID + Generator ID combinations already matched to EI
# matched_eia_keys = Set{Tuple{Any,Any}}()

# # From EIA2PF matched
# for row in eachrow(ei_matched)
# if !ismissing(row["Plant ID"]) && !ismissing(row["Generator ID"])
# push!(matched_eia_keys, (row["Plant ID"], row["Generator ID"]))
# end
# end

# # From MMWG matched (if it has EIA Plant Code)
# for row in eachrow(mmwg_matched)
# if hasproperty(row, Symbol("EIA Plant Code")) && !ismissing(row["EIA Plant Code"])
# # MMWG doesn't have Generator ID, so we can't use this approach
# # Skip for now — handle separately if needed
# end
# end

# # Filter EIA to state + thermal tech
# eia_state_thermal = filter(row -> 
# coalesce(row["State"], "") == label &&
# coalesce(row["Technology"], "") in THERMAL_TECHNOLOGIES,
# eia_expanded
# )

# # An EIA row is unmatched if (Plant ID, Generator ID) was never matched
# eia_unmatched = filter(row -> 
# !in((coalesce(row["Plant ID"], missing), 
# coalesce(row["Generator ID"], missing)), 
# matched_eia_keys),
# eia_state_thermal
# )

# # Classify by bus assignment
# has_both = filter(row -> 
# !ismissing(row["BusID_int"]) && 
# !ismissing(row["BusName"]) && 
# row["BusName"] != "",
# eia_unmatched
# )

# has_neither = filter(row -> 
# (ismissing(row["BusID_int"]) || row["BusID_int"] == 0) &&
# (ismissing(row["BusName"]) || row["BusName"] == ""),
# eia_unmatched
# )

# println("\n", "="^60)
# println("$label — EIA Thermal Plants NOT in EI model")
# println("="^60)
# println("  Total EIA thermal entries for $label: ", nrow(eia_state_thermal))
# println("  ⚠️  Not matched to any EI generator:  ", nrow(eia_unmatched))
# println("─"^60)
# println("  Classification:")
# println("    ✅ Has BusID + BusName:  ", nrow(has_both))
# println("    ❌ Has neither:          ", nrow(has_neither))
# println("="^60)

# EIA_UNMATCHED_COLS = [
# "Plant ID", "Plant Name", "Generator ID",
# "BusID_int", "BusName", "kV",
# "eia_capacity_mw", "Technology", "Prime Mover Code",
# "eia860_status", "eia_lat", "eia_lon"
# ]

# if nrow(has_both) > 0
# println("\n  ✅ Has BusID + BusName (", nrow(has_both), " rows):")
# show(sort(DataFrames.select(has_both, 
# intersect(EIA_UNMATCHED_COLS, names(has_both))),
# "BusID_int"), allrows=true)
# end

# if nrow(has_neither) > 0
# println("\n  ❌ Has neither BusID nor BusName (", nrow(has_neither), " rows):")
# show(sort(DataFrames.select(has_neither, 
# intersect(EIA_UNMATCHED_COLS, names(has_neither))),
# "Plant ID"), allrows=true)
# end

# return eia_unmatched, has_both, has_neither
# end

# # ── Run for each state ─────────────────────────────────────────────────────────
# va_thermal_eia_unmatched, va_thermal_has_both, va_thermal_neither =
# find_eia_thermal_unmatched("VA", va_thermal_matched, va_thermal_mmwg, eia_expanded)

# md_thermal_eia_unmatched, md_thermal_has_both, md_thermal_neither =
# find_eia_thermal_unmatched("MD", md_thermal_matched, md_thermal_mmwg, eia_expanded)

# wv_thermal_eia_unmatched, wv_thermal_has_both, wv_thermal_neither =
# find_eia_thermal_unmatched("WV", wv_thermal_matched, wv_thermal_mmwg, eia_expanded)

# println("\n", "="^70)
# println("  EIA Thermal Unmatched Summary")
# println("="^70)
# for (label, has_both, neither) in [
# ("VA", va_thermal_has_both, va_thermal_neither),
# ("MD", md_thermal_has_both, md_thermal_neither),
# ("WV", wv_thermal_has_both, wv_thermal_neither)
# ]
# total = nrow(has_both) + nrow(neither)
# println("  $label: $total unmatched | $(nrow(has_both)) with bus | $(nrow(neither)) without bus")
# end
# println("="^70)


# # va_thermal_export = build_thermal_export("VA", va_thermal_matched)
# # md_thermal_export = build_thermal_export("MD", md_thermal_matched)
# # wv_thermal_export = build_thermal_export("WV", wv_thermal_matched)

# # # ── 9. Write CSVs ──────────────────────────────────────────────────────────────
# # CSV.write(joinpath(OUTPUT_DIR, "thermal_export_VA.csv"), va_thermal_export)
# # CSV.write(joinpath(OUTPUT_DIR, "thermal_export_MD.csv"), md_thermal_export)
# # CSV.write(joinpath(OUTPUT_DIR, "thermal_export_WV.csv"), wv_thermal_export)

# # println("\n✅ Thermal exports written to: $OUTPUT_DIR")

# # # ── 10. Summary report ─────────────────────────────────────────────────────────
# # println("\n", "█"^70)
# # println("  THERMAL UPDATE SUMMARY")
# # println("█"^70)
# # for (label, df) in [("VA", va_thermal_export),
# #                     ("MD", md_thermal_export),
# #                     ("WV", wv_thermal_export)]
# #     total_mw = round(sum(skipmissing(df.eia_capacity_mw)), digits=1)
# #     println("  $label: $(nrow(df)) generators | $(total_mw) MW")
# # end
# # println("█"^70)






















# # ══════════════════════════════════════════════════════════════════════════════
# # THERMAL PIPELINE
# # ══════════════════════════════════════════════════════════════════════════════

# # ── Thermal prime mover codes ─────────────────────────────────────────────────
# const THERMAL_PM_CODES = ["CC", "CT", "OT", "ST", "IC", "CA", "CS", "GT"]

# # ── EIA technology → prime mover map (for filtering) ─────────────────────────
# const THERMAL_EIA_PM_CODES = [
#     "CC",   # Combined Cycle
#     "CT",   # Combustion Turbine
#     "ST",   # Steam Turbine
#     "GT",   # Gas Turbine
#     "IC",   # Internal Combustion
#     "CA",   # Combined Cycle - Steam Part
#     "CS",   # Combined Cycle Single Shaft
#     "OT",   # Other
#     "NB",   # Nuclear - Boiling Water
#     "NP",   # Nuclear - Pressurized Water
# ]

# # ── Column sets ───────────────────────────────────────────────────────────────
# const KEY_COLS_THERMAL = [
#     "name", "bus_number", "bus", "generator_type", "prime_mover_type",
#     "fuel", "rating", "eia_capacity_mw", "ts_column_name",
#     "lat", "lon", "plant_name",
#     "Plant ID", "Plant Name", "Technology", "Prime Mover Code",
#     "BusName", "kV",
# ]
# const UNMATCHED_COLS_THERMAL = [
#     "name", "bus_number", "bus", "generator_type", "prime_mover_type",
#     "fuel", "rating", "ts_column_name", "lat", "lon", "plant_name",
# ]
# const EI_THERMAL_REPORT_COLS = [
#     "name", "bus_number", "bus", "generator_type", "prime_mover_type",
#     "fuel", "rating", "lat", "lon", "plant_name",
#     "Plant ID", "Plant Name", "Technology", "Prime Mover Code",
#     "BusName", "kV", "eia_capacity_mw",
# ]

# # ── T1. Match EI → EIA2PF (thermal) ──────────────────────────────────────────
# function match_ei_to_eia_thermal(ei_gens::DataFrame, eia_expanded::DataFrame, state::String)
#     ei = copy(ei_gens)
#     ei.bus_number = [ismissing(x) ? missing : Int(x) for x in ei.bus_number]

#     eia_state = filter(row -> coalesce(row["State"] == state, false) &&
#                               coalesce(row["Prime Mover Code"] in THERMAL_EIA_PM_CODES, false),
#                        eia_expanded)
#     DataFrames.rename!(eia_state, "BusID_int" => "BusID_int_join")

#     println("\n$state: EIA Thermal rows after expansion: ", nrow(eia_state))
#     println("  Breakdown by Prime Mover Code:")
#     for pm in sort(unique(skipmissing(eia_state[!, "Prime Mover Code"])))
#         n = count(==(pm), skipmissing(eia_state[!, "Prime Mover Code"]))
#         n == 0 && continue
#         println("    $pm: $n")
#     end

#     result = leftjoin(ei, eia_state,
#                       on = "bus_number" => "BusID_int_join",
#                       matchmissing = :notequal)

#     matched   = filter(row -> !ismissing(row["Plant ID"]), result)
#     unmatched = filter(row ->  ismissing(row["Plant ID"]), result)

#     is_thermal(t) = coalesce(string(t), "") == "ThermalStandard"
#     n_thermal_total     = count(is_thermal, ei[!, "generator_type"])
#     n_thermal_matched   = count(is_thermal, matched[!, "generator_type"])
#     n_thermal_unmatched = count(is_thermal, unmatched[!, "generator_type"])

#     println("\n", "="^60)
#     println("$state EI Generators — EIA2PF Thermal Bus Match Summary")
#     println("="^60)
#     println("  Total $state EI generators:       ", nrow(ei))
#     println("  ✅ Matched to EIA2PF bus:         ", nrow(matched))
#     println("  ⚠️  No EIA2PF bus match:           ", nrow(unmatched))
#     println("─"^60)
#     println("  ThermalStandard generators in $state EI:")
#     println("    Total:        ", n_thermal_total)
#     println("    ✅ Matched:   ", n_thermal_matched)
#     println("    ⚠️  Unmatched: ", n_thermal_unmatched)
#     println("─"^60)
#     println("  Matched by prime_mover_type:")
#     thermal_matched = filter(row -> is_thermal(row["generator_type"]), matched)
#     for pm in sort(unique(skipmissing(thermal_matched[!, "prime_mover_type"])))
#         n = count(==(pm), skipmissing(thermal_matched[!, "prime_mover_type"]))
#         println("    $pm: $n")
#     end
#     println("="^60)

#     return result
# end

# # ── T2. Summarize thermal matched/unmatched ───────────────────────────────────
# function summarize_thermal(label::String, result::DataFrame)
#     is_thermal(t) = coalesce(string(t), "") == "ThermalStandard"

#     matched   = filter(row -> !ismissing(row["Plant ID"]) &&
#                                is_thermal(row["generator_type"]), result)
#     unmatched = filter(row ->  ismissing(row["Plant ID"]) &&
#                                is_thermal(row["generator_type"]), result)

#     total_mw_matched   = round(sum(Float64.(coalesce.(matched.rating,   0.0))), digits=1)
#     total_mw_unmatched = round(sum(Float64.(coalesce.(unmatched.rating, 0.0))), digits=1)

#     println("\n", "="^60)
#     println("$label — Thermal Matched vs Unmatched")
#     println("="^60)
#     println("  ✅ Matched:   ", nrow(matched),   " gens | $(total_mw_matched) MW")
#     println("  ⚠️  Unmatched: ", nrow(unmatched), " gens | $(total_mw_unmatched) MW")
#     println("─"^60)
#     println("  Matched by prime_mover_type:")
#     for pm in sort(unique(skipmissing(matched[!, "prime_mover_type"])))
#         n  = count(==(pm), skipmissing(matched[!, "prime_mover_type"]))
#         mw = round(sum(Float64.(coalesce.(
#                 filter(r -> coalesce(r["prime_mover_type"], "") == pm, matched).rating,
#                 0.0))), digits=1)
#         println("    $pm: $n gens | $(mw) MW")
#     end
#     println("─"^60)
#     println("  Matched by fuel:")
#     for fuel in sort(unique(skipmissing(matched[!, "fuel"])))
#         n  = count(==(fuel), skipmissing(matched[!, "fuel"]))
#         mw = round(sum(Float64.(coalesce.(
#                 filter(r -> coalesce(r["fuel"], "") == fuel, matched).rating,
#                 0.0))), digits=1)
#         println("    $fuel: $n gens | $(mw) MW")
#     end
#     println("="^60)

#     avail_cols_matched   = intersect(KEY_COLS_THERMAL,   names(matched))
#     avail_cols_unmatched = intersect(UNMATCHED_COLS_THERMAL, names(unmatched))

#     println("\n📋 $label matched Thermal generators (", nrow(matched), " rows):")
#     nrow(matched) > 0 ?
#         show(sort(DataFrames.select(matched, avail_cols_matched), "bus_number"), allrows=true) :
#         println("  (none)")

#     println("\n⚠️  $label unmatched Thermal generators (", nrow(unmatched), " rows):")
#     nrow(unmatched) > 0 ?
#         show(sort(DataFrames.select(unmatched, avail_cols_unmatched), "bus_number"), allrows=true) :
#         println("  (none)")

#     return matched, unmatched
# end

# # ── T2b. Misclassification audit: EI fuel/prime_mover vs EIA reference ────────
# # Checks matched ThermalStandard generators for:
# #   1. Wrong prime_mover_type (EI vs EIA Prime Mover Code)
# #   2. Wrong fuel (EI vs EIA Technology-derived fuel)
# #   3. Generator_type is NOT ThermalStandard but matched to a thermal EIA entry

# # ── T2b. Misclassification audit: EI fuel/prime_mover vs EIA reference ────────
# const EIA_PM_TO_EI_PM = Dict(
#     "CC" => "CC",
#     "CA" => "CC",   # CC steam part → still CC
#     "CS" => "CC",   # CC single shaft → still CC
#     "CT" => "CT",
#     "GT" => "GT",
#     "ST" => "ST",
#     "IC" => "IC",
#     "OT" => "OT",
#     "NB" => "ST",   # Nuclear boiling water → steam
#     "NP" => "ST",   # Nuclear pressurized water → steam
# )

# const EIA_TECH_TO_EI_FUEL = Dict(
#     # ── Natural Gas ───────────────────────────────────────────────────────────
#     "Natural Gas Fired Combined Cycle"            => "NATURAL_GAS",
#     "Natural Gas Fired Combustion Turbine"        => "NATURAL_GAS",
#     "Natural Gas Steam Turbine"                   => "NATURAL_GAS",
#     "Natural Gas Internal Combustion Engine"      => "NATURAL_GAS",
#     "Natural Gas with Compressed Air Storage"     => "NATURAL_GAS",
#     # ── Coal ─────────────────────────────────────────────────────────────────
#     "Coal Integrated Gasification Combined Cycle" => "COAL",
#     "Conventional Steam Coal"                     => "COAL",
#     # ── Oil ──────────────────────────────────────────────────────────────────
#     "Petroleum Liquids"                           => "DISTILLATE_FUEL_OIL",
#     "Petroleum Coke"                              => "DISTILLATE_FUEL_OIL",
#     # ── Nuclear ───────────────────────────────────────────────────────────────
#     "Nuclear"                                     => "NUCLEAR",
#     # ── Biomass / Biogas ──────────────────────────────────────────────────────
#     "Wood/Wood Waste Biomass"                     => "WOOD_WASTE_SOLIDS",
#     "Landfill Gas"                                => "BIOGAS",
#     "Municipal Solid Waste"                       => "BIOMASS",
#     "Other Waste Biomass"                         => "BIOMASS",
#     # ── Other ─────────────────────────────────────────────────────────────────
#     "Other Gases"                                 => "OTHER",
#     "Other"                                       => "OTHER",
#     # ── Hydro ─────────────────────────────────────────────────────────────────
#     "Hydroelectric Pumped Storage"                => "HYDRO",
#     "Conventional Hydroelectric"                  => "HYDRO",
# )

# function audit_thermal_classifications(label::String, result::DataFrame)

#     matched = filter(row -> !ismissing(row["Plant ID"]), result)
#     nrow(matched) == 0 && return DataFrame(), DataFrame(), DataFrame()

#     wrong_pm      = DataFrame()
#     wrong_fuel    = DataFrame()
#     wrong_gentype = DataFrame()

#     AUDIT_COLS = [
#         "name", "bus_number", "bus",
#         "generator_type", "prime_mover_type", "fuel",
#         "Plant ID", "Plant Name",
#         "Technology", "Prime Mover Code",
#         "rating", "eia_capacity_mw",
#     ]
#     avail = intersect(AUDIT_COLS, names(matched))

#     for row in eachrow(matched)
#         ei_gentype = coalesce(string(row["generator_type"]), "")
#         ei_pm      = coalesce(string(row["prime_mover_type"]), "")
#         ei_fuel    = coalesce(string(row["fuel"]), "")
#         eia_pm     = coalesce(string(row["Prime Mover Code"]), "")
#         eia_tech   = coalesce(string(row["Technology"]), "")

#         if ei_gentype != "ThermalStandard"
#             append!(wrong_gentype, DataFrame(row)[!, avail], promote=true)
#             continue
#         end

#         expected_pm = get(EIA_PM_TO_EI_PM, eia_pm, missing)
#         if !ismissing(expected_pm) && ei_pm != expected_pm
#             append!(wrong_pm, DataFrame(row)[!, avail], promote=true)
#         end

#         expected_fuel = get(EIA_TECH_TO_EI_FUEL, eia_tech, missing)
#         if !ismissing(expected_fuel) && ei_fuel != expected_fuel
#             append!(wrong_fuel, DataFrame(row)[!, avail], promote=true)
#         end
#     end

#     # ── Print report ──────────────────────────────────────────────────────────
#     println("\n", "█"^70)
#     println("  $label — THERMAL MISCLASSIFICATION AUDIT")
#     println("█"^70)

#     # ── Section A: wrong generator_type ──────────────────────────────────────
#     println("\n🔴 A. Matched to EIA thermal bus but generator_type ≠ ThermalStandard")
#     println("─"^70)
#     if nrow(wrong_gentype) == 0
#         println("   ✅ None found")
#     else
#         grp = combine(
#             groupby(wrong_gentype,
#                 intersect(["generator_type", "Prime Mover Code"], names(wrong_gentype))),
#             nrow => :count,
#             "rating" => (x -> round(sum(Float64.(coalesce.(x, 0.0))), digits=1)) => :total_mw,
#         )
#         println("   ⚠️  $(nrow(wrong_gentype)) generator(s):")
#         show(grp, allrows=true)
#         println()
#         show(sort(wrong_gentype, :bus_number), allrows=true)
#     end

#     # ── Section B: wrong prime_mover_type ────────────────────────────────────
#     println("\n🟠 B. ThermalStandard with wrong prime_mover_type vs EIA Prime Mover Code")
#     println("─"^70)
#     if nrow(wrong_pm) == 0
#         println("   ✅ None found")
#     else
#         # rename for display only — EI_prime_mover | EIA_prime_mover
#         grp_raw = combine(
#             groupby(wrong_pm,
#                 intersect(["prime_mover_type", "Prime Mover Code"], names(wrong_pm))),
#             nrow => :count,
#             "rating" => (x -> round(sum(Float64.(coalesce.(x, 0.0))), digits=1)) => :total_mw,
#         )
#         DataFrames.rename!(grp_raw,
#             "prime_mover_type" => "EI_prime_mover",
#             "Prime Mover Code" => "EIA_prime_mover",
#         )
#         println("   ⚠️  $(nrow(wrong_pm)) generator(s):")
#         println("   Summary:")
#         show(sort(grp_raw, :count, rev=true), allrows=true)
#         println("\n   Detail:")
#         show(sort(wrong_pm, :bus_number), allrows=true)
#     end

#     # ── Section C: wrong fuel ─────────────────────────────────────────────────
#     println("\n🟡 C. ThermalStandard with wrong fuel vs EIA Technology")
#     println("─"^70)
#     if nrow(wrong_fuel) == 0
#         println("   ✅ None found")
#     else
#         grp_raw = combine(
#             groupby(wrong_fuel,
#                 intersect(["fuel", "Technology"], names(wrong_fuel))),
#             nrow => :count,
#             "rating" => (x -> round(sum(Float64.(coalesce.(x, 0.0))), digits=1)) => :total_mw,
#         )
#         DataFrames.rename!(grp_raw,
#             "fuel"      => "EI_fuel",
#             "Technology" => "EIA_technology",
#         )
#         println("   ⚠️  $(nrow(wrong_fuel)) generator(s):")
#         println("   Summary:")
#         show(sort(grp_raw, :count, rev=true), allrows=true)
#         println("\n   Detail:")
#         show(sort(wrong_fuel, :bus_number), allrows=true)
#     end

#     # ── Section D: per-state summary ─────────────────────────────────────────
#     n_thermal = count(r -> coalesce(string(r["generator_type"]), "") == "ThermalStandard",
#                       eachrow(matched))
#     println("\n📊 D. $label Audit Summary")
#     println("─"^70)
#     println("   Total matched ThermalStandard:        $n_thermal")
#     println("   🔴 Wrong generator_type:              ", nrow(wrong_gentype))
#     println("   🟠 Wrong prime_mover_type:            ", nrow(wrong_pm))
#     println("   🟡 Wrong fuel:                        ", nrow(wrong_fuel))
#     println("   ─"^35)
#     println("   Total generators needing correction:  ",
#             nrow(wrong_gentype) + nrow(wrong_pm) + nrow(wrong_fuel))
#     println("█"^70)

#     return wrong_gentype, wrong_pm, wrong_fuel
# end


# # ── T3. MMWG fallback for unmatched EI thermal ───────────────────────────────
# const MMWG_COLS_THERMAL = [
#     "name", "bus_number", "bus", "generator_type", "prime_mover_type",
#     "fuel", "rating", "ts_column_name",
#     "Load Flow  Bus Name", "English Name", "EIA Plant Code", "Bus kV",
# ]

# function mmwg_lookup_thermal(label::String, unmatched::DataFrame, mmwg_slim::DataFrame)
#     result = leftjoin(unmatched, mmwg_slim,
#                       on = "bus_number" => "Bus Number",
#                       matchmissing = :notequal)

#     mmwg_matched   = filter(row -> !ismissing(row["EIA Plant Code"]), result)
#     mmwg_unmatched = filter(row ->  ismissing(row["EIA Plant Code"]), result)

#     println("\n", "="^60)
#     println("$label — Unmatched Thermal → MMWG Fallback")
#     println("="^60)
#     println("  ✅ Found in MMWG:    ", nrow(mmwg_matched))
#     println("  ⚠️  Still unmatched: ", nrow(mmwg_unmatched))
#     println("─"^60)

#     # ── Breakdown by prime mover ──────────────────────────────────────────────
#     if nrow(mmwg_matched) > 0
#         println("  MMWG matched by prime_mover_type:")
#         for pm in sort(unique(skipmissing(mmwg_matched[!, "prime_mover_type"])))
#             n = count(==(pm), skipmissing(mmwg_matched[!, "prime_mover_type"]))
#             println("    $pm: $n")
#         end
#     end
#     println("="^60)

#     avail_matched   = intersect(MMWG_COLS_THERMAL,        names(mmwg_matched))
#     avail_unmatched = intersect(UNMATCHED_COLS_THERMAL,   names(mmwg_unmatched))

#     println("\n📋 $label found in MMWG (", nrow(mmwg_matched), " rows):")
#     nrow(mmwg_matched) > 0 ?
#         show(sort(DataFrames.select(mmwg_matched, avail_matched), "bus_number"), allrows=true) :
#         println("  (none)")

#     println("\n⚠️  $label still unmatched after MMWG (", nrow(mmwg_unmatched), " rows):")
#     nrow(mmwg_unmatched) > 0 ?
#         show(sort(DataFrames.select(mmwg_unmatched, avail_unmatched), "bus_number"), allrows=true) :
#         println("  (none)")

#     return mmwg_matched, mmwg_unmatched
# end

# # ── T4. Run EI → EIA match ────────────────────────────────────────────────────
# va_ei_thermal_result = match_ei_to_eia_thermal(VA_ei_gens, eia_expanded, "VA")
# md_ei_thermal_result = match_ei_to_eia_thermal(MD_ei_gens, eia_expanded, "MD")
# wv_ei_thermal_result = match_ei_to_eia_thermal(WV_ei_gens, eia_expanded, "WV")

# va_thermal_matched, va_thermal_unmatched = summarize_thermal("VA", va_ei_thermal_result)
# md_thermal_matched, md_thermal_unmatched = summarize_thermal("MD", md_ei_thermal_result)
# wv_thermal_matched, wv_thermal_unmatched = summarize_thermal("WV", wv_ei_thermal_result)

# va_thermal_mmwg_matched, va_thermal_still_unmatched = mmwg_lookup_thermal("VA", va_thermal_unmatched, mmwg_slim)
# md_thermal_mmwg_matched, md_thermal_still_unmatched = mmwg_lookup_thermal("MD", md_thermal_unmatched, mmwg_slim)
# wv_thermal_mmwg_matched, wv_thermal_still_unmatched = mmwg_lookup_thermal("WV", wv_thermal_unmatched, mmwg_slim)

# println("\n", "="^60)
# println("📊 Final Thermal EI Summary by State")
# println("="^60)
# for (label, matched, mmwg, unmatched) in [
#     ("VA", va_thermal_matched, va_thermal_mmwg_matched, va_thermal_still_unmatched),
#     ("MD", md_thermal_matched, md_thermal_mmwg_matched, md_thermal_still_unmatched),
#     ("WV", wv_thermal_matched, wv_thermal_mmwg_matched, wv_thermal_still_unmatched),
# ]
#     matched_mw   = round(sum(Float64.(coalesce.(matched.rating,   0.0))), digits=1)
#     mmwg_mw      = round(sum(Float64.(coalesce.(mmwg.rating,      0.0))), digits=1)
#     unmatched_mw = round(sum(Float64.(coalesce.(unmatched.rating, 0.0))), digits=1)
#     println("\n  $label:")
#     println("    ✅ EIA2PF matched:   ", nrow(matched),   " gens | $(matched_mw) MW")
#     println("    ✅ MMWG matched:     ", nrow(mmwg),      " gens | $(mmwg_mw) MW")
#     println("    ⚠️  Still unmatched: ", nrow(unmatched), " gens | $(unmatched_mw) MW")
# end
# println("="^60)

# # ── T4b. Run misclassification audit (after summarize_thermal calls) ──────────
# va_wrong_gentype, va_wrong_pm, va_wrong_fuel =
#     audit_thermal_classifications("VA", va_ei_thermal_result)
# md_wrong_gentype, md_wrong_pm, md_wrong_fuel =
#     audit_thermal_classifications("MD", md_ei_thermal_result)
# wv_wrong_gentype, wv_wrong_pm, wv_wrong_fuel =
#     audit_thermal_classifications("WV", wv_ei_thermal_result)

# # ── T4c. Cross-state rollup ───────────────────────────────────────────────────
# println("\n", "█"^70)
# println("  THERMAL MISCLASSIFICATION ROLLUP — VA + MD + WV")
# println("█"^70)
# for (issue, va_df, md_df, wv_df) in [
#     ("🔴 Wrong generator_type", va_wrong_gentype, md_wrong_gentype, wv_wrong_gentype),
#     ("🟠 Wrong prime_mover",    va_wrong_pm,      md_wrong_pm,      wv_wrong_pm),
#     ("🟡 Wrong fuel",           va_wrong_fuel,    md_wrong_fuel,    wv_wrong_fuel),
# ]
#     all_df = vcat(va_df, md_df, wv_df, cols=:union)
#     total  = nrow(all_df)

#     println("\n  $issue:")
#     println("    VA: $(nrow(va_df))  |  MD: $(nrow(md_df))  |  WV: $(nrow(wv_df))",
#             "  →  Total: $total gens")

#     if total == 0
#         println("    ✅ None found across all states")
#         continue
#     end

#     # ── MW total only if rating column exists and has rows ───────────────────
#     mw = "rating" in names(all_df) ?
#         round(sum(Float64.(coalesce.(all_df[!, "rating"], 0.0))), digits=1) : 0.0
#     println("    Total capacity: $(mw) MW")

#     # ── Per-issue grouped detail ──────────────────────────────────────────────
#     if issue == "🟠 Wrong prime_mover" && "expected_prime_mover" in names(all_df)
#         grp_cols = intersect(["prime_mover_type", "Prime Mover Code",
#                               "expected_prime_mover"], names(all_df))
#         if length(grp_cols) == 3
#             grp = combine(
#                 groupby(all_df, grp_cols),
#                 nrow => :count,
#                 "rating" => (x -> round(sum(Float64.(coalesce.(x, 0.0))), digits=1)) => :total_mw,
#             )
#             println("    Mismatch pairs (across all states):")
#             show(grp, allrows=true)
#         end
#     end

#     if issue == "🟡 Wrong fuel" && "expected_fuel" in names(all_df)
#         grp_cols = intersect(["fuel", "Technology", "expected_fuel"], names(all_df))
#         if length(grp_cols) == 3
#             grp = combine(
#                 groupby(all_df, grp_cols),
#                 nrow => :count,
#                 "rating" => (x -> round(sum(Float64.(coalesce.(x, 0.0))), digits=1)) => :total_mw,
#             )
#             println("    Mismatch pairs (across all states):")
#             show(grp, allrows=true)
#         end
#     end

#     if issue == "🔴 Wrong generator_type" && "generator_type" in names(all_df)
#         grp_cols = intersect(["generator_type", "Prime Mover Code"], names(all_df))
#         if length(grp_cols) == 2
#             grp = combine(
#                 groupby(all_df, grp_cols),
#                 nrow => :count,
#                 "rating" => (x -> round(sum(Float64.(coalesce.(x, 0.0))), digits=1)) => :total_mw,
#             )
#             println("    Types found where ThermalStandard expected:")
#             show(grp, allrows=true)
#         end
#     end
# end
# println("█"^70)

# # ── T4c. Cross-state rollup ───────────────────────────────────────────────────
# println("\n", "█"^70)
# println("  THERMAL MISCLASSIFICATION ROLLUP — VA + MD + WV")
# println("█"^70)
# for (issue, va_df, md_df, wv_df) in [
#     ("🔴 Wrong generator_type", va_wrong_gentype, md_wrong_gentype, wv_wrong_gentype),
#     ("🟠 Wrong prime_mover",    va_wrong_pm,      md_wrong_pm,      wv_wrong_pm),
#     ("🟡 Wrong fuel",           va_wrong_fuel,    md_wrong_fuel,    wv_wrong_fuel),
# ]
#     all_df = vcat(va_df, md_df, wv_df, cols=:union)
#     total  = nrow(all_df)

#     println("\n  $issue:")
#     println("    VA: $(nrow(va_df))  |  MD: $(nrow(md_df))  |  WV: $(nrow(wv_df))",
#             "  →  Total: $total gens")

#     if total == 0
#         println("    ✅ None found across all states")
#         continue
#     end

#     mw = "rating" in names(all_df) ?
#         round(sum(Float64.(coalesce.(all_df[!, "rating"], 0.0))), digits=1) : 0.0
#     println("    Total capacity: $(mw) MW")

#     # ── Helper: grouped table for each issue type ─────────────────────────────
#     function misclass_grp(df::DataFrame, issue::String)
#         nrow(df) == 0 && return nothing
#         if issue == "🟠 Wrong prime_mover"
#             grp_cols = intersect(["prime_mover_type", "Prime Mover Code"], names(df))
#             length(grp_cols) < 2 && return nothing
#             grp = combine(
#                 groupby(df, grp_cols),
#                 nrow => :count,
#                 "rating" => (x -> round(sum(Float64.(coalesce.(x, 0.0))), digits=1)) => :total_mw,
#             )
#             DataFrames.rename!(grp,
#                 "prime_mover_type" => "EI_prime_mover",
#                 "Prime Mover Code" => "EIA_prime_mover",
#             )
#             return sort(grp, :count, rev=true)

#         elseif issue == "🟡 Wrong fuel"
#             grp_cols = intersect(["fuel", "Technology"], names(df))
#             length(grp_cols) < 2 && return nothing
#             grp = combine(
#                 groupby(df, grp_cols),
#                 nrow => :count,
#                 "rating" => (x -> round(sum(Float64.(coalesce.(x, 0.0))), digits=1)) => :total_mw,
#             )
#             DataFrames.rename!(grp,
#                 "fuel"       => "EI_fuel",
#                 "Technology" => "EIA_technology",
#             )
#             return sort(grp, :count, rev=true)

#         elseif issue == "🔴 Wrong generator_type"
#             grp_cols = intersect(["generator_type", "Prime Mover Code"], names(df))
#             length(grp_cols) < 2 && return nothing
#             grp = combine(
#                 groupby(df, grp_cols),
#                 nrow => :count,
#                 "rating" => (x -> round(sum(Float64.(coalesce.(x, 0.0))), digits=1)) => :total_mw,
#             )
#             return sort(grp, :count, rev=true)
#         end
#         return nothing
#     end

#     # ── All-states combined table ─────────────────────────────────────────────
#     println("\n    📊 Combined (all states):")
#     grp_all = misclass_grp(all_df, issue)
#     grp_all !== nothing && show(grp_all, allrows=true)

#     # ── Per-state breakdown ───────────────────────────────────────────────────
#     for (state_label, state_df) in [("VA", va_df), ("MD", md_df), ("WV", wv_df)]
#         nrow(state_df) == 0 && continue
#         state_mw = "rating" in names(state_df) ?
#             round(sum(Float64.(coalesce.(state_df[!, "rating"], 0.0))), digits=1) : 0.0
#         println("\n    📍 $state_label  ($(nrow(state_df)) gens | $(state_mw) MW):")
#         grp_state = misclass_grp(state_df, issue)
#         grp_state !== nothing && show(grp_state, allrows=true)
#     end
# end
# println("█"^70)

# # ── T5. Write EI thermal reports ─────────────────────────────────────────────
# # ei_thermal_VA = write_ei_thermal_report("VA", va_thermal_matched, va_thermal_unmatched, OUTPUT_DIR)
# # ei_thermal_MD = write_ei_thermal_report("MD", md_thermal_matched, md_thermal_unmatched, OUTPUT_DIR)
# # ei_thermal_WV = write_ei_thermal_report("WV", wv_thermal_matched, wv_thermal_unmatched, OUTPUT_DIR)

# # ── T6. Find EIA thermal plants NOT in EI ────────────────────────────────────
# function find_eia_thermal_not_in_ei(label::String, ei_thermal_result::DataFrame,
#                                      eia_expanded::DataFrame, state::String)

#     matched_uids = Set{String}()
#     for row in eachrow(ei_thermal_result)
#         ismissing(row["Plant ID"]) && continue
#         ismissing(row["Generator ID"]) && continue
#         push!(matched_uids, string(row["Plant ID"]) * "_" * string(row["Generator ID"]))
#     end

#     eia_state_thermal = filter(row -> coalesce(row["State"] == state, false) &&
#                                       coalesce(row["Prime Mover Code"] in THERMAL_EIA_PM_CODES, false),
#                                eia_expanded)

#     eia_state_thermal[!, "uid"] = [
#         (ismissing(r["Plant ID"]) || ismissing(r["Generator ID"])) ? missing :
#         string(r["Plant ID"]) * "_" * string(r["Generator ID"])
#         for r in eachrow(eia_state_thermal)
#     ]

#     eia_unmatched = filter(row -> ismissing(row["uid"]) ||
#                                   !in(row["uid"], matched_uids), eia_state_thermal)

#     has_both    = filter(row -> !ismissing(row["BusID_int"]) &&
#                                 !ismissing(row["BusName"]) &&
#                                 row["BusName"] != "", eia_unmatched)
#     has_neither = filter(row ->  ismissing(row["BusID_int"]) &&
#                                 (ismissing(row["BusName"]) || row["BusName"] == ""),
#                          eia_unmatched)

#     println("\n", "="^60)
#     println("$label — EIA Thermal Plants NOT in EI model")
#     println("="^60)
#     println("  Total EIA Thermal entries for $label:  ", nrow(eia_state_thermal))
#     println("  ⚠️  Not matched to any EI generator:   ", nrow(eia_unmatched))
#     println("─"^60)
#     println("    ✅ Has BusID + BusName:  ", nrow(has_both))
#     println("    ❌ Has neither:          ", nrow(has_neither))
#     println("─"^60)
#     println("  Unmatched by Prime Mover Code:")
#     for pm in sort(unique(skipmissing(eia_unmatched[!, "Prime Mover Code"])))
#         n = count(==(pm), skipmissing(eia_unmatched[!, "Prime Mover Code"]))
#         println("    $pm: $n")
#     end
#     println("="^60)

#     EIA_COLS = ["Plant ID", "Plant Name", "Generator ID",
#                 "BusID_int", "BusName", "kV",
#                 "eia_capacity_mw", "Technology", "Prime Mover Code",
#                 "eia_lat", "eia_lon"]

#     println("\n  ✅ Has BusID + BusName (", nrow(has_both), " rows):")
#     nrow(has_both) > 0 && show(DataFrames.select(has_both,
#         intersect(EIA_COLS, names(has_both))), allrows=true)

#     println("\n  ❌ Has neither (", nrow(has_neither), " rows):")
#     nrow(has_neither) > 0 && show(DataFrames.select(has_neither,
#         intersect(EIA_COLS, names(has_neither))), allrows=true)

#     return eia_unmatched, has_both, has_neither
# end

# va_thermal_eia_unmatched, va_thermal_eia_has_both, va_thermal_eia_neither =
#     find_eia_thermal_not_in_ei("VA", va_ei_thermal_result, eia_expanded, "VA")
# md_thermal_eia_unmatched, md_thermal_eia_has_both, md_thermal_eia_neither =
#     find_eia_thermal_not_in_ei("MD", md_ei_thermal_result, eia_expanded, "MD")
# wv_thermal_eia_unmatched, wv_thermal_eia_has_both, wv_thermal_eia_neither =
#     find_eia_thermal_not_in_ei("WV", wv_ei_thermal_result, eia_expanded, "WV")

# # ── T7. Exact name match → MMWG ──────────────────────────────────────────────
# function match_neither_to_mmwg_thermal(label::String, neither::DataFrame,
#                                         mmwg_slim::DataFrame)
#     println("\n", "="^60)
#     println("$label — EIA Thermal 'neither' plants → MMWG exact name match")
#     println("="^60)

#     nrow(neither) == 0 && (println("  ℹ️  No 'neither' rows to match.");
#                            println("="^60);
#                            return DataFrame(), DataFrame())

#     region      = get(STATE_REGION, label, missing)
#     mmwg_region = !ismissing(region) ?
#         filter(r -> coalesce(r["Region/PC"] == region, false), mmwg_slim) : mmwg_slim

#     normalize(s) = ismissing(s) ? "" : lowercase(strip(string(s)))

#     matches  = DataFrame()
#     no_match = DataFrame()

#     for row in eachrow(neither)
#         plant_name = normalize(row["Plant Name"])
#         mmwg_match = filter(r -> normalize(r["English Name"]) == plant_name, mmwg_region)
#         if nrow(mmwg_match) > 0
#             new_row = copy(DataFrame(row))
#             new_row[!, "Bus Number"]          .= mmwg_match[1, "Bus Number"]
#             new_row[!, "Load Flow  Bus Name"] .= mmwg_match[1, "Load Flow  Bus Name"]
#             new_row[!, "English Name"]        .= mmwg_match[1, "English Name"]
#             new_row[!, "EIA Plant Code"]      .= mmwg_match[1, "EIA Plant Code"]
#             new_row[!, "Bus kV"]              .= mmwg_match[1, "Bus kV"]
#             new_row[!, "Region/PC"]           .= mmwg_match[1, "Region/PC"]
#             append!(matches, new_row, promote=true)
#         else
#             append!(no_match, DataFrame(row), promote=true)
#         end
#     end

#     println("  Total 'neither':      ", nrow(neither))
#     println("  ✅ Matched via name:  ", nrow(matches))
#     println("  ⚠️  Still no match:   ", nrow(no_match))
#     println("="^60)

#     return matches, no_match
# end

# va_thermal_neither_mmwg, va_thermal_neither_unmatched =
#     match_neither_to_mmwg_thermal("VA", va_thermal_eia_neither, mmwg_slim)
# md_thermal_neither_mmwg, md_thermal_neither_unmatched =
#     match_neither_to_mmwg_thermal("MD", md_thermal_eia_neither, mmwg_slim)
# wv_thermal_neither_mmwg, wv_thermal_neither_unmatched =
#     match_neither_to_mmwg_thermal("WV", wv_thermal_eia_neither, mmwg_slim)

# # ── T8. Fuzzy match → MMWG ───────────────────────────────────────────────────
# function fuzzy_match_to_mmwg_thermal(label::String, still_unmatched::DataFrame,
#                                       mmwg_slim::DataFrame; threshold::Float64 = 0.7)
#     println("\n", "="^60)
#     println("$label — Fuzzy Thermal match → MMWG (threshold = $threshold)")
#     println("="^60)

#     nrow(still_unmatched) == 0 && (println("  ℹ️  Nothing to fuzzy match.");
#                                    println("="^60);
#                                    return DataFrame(), DataFrame())

#     region      = get(STATE_REGION, label, missing)
#     mmwg_region = !ismissing(region) ?
#         filter(r -> coalesce(r["Region/PC"] == region, false), mmwg_slim) : mmwg_slim

#     normalize(s) = ismissing(s) ? "" : lowercase(strip(string(s)))
#     mmwg_names    = [normalize(r["English Name"]) for r in eachrow(mmwg_region)]
#     mmwg_bus_nums = mmwg_region[!, "Bus Number"]
#     mmwg_bus_kv   = mmwg_region[!, "Bus kV"]
#     mmwg_lf_names = mmwg_region[!, "Load Flow  Bus Name"]
#     mmwg_eia_code = mmwg_region[!, "EIA Plant Code"]

#     fuzzy_matched   = DataFrame()
#     fuzzy_unmatched = DataFrame()

#     for row in eachrow(still_unmatched)
#         plant_name = normalize(row["Plant Name"])
#         scores     = [compare(plant_name, mn, Jaro()) for mn in mmwg_names]
#         best_idx   = argmax(scores)
#         best_score = scores[best_idx]

#         new_row = copy(DataFrame(row))
#         if best_score >= threshold
#             new_row[!, "Bus Number"]          .= mmwg_bus_nums[best_idx]
#             new_row[!, "Load Flow  Bus Name"] .= mmwg_lf_names[best_idx]
#             new_row[!, "English Name"]        .= mmwg_region[best_idx, "English Name"]
#             new_row[!, "EIA Plant Code"]      .= mmwg_eia_code[best_idx]
#             new_row[!, "Bus kV"]              .= mmwg_bus_kv[best_idx]
#             new_row[!, "match_score"]         .= round(best_score, digits=3)
#             new_row[!, "Region/PC"]           .= mmwg_region[best_idx, "Region/PC"]
#             append!(fuzzy_matched, new_row, promote=true)
#         else
#             new_row[!, "best_mmwg_name"] .= mmwg_region[best_idx, "English Name"]
#             new_row[!, "match_score"]    .= round(best_score, digits=3)
#             append!(fuzzy_unmatched, new_row, promote=true)
#         end
#     end

#     println("  Total still unmatched:          ", nrow(still_unmatched))
#     println("  ✅ Fuzzy matched (≥$threshold):  ", nrow(fuzzy_matched))
#     println("  ⚠️  No fuzzy match:               ", nrow(fuzzy_unmatched))

#     if nrow(fuzzy_matched) > 0
#         println("\n  Fuzzy match details:")
#         show(DataFrames.select(fuzzy_matched,
#             intersect(["Plant Name", "Bus Number", "English Name",
#                        "match_score", "eia_capacity_mw", "Prime Mover Code"],
#                       names(fuzzy_matched))), allrows=true)
#     end
#     if nrow(fuzzy_unmatched) > 0
#         println("\n  ⚠️  No match found (best candidate shown):")
#         show(DataFrames.select(fuzzy_unmatched,
#             intersect(["Plant Name", "best_mmwg_name", "match_score",
#                        "eia_capacity_mw", "Prime Mover Code"],
#                       names(fuzzy_unmatched))), allrows=true)
#     end
#     println("="^60)

#     return fuzzy_matched, fuzzy_unmatched
# end

# va_thermal_fuzzy_matched, va_thermal_fuzzy_unmatched =
#     fuzzy_match_to_mmwg_thermal("VA", va_thermal_neither_unmatched, mmwg_slim)
# md_thermal_fuzzy_matched, md_thermal_fuzzy_unmatched =
#     fuzzy_match_to_mmwg_thermal("MD", md_thermal_neither_unmatched, mmwg_slim)
# wv_thermal_fuzzy_matched, wv_thermal_fuzzy_unmatched =
#     fuzzy_match_to_mmwg_thermal("WV", wv_thermal_neither_unmatched, mmwg_slim)

# # Merge location-assigned back into fuzzy_matched (reuses auto_assign_by_location)
# va_thermal_unmatched_located, va_thermal_unmatched_no_coords =
#     auto_assign_by_location("VA thermal unmatched", va_thermal_fuzzy_unmatched,
#                             eia_plants_loc, bus_coords, max_cap_mw=Inf)
# md_thermal_unmatched_located, md_thermal_unmatched_no_coords =
#     auto_assign_by_location("MD thermal unmatched", md_thermal_fuzzy_unmatched,
#                             eia_plants_loc, bus_coords, max_cap_mw=Inf)
# wv_thermal_unmatched_located, wv_thermal_unmatched_no_coords =
#     auto_assign_by_location("WV thermal unmatched", wv_thermal_fuzzy_unmatched,
#                             eia_plants_loc, bus_coords, max_cap_mw=Inf)

# va_thermal_fuzzy_matched = vcat(va_thermal_fuzzy_matched, va_thermal_unmatched_located, cols=:union)
# md_thermal_fuzzy_matched = vcat(md_thermal_fuzzy_matched, md_thermal_unmatched_located, cols=:union)
# wv_thermal_fuzzy_matched = vcat(wv_thermal_fuzzy_matched, wv_thermal_unmatched_located, cols=:union)

# println("\n", "="^60)
# println("📊 Final Thermal EIA-not-in-EI Summary")
# println("="^60)
# for (label, has_both, neither_mmwg, fuzzy, loc, no_coords) in [
#     ("VA", va_thermal_eia_has_both, va_thermal_neither_mmwg,
#            va_thermal_fuzzy_matched,
#            va_thermal_unmatched_located, va_thermal_unmatched_no_coords),
#     ("MD", md_thermal_eia_has_both, md_thermal_neither_mmwg,
#            md_thermal_fuzzy_matched,
#            md_thermal_unmatched_located, md_thermal_unmatched_no_coords),
#     ("WV", wv_thermal_eia_has_both, wv_thermal_neither_mmwg,
#            wv_thermal_fuzzy_matched,
#            wv_thermal_unmatched_located, wv_thermal_unmatched_no_coords),
# ]
#     total_assigned = nrow(has_both) + nrow(neither_mmwg) + nrow(fuzzy)
#     println("\n  $label:")
#     println("    EIA_ONLY (has BusID+BusName):   ", nrow(has_both))
#     println("    EIA_MMWG_EXACT:                 ", nrow(neither_mmwg))
#     println("    EIA_MMWG_FUZZY (post-override):  ", nrow(fuzzy))
#     println("    ─── of which location-assigned:  ", nrow(loc))
#     println("    ❌ Still no bus (no coords):     ", nrow(no_coords))
#     println("    ──────────────────────────────────")
#     println("    Total assigned:                 ", total_assigned)
# end
# println("="^60)

# # ── T9. Build EI thermal DataFrames ──────────────────────────────────────────
# function build_state_thermal_df(label::String,
#                                  thermal_matched::DataFrame,
#                                  mmwg_matched::DataFrame)
#     s1 = DataFrame(
#         source          = fill("EI_EIA2PF", nrow(thermal_matched)),
#         gen_name        = thermal_matched.name,
#         ts_column_name  = thermal_matched.ts_column_name,
#         prime_mover     = thermal_matched.prime_mover_type,
#         fuel            = thermal_matched.fuel,
#         ei_lat          = thermal_matched.lat,
#         ei_lon          = thermal_matched.lon,
#         eia_lat         = thermal_matched[!, "eia_lat"],
#         eia_lon         = thermal_matched[!, "eia_lon"],
#         bus_id          = thermal_matched.bus_number,
#         bus_name        = thermal_matched.bus,
#         ei_capacity_mw  = thermal_matched.rating,
#         eia_capacity_mw = thermal_matched.eia_capacity_mw,
#         bus_voltage_kv  = [clean_bus_voltage(x) for x in thermal_matched.kV],
#     )
#     s2 = DataFrame(
#         source          = fill("EI_MMWG", nrow(mmwg_matched)),
#         gen_name        = mmwg_matched.name,
#         ts_column_name  = mmwg_matched.ts_column_name,
#         prime_mover     = mmwg_matched.prime_mover_type,
#         fuel            = mmwg_matched.fuel,
#         ei_lat          = mmwg_matched.lat,
#         ei_lon          = mmwg_matched.lon,
#         eia_lat         = fill(missing, nrow(mmwg_matched)),
#         eia_lon         = fill(missing, nrow(mmwg_matched)),
#         bus_id          = mmwg_matched.bus_number,
#         bus_name        = mmwg_matched.bus,
#         ei_capacity_mw  = mmwg_matched.rating,
#         eia_capacity_mw = fill(missing, nrow(mmwg_matched)),
#         bus_voltage_kv  = [clean_bus_voltage(x) for x in mmwg_matched[!, "Bus kV"]],
#     )
#     final_df = vcat(s1, s2, cols=:union)
#     final_df[!, "state"] .= label

#     println("\n", "="^60)
#     println("$label — Final Thermal DataFrame (EI generators)")
#     println("="^60)
#     println("  EI_EIA2PF: ", nrow(s1),
#             "  |  EI_MMWG: ", nrow(s2),
#             "  |  Total: ", nrow(final_df))
#     println("─"^60)
#     println("  By prime mover:")
#     for pm in sort(unique(skipmissing(final_df.prime_mover)))
#         n  = count(==(pm), skipmissing(final_df.prime_mover))
#         mw = round(sum(Float64.(coalesce.(
#                 filter(r -> coalesce(r.prime_mover, "") == pm, final_df).ei_capacity_mw,
#                 0.0))), digits=1)
#         println("    $pm: $n gens | $(mw) MW")
#     end
#     println("="^60)
#     show(sort(final_df, :bus_id), allrows=true)
#     return final_df
# end

# va_thermal = build_state_thermal_df("VA", va_thermal_matched, va_thermal_mmwg_matched)
# md_thermal = build_state_thermal_df("MD", md_thermal_matched, md_thermal_mmwg_matched)
# wv_thermal = build_state_thermal_df("WV", wv_thermal_matched, wv_thermal_mmwg_matched)

# # ── T10. Build EIA-only thermal DataFrames ────────────────────────────────────
# function build_eia_only_thermal_df(label::String,
#                                     eia_has_both::DataFrame,
#                                     neither_mmwg_matched::DataFrame,
#                                     fuzzy_matched::DataFrame)

#     safe_col(df, col) = nrow(df) == 0 || !(col in names(df)) ?
#         fill(missing, nrow(df)) : df[!, col]

#     s3 = if nrow(eia_has_both) == 0
#         println("  ℹ️  $label: no EIA_ONLY thermal entries")
#         DataFrame(source=String[], gen_name=String[], prime_mover=Any[],
#                   fuel=Any[], ts_column_name=Any[], ei_lat=Any[], ei_lon=Any[],
#                   eia_lat=Any[], eia_lon=Any[], bus_id=Any[], bus_name=Any[],
#                   ei_capacity_mw=Any[], eia_capacity_mw=Any[], bus_voltage_kv=Any[])
#     else
#         DataFrame(
#             source          = fill("EIA_ONLY", nrow(eia_has_both)),
#             gen_name        = [make_gen_name(eia_has_both[i, "BusID_int"], i)
#                                for i in 1:nrow(eia_has_both)],
#             prime_mover     = safe_col(eia_has_both, "Prime Mover Code"),
#             fuel            = fill(missing, nrow(eia_has_both)),
#             ts_column_name  = fill(missing, nrow(eia_has_both)),
#             ei_lat          = fill(missing, nrow(eia_has_both)),
#             ei_lon          = fill(missing, nrow(eia_has_both)),
#             eia_lat         = safe_col(eia_has_both, "eia_lat"),
#             eia_lon         = safe_col(eia_has_both, "eia_lon"),
#             bus_id          = safe_col(eia_has_both, "BusID_int"),
#             bus_name        = [clean_bus_name(x)
#                                for x in safe_col(eia_has_both, "BusName")],
#             ei_capacity_mw  = fill(missing, nrow(eia_has_both)),
#             eia_capacity_mw = safe_col(eia_has_both, "eia_capacity_mw"),
#             bus_voltage_kv  = [clean_bus_voltage(x)
#                                for x in safe_col(eia_has_both, "kV")],
#         )
#     end

#     s4 = if nrow(neither_mmwg_matched) == 0
#         println("  ℹ️  $label: no EIA_MMWG_EXACT thermal entries")
#         DataFrame(source=String[], gen_name=String[], prime_mover=Any[],
#                   fuel=Any[], ts_column_name=Any[], ei_lat=Any[], ei_lon=Any[],
#                   eia_lat=Any[], eia_lon=Any[], bus_id=Any[], bus_name=Any[],
#                   ei_capacity_mw=Any[], eia_capacity_mw=Any[], bus_voltage_kv=Any[])
#     else
#         DataFrame(
#             source          = fill("EIA_MMWG_EXACT", nrow(neither_mmwg_matched)),
#             gen_name        = [make_gen_name(neither_mmwg_matched[i, "Bus Number"], i)
#                                for i in 1:nrow(neither_mmwg_matched)],
#             prime_mover     = safe_col(neither_mmwg_matched, "Prime Mover Code"),
#             fuel            = fill(missing, nrow(neither_mmwg_matched)),
#             ts_column_name  = fill(missing, nrow(neither_mmwg_matched)),
#             ei_lat          = fill(missing, nrow(neither_mmwg_matched)),
#             ei_lon          = fill(missing, nrow(neither_mmwg_matched)),
#             eia_lat         = safe_col(neither_mmwg_matched, "eia_lat"),
#             eia_lon         = safe_col(neither_mmwg_matched, "eia_lon"),
#             bus_id          = safe_col(neither_mmwg_matched, "Bus Number"),
#             bus_name        = safe_col(neither_mmwg_matched, "Load Flow  Bus Name"),
#             ei_capacity_mw  = fill(missing, nrow(neither_mmwg_matched)),
#             eia_capacity_mw = safe_col(neither_mmwg_matched, "eia_capacity_mw"),
#             bus_voltage_kv  = [clean_bus_voltage(x)
#                                for x in safe_col(neither_mmwg_matched, "Bus kV")],
#         )
#     end

#     s5 = if nrow(fuzzy_matched) == 0
#         println("  ℹ️  $label: no EIA_MMWG_FUZZY thermal entries")
#         DataFrame(source=String[], gen_name=String[], prime_mover=Any[],
#                   fuel=Any[], ts_column_name=Any[], ei_lat=Any[], ei_lon=Any[],
#                   eia_lat=Any[], eia_lon=Any[], bus_id=Any[], bus_name=Any[],
#                   ei_capacity_mw=Any[], eia_capacity_mw=Any[], bus_voltage_kv=Any[])
#     else
#         DataFrame(
#             source          = fill("EIA_MMWG_FUZZY", nrow(fuzzy_matched)),
#             gen_name        = [make_gen_name(fuzzy_matched[i, "Bus Number"], i)
#                                for i in 1:nrow(fuzzy_matched)],
#             prime_mover     = safe_col(fuzzy_matched, "Prime Mover Code"),
#             fuel            = fill(missing, nrow(fuzzy_matched)),
#             ts_column_name  = fill(missing, nrow(fuzzy_matched)),
#             ei_lat          = fill(missing, nrow(fuzzy_matched)),
#             ei_lon          = fill(missing, nrow(fuzzy_matched)),
#             eia_lat         = safe_col(fuzzy_matched, "eia_lat"),
#             eia_lon         = safe_col(fuzzy_matched, "eia_lon"),
#             bus_id          = safe_col(fuzzy_matched, "Bus Number"),
#             bus_name        = safe_col(fuzzy_matched, "English Name"),
#             ei_capacity_mw  = fill(missing, nrow(fuzzy_matched)),
#             eia_capacity_mw = safe_col(fuzzy_matched, "eia_capacity_mw"),
#             bus_voltage_kv  = [clean_bus_voltage(x)
#                                for x in safe_col(fuzzy_matched, "Bus kV")],
#         )
#     end

#     final_df = vcat(s3, s4, s5, cols=:union)
#     final_df[!, "state"] .= label

#     if nrow(final_df) > 0
#         source_order = Dict("EIA_ONLY" => 1, "EIA_MMWG_EXACT" => 2, "EIA_MMWG_FUZZY" => 3)
#         final_df[!, "source_order"] = [source_order[s] for s in final_df.source]
#         sort!(final_df, [:source_order, :bus_id])
#         select!(final_df, Not(:source_order))
#     end

#     println("\n", "="^60)
#     println("$label — EIA-only Thermal DataFrame")
#     println("="^60)
#     println("  EIA_ONLY: $(nrow(s3))  |  EIA_MMWG_EXACT: $(nrow(s4))  |",
#             "  EIA_MMWG_FUZZY: $(nrow(s5))  |  Total: $(nrow(final_df))")
#     println("="^60)
#     nrow(final_df) > 0 && show(final_df, allrows=true)

#     return final_df
# end

# va_thermal_eia_only = build_eia_only_thermal_df("VA", va_thermal_eia_has_both,
#     va_thermal_neither_mmwg, va_thermal_fuzzy_matched)
# md_thermal_eia_only = build_eia_only_thermal_df("MD", md_thermal_eia_has_both,
#     md_thermal_neither_mmwg, md_thermal_fuzzy_matched)
# wv_thermal_eia_only = build_eia_only_thermal_df("WV", wv_thermal_eia_has_both,
#     wv_thermal_neither_mmwg, wv_thermal_fuzzy_matched)

# # ── T11. Build export DataFrames ──────────────────────────────────────────────
# function build_thermal_export_df(ei_df::DataFrame, eia_only_df::DataFrame, label::String)
#     ei_out = if nrow(ei_df) == 0
#         DataFrame(gen_name=String[], bus_id=Int[], bus_name=String[],
#                   prime_mover=Any[], fuel=Any[], lat=Float64[], lon=Float64[],
#                   capacity_mw=Any[], bus_voltage_kv=Float64[],
#                   source=String[], state=String[])
#     else
#         DataFrame(
#             gen_name       = ei_df[!, "gen_name"],
#             bus_id         = ei_df[!, "bus_id"],
#             bus_name       = ei_df[!, "bus_name"],
#             prime_mover    = ei_df[!, "prime_mover"],
#             fuel           = ei_df[!, "fuel"],
#             lat            = coalesce.(ei_df[!, "eia_lat"], ei_df[!, "ei_lat"]),
#             lon            = coalesce.(ei_df[!, "eia_lon"], ei_df[!, "ei_lon"]),
#             capacity_mw    = coalesce.(ei_df[!, "eia_capacity_mw"],
#                                        ei_df[!, "ei_capacity_mw"]),
#             bus_voltage_kv = ei_df[!, "bus_voltage_kv"],
#             source         = ei_df[!, "source"],
#             state          = fill(label, nrow(ei_df)),
#         )
#     end

#     eia_out = if nrow(eia_only_df) == 0
#         DataFrame(gen_name=String[], bus_id=Any[], bus_name=Any[],
#                   prime_mover=Any[], fuel=Any[], lat=Any[], lon=Any[],
#                   capacity_mw=Any[], bus_voltage_kv=Any[],
#                   source=String[], state=String[])
#     else
#         DataFrame(
#             gen_name       = eia_only_df[!, "gen_name"],
#             bus_id         = eia_only_df[!, "bus_id"],
#             bus_name       = eia_only_df[!, "bus_name"],
#             prime_mover    = eia_only_df[!, "prime_mover"],
#             fuel           = eia_only_df[!, "fuel"],
#             lat            = eia_only_df[!, "eia_lat"],
#             lon            = eia_only_df[!, "eia_lon"],
#             capacity_mw    = eia_only_df[!, "eia_capacity_mw"],
#             bus_voltage_kv = eia_only_df[!, "bus_voltage_kv"],
#             source         = eia_only_df[!, "source"],
#             state          = fill(label, nrow(eia_only_df)),
#         )
#     end

#     combined = sort(vcat(ei_out, eia_out, cols=:union), :bus_id)

#     println("\n", "="^60)
#     println("$label — Thermal Export Summary")
#     println("="^60)
#     println("  EI generators: $(nrow(ei_out))  |  EIA-only: $(nrow(eia_out))",
#             "  |  Total: $(nrow(combined))")
#     for src in sort(unique(skipmissing(combined.source)))
#         n  = count(==(src), skipmissing(combined.source))
#         mw = round(sum(skipmissing(combined[combined.source .== src, "capacity_mw"])),
#                    digits=1)
#         println("    $src: $n gens | $(mw) MW")
#     end
#     println("─"^60)
#     println("  By prime mover (combined):")
#     for pm in sort(unique(skipmissing(combined.prime_mover)))
#         n  = count(==(pm), skipmissing(combined.prime_mover))
#         mw = round(sum(skipmissing(
#                 combined[coalesce.(combined.prime_mover, "") .== pm, "capacity_mw"])),
#                 digits=1)
#         println("    $pm: $n gens | $(mw) MW")
#     end
#     println("="^60)
#     return combined
# end

# va_thermal_export = build_thermal_export_df(va_thermal, va_thermal_eia_only, "VA")
# md_thermal_export = build_thermal_export_df(md_thermal, md_thermal_eia_only, "MD")
# wv_thermal_export = build_thermal_export_df(wv_thermal, wv_thermal_eia_only, "WV")

# # ── T12. Write CSVs ───────────────────────────────────────────────────────────
# CSV.write(joinpath(OUTPUT_DIR, "thermal_RE_VA.csv"), va_thermal_export)
# CSV.write(joinpath(OUTPUT_DIR, "thermal_RE_MD.csv"), md_thermal_export)
# CSV.write(joinpath(OUTPUT_DIR, "thermal_RE_WV.csv"), wv_thermal_export)

# println("\n✅ Thermal CSVs written:")
# println("  → thermal_RE_VA.csv (", nrow(va_thermal_export), " rows)")
# println("  → thermal_RE_MD.csv (", nrow(md_thermal_export), " rows)")
# println("  → thermal_RE_WV.csv (", nrow(wv_thermal_export), " rows)")

# # ── T13. Thermal EI Update Report ─────────────────────────────────────────────
# all_thermal_updates = vcat(va_thermal_export, md_thermal_export, wv_thermal_export,
#                            cols=:union)

# const THERMAL_SOURCE_COLORS = Dict(
#     "EI_EIA2PF"      => "#2980b9",
#     "EI_MMWG"        => "#1a6fa8",
#     "EIA_ONLY"       => "#e74c3c",
#     "EIA_MMWG_EXACT" => "#e67e22",
#     "EIA_MMWG_FUZZY" => "#f39c12",
# )

# eia_thermal_ref = filter(row -> coalesce(row["Prime Mover Code"] in THERMAL_EIA_PM_CODES, false) &&
#                                  coalesce(row["State"] in ["VA", "MD", "WV"], false),
#                          eia_2_pf_mapping)

# eia_thermal_ref_by_state = combine(groupby(eia_thermal_ref, "State"),
#     "Nameplate Capacity (MW)" => (x -> sum(skipmissing(x))) => "eia2pf_total_mw",
#     nrow => "eia2pf_n_generators",
# )

# update_thermal_by_state = combine(groupby(all_thermal_updates, "state"),
#     "capacity_mw" => (x -> sum(skipmissing(x))) => "update_total_mw",
#     nrow => "update_n_generators",
# )
# DataFrames.rename!(update_thermal_by_state, "state" => "State")

# update_thermal_by_source = sort(
#     combine(groupby(all_thermal_updates, ["state", "source"]),
#         "capacity_mw" => (x -> sum(skipmissing(x))) => "total_mw",
#         nrow => "n_generators",
#     ), ["state", "source"])

# thermal_comparison = leftjoin(eia_thermal_ref_by_state, update_thermal_by_state, on="State")
# thermal_comparison[!, "diff_mw"] = thermal_comparison[!, "update_total_mw"] .-
#                                     thermal_comparison[!, "eia2pf_total_mw"]
# thermal_comparison[!, "pct_captured"] = round.(
#     thermal_comparison[!, "update_total_mw"] ./
#     thermal_comparison[!, "eia2pf_total_mw"] .* 100, digits=1)

# println("\n", "█"^70)
# println("  THERMAL GENERATORS — EI UPDATE REPORT")
# println("  States: VA | MD | WV")
# println("  Tech: CC | CT | ST | GT | OT (ThermalStandard)")
# println("█"^70)

# println("\n📊 CAPACITY COMPARISON — EI UPDATE vs EIA2PF REFERENCE")
# println("─"^70)
# show(thermal_comparison, allrows=true)

# println("\n\n  ⚠️  Notes on gaps:")
# for row in eachrow(thermal_comparison)
#     pct  = coalesce(row.pct_captured, 0.0)
#     diff = coalesce(row.diff_mw, 0.0)
#     if pct < 95.0
#         println("  • $(row.State): capturing $(pct)% — missing ~$(abs(round(diff, digits=1))) MW")
#     elseif pct > 105.0
#         println("  • $(row.State): capturing $(pct)% — OVER EIA2PF by $(round(diff, digits=1)) MW")
#     else
#         println("  • $(row.State): ✅ $(pct)% captured — within ±5% of EIA2PF reference")
#     end
# end

# println("\n\n📋 BREAKDOWN BY MATCH SOURCE")
# println("─"^70)
# show(update_thermal_by_source, allrows=true)

# println("\n\n🔍 DATA QUALITY FLAGS")
# println("─"^70)
# for (label, df) in [("VA", va_thermal_export),
#                     ("MD", md_thermal_export),
#                     ("WV", wv_thermal_export)]
#     no_bus  = filter(r -> ismissing(r.bus_id),      df)
#     no_cap  = filter(r -> ismissing(r.capacity_mw), df)
#     no_loc  = filter(r -> ismissing(r.lat) || ismissing(r.lon), df)
#     fuzzy   = filter(r -> coalesce(r.source, "") == "EIA_MMWG_FUZZY", df)

#     println("\n  $label ($(nrow(df)) total generators):")
#     println("    ❌ Missing bus_id:       ", nrow(no_bus),
#             nrow(no_bus)  > 0 ? "  ← cannot be added to EI model" : "  ✅")
#     println("    ⚠️  Missing capacity_mw: ", nrow(no_cap),
#             nrow(no_cap)  > 0 ? "  ← manual lookup needed"        : "  ✅")
#     println("    📍 Missing lat/lon:      ", nrow(no_loc),
#             nrow(no_loc)  > 0 ? "  ← ts assignment may fail"      : "  ✅")
#     println("    🔶 Fuzzy-matched:        ", nrow(fuzzy),
#             nrow(fuzzy)   > 0 ? "  ← verify bus manually"         : "  ✅")

#     nrow(no_bus) > 0 && (println("\n    ❌ $label generators with no bus:");
#         show(DataFrames.select(no_bus,
#             intersect(["gen_name","source","prime_mover","capacity_mw"], names(no_bus))),
#             allrows=true))
#     nrow(fuzzy)  > 0 && (println("\n    🔶 $label fuzzy-matched — verify:");
#         show(DataFrames.select(fuzzy,
#             intersect(["gen_name","bus_id","bus_name","prime_mover","capacity_mw","source"],
#                       names(fuzzy))), allrows=true))
# end

# println("\n\n📌 FINAL SUMMARY — THERMAL GENERATORS TO UPDATE IN EI")
# println("─"^70)
# total_ei_update  = 0
# total_ei_new     = 0
# for (label, df) in [("VA", va_thermal_export),
#                     ("MD", md_thermal_export),
#                     ("WV", wv_thermal_export)]
#     ei_rows  = filter(r -> startswith(coalesce(r.source, ""), "EI_"),  df)
#     new_rows = filter(r -> startswith(coalesce(r.source, ""), "EIA_"), df)
#     global total_ei_update += nrow(ei_rows)
#     global total_ei_new    += nrow(new_rows)
#     ei_mw  = round(sum(skipmissing(ei_rows.capacity_mw)),  digits=1)
#     new_mw = round(sum(skipmissing(new_rows.capacity_mw)), digits=1)
#     println("  $label:")
#     println("    Update existing EI gens:  $(nrow(ei_rows))  ($(ei_mw) MW)")
#     println("    Add new gens to EI:       $(nrow(new_rows))  ($(new_mw) MW)")
# end
# println("\n  ─"^35)
# println("  TOTAL update existing: $total_ei_update generators")
# println("  TOTAL add new:         $total_ei_new generators")
# println("█"^70)

# # ── T14. Plots ─────────────────────────────────────────────────────────────────
# tp1 = plot(
#     [bar(x    = thermal_comparison[!, "State"],
#          y    = thermal_comparison[!, "eia2pf_total_mw"],
#          name = "EIA2PF Reference (MW)",
#          marker_color = "steelblue"),
#      bar(x    = thermal_comparison[!, "State"],
#          y    = thermal_comparison[!, "update_total_mw"],
#          name = "EI Update Total (MW)",
#          marker_color = "tomato")],
#     Layout(title="Thermal: EI Update vs EIA2PF Reference (MW)",
#            barmode="group", xaxis_title="State", yaxis_title="Capacity (MW)",
#            legend=attr(orientation="h", y=-0.2))
# )

# tp2 = plot(
#     [bar(x    = filter(r -> r.source == src, update_thermal_by_source)[!, "state"],
#          y    = filter(r -> r.source == src, update_thermal_by_source)[!, "total_mw"],
#          name = src,
#          marker_color = get(THERMAL_SOURCE_COLORS, src, "gray"))
#      for src in ["EI_EIA2PF", "EI_MMWG", "EIA_ONLY", "EIA_MMWG_EXACT", "EIA_MMWG_FUZZY"]
#      if src in unique(update_thermal_by_source[!, "source"])],
#     Layout(title="Thermal EI Update — Capacity by Match Source (MW)",
#            barmode="stack", xaxis_title="State", yaxis_title="Capacity (MW)",
#            legend=attr(orientation="h", y=-0.2))
# )

# tp3 = plot(
#     bar(x    = thermal_comparison[!, "State"],
#         y    = thermal_comparison[!, "pct_captured"],
#         marker_color = [
#             pct >= 95 ? "#2ecc71" : pct >= 80 ? "#e67e22" : "#e74c3c"
#             for pct in coalesce.(thermal_comparison[!, "pct_captured"], 0.0)
#         ],
#         text         = [string(p, "%") for p in
#                         coalesce.(thermal_comparison[!, "pct_captured"], 0.0)],
#         textposition = "outside"),
#     Layout(title="Thermal EI Update — % of EIA2PF Capacity Captured",
#            xaxis_title="State", yaxis_title="% Captured",
#            yaxis=attr(range=[0, 115]),
#            shapes=[attr(type="line", x0=-0.5, x1=2.5, y0=100, y1=100,
#                         line=attr(color="black", dash="dash", width=1))])
# )

# display(tp1)
# display(tp2)
# display(tp3)

# # ── Thermal comparison by prime mover ────────────────────────────────────────

# # EIA2PF reference grouped by state + prime mover
# eia_thermal_ref_by_pm = combine(
#     groupby(
#         filter(row -> coalesce(row["Prime Mover Code"] in THERMAL_EIA_PM_CODES, false) &&
#                       coalesce(row["State"] in ["VA", "MD", "WV"], false),
#                eia_2_pf_mapping),
#         ["State", "Prime Mover Code"]
#     ),
#     "Nameplate Capacity (MW)" => (x -> sum(skipmissing(x))) => "eia2pf_mw",
#     nrow => "eia2pf_n",
# )
# DataFrames.rename!(eia_thermal_ref_by_pm, "Prime Mover Code" => "prime_mover")

# # EI update grouped by state + prime mover
# update_thermal_by_pm = combine(
#     groupby(all_thermal_updates, ["state", "prime_mover"]),
#     "capacity_mw" => (x -> sum(skipmissing(x))) => "update_mw",
#     nrow => "update_n",
# )
# DataFrames.rename!(update_thermal_by_pm, "state" => "State")

# # Join
# pm_comparison = outerjoin(eia_thermal_ref_by_pm, update_thermal_by_pm,
#                            on = ["State", "prime_mover"])
# pm_comparison[!, "eia2pf_mw"] = coalesce.(pm_comparison[!, "eia2pf_mw"], 0.0)
# pm_comparison[!, "update_mw"] = coalesce.(pm_comparison[!, "update_mw"], 0.0)
# pm_comparison[!, "pct_captured"] = round.(
#     ifelse.(pm_comparison[!, "eia2pf_mw"] .== 0.0, 0.0,
#             pm_comparison[!, "update_mw"] ./ pm_comparison[!, "eia2pf_mw"] .* 100),
#     digits=1)
# pm_comparison[!, "x_label"] = pm_comparison[!, "State"] .* " - " .*
#                                coalesce.(pm_comparison[!, "prime_mover"], "?")

# sort!(pm_comparison, ["State", "prime_mover"])

# println("\n── Thermal comparison by prime mover ────────────────────────")
# show(pm_comparison, allrows=true)

# # ── Plot 1: MW comparison EIA2PF vs EI update per state + PM ─────────────────
# tp_pm1 = plot(
#     [bar(x    = pm_comparison[!, "x_label"],
#          y    = pm_comparison[!, "eia2pf_mw"],
#          name = "EIA2PF Reference (MW)",
#          marker_color = "steelblue"),
#      bar(x    = pm_comparison[!, "x_label"],
#          y    = pm_comparison[!, "update_mw"],
#          name = "EI Update (MW)",
#          marker_color = "tomato")],
#     Layout(title     = "Thermal: EI Update vs EIA2PF by Prime Mover (MW)",
#            barmode   = "group",
#            xaxis     = attr(title="State — Prime Mover", tickangle=-35),
#            yaxis     = attr(title="Capacity (MW)"),
#            legend    = attr(orientation="h", y=-0.3))
# )

# # ── Plot 2: % captured per state + PM ────────────────────────────────────────
# tp_pm2 = plot(
#     bar(x            = pm_comparison[!, "x_label"],
#         y            = pm_comparison[!, "pct_captured"],
#         marker_color = [
#             pct >= 95 ? "#2ecc71" : pct >= 80 ? "#e67e22" : "#e74c3c"
#             for pct in pm_comparison[!, "pct_captured"]
#         ],
#         text         = [string(p, "%") for p in pm_comparison[!, "pct_captured"]],
#         textposition = "outside"),
#     Layout(title  = "Thermal EI Update — % of EIA2PF Captured by Prime Mover",
#            xaxis  = attr(title="State — Prime Mover", tickangle=-35),
#            yaxis  = attr(title="% Captured", range=[0, 130]),
#            shapes = [attr(type="line", x0=-0.5, x1=nrow(pm_comparison)-0.5,
#                           y0=100, y1=100,
#                           line=attr(color="black", dash="dash", width=1))])
# )

# # ── Plot 3: stacked MW by PM per state (EI update only) ──────────────────────
# all_pms = sort(unique(skipmissing(all_thermal_updates[!, "prime_mover"])))
# pm_colors = Dict(
#     "CC" => "#2980b9",
#     "CT" => "#e74c3c",
#     "ST" => "#2ecc71",
#     "GT" => "#e67e22",
#     "IC" => "#9b59b6",
#     "OT" => "#95a5a6",
#     "CA" => "#1abc9c",
#     "CS" => "#f39c12",
# )

# tp_pm3 = plot(
#     [bar(x    = filter(r -> coalesce(r.prime_mover, "") == pm,
#                        update_thermal_by_pm)[!, "State"],
#          y    = filter(r -> coalesce(r.prime_mover, "") == pm,
#                        update_thermal_by_pm)[!, "update_mw"],
#          name = pm,
#          marker_color = get(pm_colors, pm, "gray"))
#      for pm in all_pms
#      if pm in unique(skipmissing(update_thermal_by_pm[!, "prime_mover"]))],
#     Layout(title   = "Thermal EI Update — Capacity by Prime Mover per State (MW)",
#            barmode = "stack",
#            xaxis   = attr(title="State"),
#            yaxis   = attr(title="Capacity (MW)"),
#            legend  = attr(orientation="h", y=-0.2))
# )

# display(tp_pm1)
# display(tp_pm2)
# display(tp_pm3)