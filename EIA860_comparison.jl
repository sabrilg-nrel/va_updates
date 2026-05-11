# ── Step 1: Load EIA 860 Generator file — all 3 sheets ───────────────────────
using XLSX

EIA860_PATH = "/Users/sabrilg/Documents/GitHub/va_updates/3_1_Generator_Y2024.xlsx"

EIA860_COLS = ["Utility ID", "Plant Code", "Plant Name", "State","County", 
               "Generator ID", "Prime Mover",
               "Nameplate Capacity (MW)", "Operating Year",
               "Planned Retirement Month", "Planned Retirement Year"]

function load_eia860_sheet(path::String, sheet::String, status::String)
    df = DataFrame(XLSX.readtable(path, sheet, first_row=2))
    df = DataFrames.select(df, intersect(EIA860_COLS, names(df)))
    df[!, "eia860_status"] .= status
    df[!, "Plant Code"] = [ismissing(x) ? missing : Int(x) for x in df[!, "Plant Code"]]
    return df
end

eia860_operable  = load_eia860_sheet(EIA860_PATH, "Operable",              "Operable")
eia860_proposed  = load_eia860_sheet(EIA860_PATH, "Proposed",              "Proposed")
eia860_retired   = load_eia860_sheet(EIA860_PATH, "Retired and Canceled",  "Retired/Canceled")

eia860_all = vcat(eia860_operable, eia860_proposed, eia860_retired, cols=:union)

println("✅ EIA 860 loaded:")
println("   Operable:          ", nrow(eia860_operable))
println("   Proposed:          ", nrow(eia860_proposed))
println("   Retired/Canceled:  ", nrow(eia860_retired))
println("   Total:             ", nrow(eia860_all))


# ── Match eia_expanded → eia860_all ──────────────────────────────────────────
# Step 1: join on hard keys Plant ID + Generator ID
eia_with_status = leftjoin(
    eia_expanded,
    DataFrames.select(eia860_all, 
        "Plant Code", "Plant Name", "State", "County",
        "Generator ID", "Prime Mover",
        "Nameplate Capacity (MW)", "Operating Year",
        "Planned Retirement Month", "Planned Retirement Year",
        "eia860_status"),
    on           = ["Plant ID" => "Plant Code", "Generator ID" => "Generator ID"],
    makeunique   = true,
    matchmissing = :notequal
)

# Step 2: validate soft criteria — flag suspicious matches
eia_with_status[!, "name_match"]     = [coalesce(r["Plant Name"] == r["Plant Name_1"], false)    for r in eachrow(eia_with_status)]
eia_with_status[!, "state_match"]    = [coalesce(r["State"] == r["State_1"], false)              for r in eachrow(eia_with_status)]
eia_with_status[!, "county_match"]   = [coalesce(r["County"] == r["County_1"], false)            for r in eachrow(eia_with_status)]
eia_with_status[!, "cap_diff_mw"]    = [
    let ei = coalesce(r["eia_capacity_mw"], NaN),
        e8 = coalesce(r["Nameplate Capacity (MW)"], NaN)
        (isnan(ei) || isnan(e8)) ? missing : abs(Float64(ei) - Float64(e8))
    end
    for r in eachrow(eia_with_status)
]
eia_with_status[!, "cap_match"]      = [coalesce(r["cap_diff_mw"] < 1.0, false) for r in eachrow(eia_with_status)]
eia_with_status[!, "match_score"]    = eia_with_status.name_match .+ 
                                       eia_with_status.state_match .+ 
                                       eia_with_status.county_match .+ 
                                       eia_with_status.cap_match

# Step 3: report
println("\n", "="^70)
println("  eia_expanded → EIA 860 Match Summary")
println("="^70)
matched   = filter(r -> !ismissing(r["eia860_status"]), eia_with_status)
unmatched = filter(r ->  ismissing(r["eia860_status"]), eia_with_status)
println("  ✅ Matched:   $(nrow(matched))")
println("  ⚠️  Unmatched: $(nrow(unmatched))")
println("\n  Status breakdown:")
for s in ["Operable", "Proposed", "Retired/Canceled"]
    rows = filter(r -> coalesce(r["eia860_status"], "") == s, matched)
    println("    $s: $(nrow(rows))")
end

# Step 4: flag low-confidence matches (matched but soft criteria disagree)
suspicious = filter(r -> !ismissing(r["eia860_status"]) && r["match_score"] < 2, eia_with_status)
if nrow(suspicious) > 0
    println("\n⚠️  Low-confidence matches (score < 2):")
    show(DataFrames.select(suspicious,
        intersect(["Plant ID", "Plant Name", "Plant Name_1", "State", "State_1",
                   "County", "County_1", "Generator ID", 
                   "eia_capacity_mw", "Nameplate Capacity (MW)", "cap_diff_mw",
                   "match_score", "eia860_status"], names(suspicious))),
        allrows=true)
end

# ── Step: Add eia860_status back to eia_expanded ─────────────────────────────
# Build lookup: (Plant ID, Generator ID) → eia860_status
status_lookup = Dict{Tuple{Any,Any}, String}()
for r in eachrow(eia_with_status)
    ismissing(r["eia860_status"]) && continue
    key = (r["Plant ID"], r["Generator ID"])
    status_lookup[key] = r["eia860_status"]
end

eia_expanded[!, "eia860_status"] = [
    let key = (r["Plant ID"], r["Generator ID"])
        get(status_lookup, key, "NA")
    end
    for r in eachrow(eia_expanded)
]

# ── Quick report ──────────────────────────────────────────────────────────────
println("\n✅ eia860_status added to eia_expanded:")
for s in ["Operable", "Proposed", "Retired/Canceled", "NA"]
    n = count(==(s), eia_expanded[!, "eia860_status"])
    println("   $s: $n")
end








# # ── Step 2 (revised): Recover Plant ID via bus_id → eia_slim BusID ───────────
# # gen_name format: "generator-{bus_id}-{hash}"
# function extract_bus_id_from_gen_name(gen_name::AbstractString)
#     parts = split(gen_name, "-")
#     length(parts) >= 2 || return missing
#     x = tryparse(Int, parts[2])
#     return isnothing(x) ? missing : x
# end

# # ── Build lookup: strip brackets from "[384357]" format ──────────────────────
# busid_to_plant = Dict{Int, Any}()
# for r in eachrow(eia_slim)
#     ismissing(r["BusID"])    && continue
#     ismissing(r["Plant ID"]) && continue
#     cleaned = replace(string(r["BusID"]), "[" => "", "]" => "", " " => "")
#     parsed  = tryparse(Int, cleaned)
#     isnothing(parsed) && continue
#     busid_to_plant[parsed] = r["Plant ID"]
# end
# println("✅ BusID → Plant ID lookup: $(length(busid_to_plant)) entries")

# # ── Helper: recover Plant ID via bus ID when missing ─────────────────────────
# function recover_plant_id_via_busid(export_df::DataFrame, eia2pf::DataFrame)
#     result = copy(export_df)
#     for i in 1:nrow(result)
#         !ismissing(result[i, "Plant Code"]) && continue
#         bus_id = result[i, "bus_id"]
#         ismissing(bus_id) && continue
#         match = filter(r -> coalesce(r["BusID_int"] == bus_id, false), eia2pf)
#         if nrow(match) > 0
#             result[i, "Plant Code"] = match[1, "Plant ID"]
#             result[i, "Plant Name"] = match[1, "Plant Name"]
#         end
#     end
#     return result
# end

# ## ── Helper: extract bus_id from gen_name "generator-{bus_id}-{hash}" ─────────
# function extract_bus_id_from_gen_name(gen_name::AbstractString)
#     parts = split(gen_name, "-")
#     length(parts) >= 2 || return missing
#     x = tryparse(Int, parts[2])
#     return isnothing(x) ? missing : x
# end

# # ── Build lookup: BusID → Plant ID from eia_slim ─────────────────────────────
# busid_to_plant = Dict{Int, Any}()
# for r in eachrow(eia_slim)
#     ismissing(r["BusID"])    && continue
#     ismissing(r["Plant ID"]) && continue
#     # Handle both "[232404]" and "232404" formats
#     cleaned = replace(string(r["BusID"]), "[" => "", "]" => "", " " => "")
#     # Handle multi-bus "[232404, 232406]" — take first only
#     first_id = split(cleaned, ",")[1]
#     parsed = tryparse(Int, strip(first_id))
#     isnothing(parsed) && continue
#     busid_to_plant[parsed] = r["Plant ID"]
# end
# println("✅ BusID → Plant ID lookup: $(length(busid_to_plant)) entries")

# # ── Helper: recover Plant Code via busid_to_plant lookup ─────────────────────
# function recover_plant_id_via_busid(export_df::DataFrame)
#     result = copy(export_df)
#     if !("Plant Code" in names(result))
#         result[!, "Plant Code"] = Vector{Union{Int, Missing}}(missing, nrow(result))
#     end
#     recovered = 0
#     for i in 1:nrow(result)
#         !ismissing(result[i, "Plant Code"]) && continue
#         bus_id = result[i, "bus_id"]
#         ismissing(bus_id) && continue
#         plant_id = get(busid_to_plant, bus_id, missing)
#         if !ismissing(plant_id)
#             result[i, "Plant Code"] = Int(plant_id)
#             recovered += 1
#         end
#     end
#     println("  Plant Code recovered via bus_id: $recovered / $(nrow(result)) gens")
#     return result
# end

# # ── Summary: cast capacity_mw to Float64 before summing ──────────────────────
# function safe_sum_mw(df::DataFrame)
#     vals = Float64[]
#     for x in df[!, "capacity_mw"]
#         ismissing(x) && continue
#         isnothing(x) && continue
#         p = tryparse(Float64, string(x))
#         isnothing(p) && continue
#         push!(vals, p)
#     end
#     return round(sum(vals), digits=1)
# end

# # ── Classify export generators against EIA 860 ───────────────────────────────
# function classify_export_vs_eia860(label::String, export_df::DataFrame, eia860::DataFrame)
#     println("\n", "="^70)
#     println("  $label — EIA 860 Status Classification")
#     println("="^70)

#     df = recover_plant_id_via_busid(export_df)

#     has_plant = filter(r -> !ismissing(r["Plant Code"]), df)
#     no_plant  = filter(r ->  ismissing(r["Plant Code"]), df)
#     println("  Has Plant Code: $(nrow(has_plant)) | Missing: $(nrow(no_plant))")

#     joined = leftjoin(has_plant, eia860,
#                       on           = "Plant Code",
#                       makeunique   = true,
#                       matchmissing = :notequal)

#     if nrow(no_plant) > 0
#         no_plant[!, "eia860_status"] .= missing
#         joined = vcat(joined, no_plant, cols=:union)
#     end

#     println("\n  Status breakdown:")
#     for s in ["Operable", "Proposed", "Retired/Canceled"]
#         rows = filter(r -> coalesce(r["eia860_status"], "") == s, joined)
#         println("    $s: $(nrow(rows)) gens | $(safe_sum_mw(rows)) MW")
#     end
#     not_found = filter(r -> ismissing(r["eia860_status"]), joined)
#     println("    Not in EIA 860: $(nrow(not_found)) gens | $(safe_sum_mw(not_found)) MW")

#     DISPLAY_COLS = ["gen_name", "Plant Code", "Plant Name",
#                     "Generator ID", "Prime Mover",
#                     "bus_id", "bus_name", "source",
#                     "capacity_mw", "Nameplate Capacity (MW)",
#                     "Operating Year", "Planned Retirement Year",
#                     "eia860_status"]

#     println("\n  Full classification ($(nrow(joined)) rows):")
#     show(DataFrames.select(joined,
#          intersect(DISPLAY_COLS, names(joined))), allrows=true)

#     return joined
# end
# # ── Run ───────────────────────────────────────────────────────────────────────
# va_solar_classified = classify_export_vs_eia860("VA Solar", va_export,    eia860_all)
# md_solar_classified = classify_export_vs_eia860("MD Solar", md_export,    eia860_all)
# wv_solar_classified = classify_export_vs_eia860("WV Solar", wv_export,    eia860_all)
# va_wind_classified  = classify_export_vs_eia860("VA Wind",  va_wind_export, eia860_all)
# md_wind_classified  = classify_export_vs_eia860("MD Wind",  md_wind_export, eia860_all)
# wv_wind_classified  = classify_export_vs_eia860("WV Wind",  wv_wind_export, eia860_all)

# # ── EIA 860 Classification Report ────────────────────────────────────────────
# function print_eia860_report(classified_dfs::Vector{Pair{String, DataFrame}})

#     DETAIL_COLS = ["gen_name", "Plant Code", "Plant Name",
#                    "Generator ID", "Prime Mover",
#                    "bus_id", "bus_name", "source",
#                    "capacity_mw", "Operating Year",
#                    "Planned Retirement Year", "eia860_status"]

#     println("\n", "█"^70)
#     println("  EIA 860 CLASSIFICATION REPORT")
#     println("█"^70)

#     # ── 1. Summary table across all datasets ─────────────────────────────────
#     println("\n── SUMMARY ──────────────────────────────────────────────────────────")
#     println(rpad("Dataset", 18),
#             rpad("Operable", 20),
#             rpad("Proposed", 20),
#             rpad("Retired/Canceled", 22),
#             rpad("Not in EIA 860", 20))
#     println("─"^90)

#     for (label, df) in classified_dfs
#         op  = filter(r -> coalesce(r["eia860_status"], "") == "Operable",         df)
#         pr  = filter(r -> coalesce(r["eia860_status"], "") == "Proposed",         df)
#         ret = filter(r -> coalesce(r["eia860_status"], "") == "Retired/Canceled", df)
#         unk = filter(r -> ismissing(r["eia860_status"]),                          df)
#         println(
#             rpad(label, 18),
#             rpad("$(nrow(op)) | $(safe_sum_mw(op)) MW",   20),
#             rpad("$(nrow(pr)) | $(safe_sum_mw(pr)) MW",   20),
#             rpad("$(nrow(ret)) | $(safe_sum_mw(ret)) MW", 22),
#             rpad("$(nrow(unk)) | $(safe_sum_mw(unk)) MW", 20),
#         )
#     end
#     println("─"^90)

#     # ── 2. Proposed — need to be added but not yet operable ──────────────────
#     println("\n", "="^70)
#     println("  ⚠️  PROPOSED (not yet operable)")
#     println("="^70)
#     for (label, df) in classified_dfs
#         proposed = filter(r -> coalesce(r["eia860_status"], "") == "Proposed", df)
#         nrow(proposed) == 0 && continue
#         println("\n── $label ($(nrow(proposed)) rows | $(safe_sum_mw(proposed)) MW) ──")
#         show(DataFrames.select(proposed,
#              intersect(DETAIL_COLS, names(proposed))), allrows=true)
#     end

#     # ── 3. Retired/Canceled — should not be in the model ─────────────────────
#     println("\n", "="^70)
#     println("  ❌  RETIRED / CANCELED (should be removed from model?)")
#     println("="^70)
#     for (label, df) in classified_dfs
#         retired = filter(r -> coalesce(r["eia860_status"], "") == "Retired/Canceled", df)
#         nrow(retired) == 0 && continue
#         println("\n── $label ($(nrow(retired)) rows | $(safe_sum_mw(retired)) MW) ──")
#         show(DataFrames.select(retired,
#              intersect(DETAIL_COLS, names(retired))), allrows=true)
#     end

#     # ── 4. Not in EIA 860 at all — unclassified ───────────────────────────────
#     println("\n", "="^70)
#     println("  ❓  NOT FOUND IN EIA 860 (unclassified)")
#     println("="^70)
#     for (label, df) in classified_dfs
#         unknown = filter(r -> ismissing(r["eia860_status"]), df)
#         nrow(unknown) == 0 && continue
#         println("\n── $label ($(nrow(unknown)) rows | $(safe_sum_mw(unknown)) MW) ──")
#         show(DataFrames.select(unknown,
#              intersect(DETAIL_COLS, names(unknown))), allrows=true)
#     end

#     println("\n", "█"^70)
# end

# # ── Run report ────────────────────────────────────────────────────────────────
# print_eia860_report([
#     "VA Solar" => va_solar_classified])
# print_eia860_report([
# "MD Solar" => md_solar_classified,])
# print_eia860_report([
#     "WV Solar" => wv_solar_classified,])
# print_eia860_report([    
#     "VA Wind"  => va_wind_classified,])
# print_eia860_report([
#     "MD Wind"  => md_wind_classified,])
# print_eia860_report([    
#     "WV Wind"  => wv_wind_classified,])