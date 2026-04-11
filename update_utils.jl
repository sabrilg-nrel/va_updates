# ── Location-based Plant → Bus matcher ───────────────────────────────────────
using XLSX

# ── Step 1: Load all bus coordinates ONCE ────────────────────────────────────
println("Loading bus coordinates from system...")
all_buses = collect(get_components(ACBus, sys))

bus_coords = DataFrame(
    bus_number   = Int[],
    bus_name     = String[],
    lat          = Float64[],
    lon          = Float64[],
    base_voltage = Float64[],
)

for b in all_buses
    attrs = try
        get_supplemental_attributes(GeographicInfo, b)
    catch
        continue
    end
    isempty(attrs) && continue
    geo_info = first(attrs)
    coords   = geo_info.geo_json["coordinates"]
    push!(bus_coords, (
        get_number(b),
        get_name(b),
        coords[2],   # lat
        coords[1],   # lon
        get_base_voltage(b),
    ))
end
println("✅ Loaded $(nrow(bus_coords)) buses with coordinates")

# ── Step 2: Load EIA plant locations ONCE ────────────────────────────────────
eia_plants_loc = DataFrame(XLSX.readtable(
    "/Users/sabrilg/Documents/GitHub/va_updates/2___Plant_Y2024.xlsx",
    "Plant", first_row = 2
))
eia_plants_loc = DataFrames.select(eia_plants_loc, [
    "Plant Code", "Plant Name", "State", "County", "Latitude", "Longitude",
])
eia_plants_loc[!, "Plant Code"] = [ismissing(x) ? missing : Int(x) for x in eia_plants_loc[!, "Plant Code"]]
println("✅ Loaded $(nrow(eia_plants_loc)) EIA plant locations")

# ── Step 3: Core lookup function ──────────────────────────────────────────────
"""
    find_buses_for_plant(plant_id; top_n=5, kv_filter=nothing)

Given an EIA Plant Code, print the N closest buses sorted by distance.
Optionally filter by base_voltage (e.g. kv_filter=230.0).
"""
function find_buses_for_plant(plant_id::Int; top_n::Int=5, kv_filter=nothing)
    plant_row = filter(r -> coalesce(r["Plant Code"] == plant_id, false), eia_plants_loc)

    if nrow(plant_row) == 0
        println("⚠️  Plant ID $plant_id not found in EIA plant file")
        return nothing
    end

    plant_lat = plant_row[1, "Latitude"]
    plant_lon = plant_row[1, "Longitude"]
    plant_name = plant_row[1, "Plant Name"]

    if ismissing(plant_lat) || ismissing(plant_lon)
        println("⚠️  Plant ID $plant_id ('$plant_name') has no coordinates")
        return nothing
    end

    buses = copy(bus_coords)
    if !isnothing(kv_filter)
        buses = filter(r -> r.base_voltage == kv_filter, buses)
    end

    buses[!, :dist_km] = [
        haversine_km(plant_lat, plant_lon, r.lat, r.lon)
        for r in eachrow(buses)
    ]

    sort!(buses, :dist_km)
    top = first(buses, top_n)

    println("\n", "="^60)
    println("Plant: $plant_name (ID: $plant_id)")
    println("  Location: lat=$plant_lat, lon=$plant_lon")
    println("  County: $(plant_row[1, "County"]), $(plant_row[1, "State"])")
    isnothing(kv_filter) || println("  kV filter: $(kv_filter) kV")
    println("─"^60)
    println("  Top $top_n nearest buses:")
    println("─"^60)
    for (i, r) in enumerate(eachrow(top))
        println("  $i. Bus $(r.bus_number) | $(r.bus_name)")
        println("     $(r.base_voltage) kV | $(round(r.dist_km, digits=2)) km away")
        println("     (lat=$(r.lat), lon=$(r.lon))")
    end
    println("="^60)

    return top
end

# ── Haversine distance in km ──────────────────────────────────────────────────
function haversine_km(lat1, lon1, lat2, lon2)
    R = 6371.0
    φ1, φ2 = deg2rad(lat1), deg2rad(lat2)
    Δφ = deg2rad(lat2 - lat1)
    Δλ = deg2rad(lon2 - lon1)
    a = sin(Δφ/2)^2 + cos(φ1)*cos(φ2)*sin(Δλ/2)^2
    return R * 2 * atan(sqrt(a), sqrt(1-a))
end


# Find 5 closest buses to plant 68076 (any voltage)
#find_buses_for_plant(65665)

# Find 5 closest 230 kV buses only
#find_buses_for_plant(68076, kv_filter=230.0)

# Find top 10 closest buses
#find_buses_for_plant(64083 , top_n=50)
