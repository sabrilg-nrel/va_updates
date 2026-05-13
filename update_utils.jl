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
#find_buses_for_plant(63766)

# Find 5 closest 230 kV buses only
#find_buses_for_plant(68076, kv_filter=230.0)

# Find top 10 closest buses
# find_buses_for_plant(67642, top_n=50)

bus = get_bus(sys, 335335)
gens = filter(g -> get_bus(g) == bus, collect(get_components(Generator, sys)))

# Get bus coordinates
geo = get_supplemental_attributes(GeographicInfo, bus)
coords = isempty(geo) ? (missing, missing) : (geo[1].geo_json["coordinates"][2], geo[1].geo_json["coordinates"][1])


# ── 3. Load U.S. state boundaries (shapefile) ─────────────────────────────────
# Source: U.S. Census Bureau TIGER/Line shapefiles
# https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html

const STATE_SHAPES = Shapefile.Table(
    joinpath(homedir(), "Documents/GitHub/TA_repo/cb_2022_us_state_500k/cb_2022_us_state_500k.shp")
)

function get_state_shape(state_name::String)
    for row in STATE_SHAPES
        if row.NAME == state_name
            return GeoInterface.geometry(row)
        end
    end
    error("State '$state_name' not found in shapefile")
end

function build_polygon(shape)
    return ArchGDAL.fromWKT(GeoInterface.convert(ArchGDAL, shape) |> ArchGDAL.toWKT)
end

function point_in_polygon(lat, lon, poly)
    point = ArchGDAL.createpoint(lon, lat)
    return ArchGDAL.contains(poly, point)
end

# Cache polygons to avoid rebuilding them on repeated calls
const _polygon_cache = Dict{String, Any}()

function get_state_polygon(state_name::String)
    if !haskey(_polygon_cache, state_name)
        _polygon_cache[state_name] = build_polygon(get_state_shape(state_name))
    end
    return _polygon_cache[state_name]
end

# ── Get all buses within VA, MD, WV using state polygons ─────────────────────
println("Loading state polygons...")
va_poly = get_state_polygon("Virginia")
md_poly = get_state_polygon("Maryland")
wv_poly = get_state_polygon("West Virginia")

ar_poly = get_state_polygon("Arkansas")
il_poly = get_state_polygon("Illinois")

mn_poly = get_state_polygon("Minnesota")

# ── Classify each bus by state ────────────────────────────────────────────────
println("Classifying $(nrow(bus_coords)) buses by state (this may take a moment)...")

bus_coords[!, :state] = Vector{Union{String, Missing}}(missing, nrow(bus_coords))

for (i, r) in enumerate(eachrow(bus_coords))
    (ismissing(r.lat) || ismissing(r.lon)) && continue
    if point_in_polygon(r.lat, r.lon, va_poly)
        bus_coords[i, :state] = "VA"
    elseif point_in_polygon(r.lat, r.lon, md_poly)
        bus_coords[i, :state] = "MD"
    elseif point_in_polygon(r.lat, r.lon, wv_poly)
        bus_coords[i, :state] = "WV"
    elseif point_in_polygon(r.lat, r.lon, ar_poly)
        bus_coords[i, :state] = "AR"
    elseif point_in_polygon(r.lat, r.lon, il_poly)
        bus_coords[i, :state] = "IL"
    elseif point_in_polygon(r.lat, r.lon, mn_poly)
        bus_coords[i, :state] = "MN"
    end
end

# ── Filter per state ──────────────────────────────────────────────────────────
va_buses = filter(r -> coalesce(r.state, "") == "VA", bus_coords)
md_buses = filter(r -> coalesce(r.state, "") == "MD", bus_coords)
wv_buses = filter(r -> coalesce(r.state, "") == "WV", bus_coords)

ar_buses = filter(r -> coalesce(r.state, "") == "AR", bus_coords)
il_buses = filter(r -> coalesce(r.state, "") == "IL", bus_coords)
mn_buses = filter(r -> coalesce(r.state, "") == "MN", bus_coords)

# println("\n── Bus count by state ───────────────────────────────────────")
# println("  VA: $(nrow(va_buses)) buses")
# println("  MD: $(nrow(md_buses)) buses")
# println("  WV: $(nrow(wv_buses)) buses")
# println("  No state assigned: ",
#         count(ismissing, bus_coords[!, :state]), " buses")

# println("\n── VA buses ─────────────────────────────────────────────────")
# show(sort(va_buses, :bus_number), allrows=true)

# println("\n── MD buses ─────────────────────────────────────────────────")
# show(sort(md_buses, :bus_number), allrows=true)

# println("\n── WV buses ─────────────────────────────────────────────────")
# show(sort(wv_buses, :bus_number), allrows=true)

bus = get_bus(sys, 67991)
gens = filter(g -> get_bus(g) == bus, collect(get_components(Generator, sys)))

# Get bus coordinates
geo = get_supplemental_attributes(GeographicInfo, bus)
coords = isempty(geo) ? (missing, missing) : (geo[1].geo_json["coordinates"][2], geo[1].geo_json["coordinates"][1])

DataFrame(
    name         = get_name.(gens),
    type         = string.(typeof.(gens)),
    rating       = get_rating.(gens),
    bus          = get_number.(get_bus.(gens)),
    prime_mover  = get_prime_mover_type.(gens),
    bus_lat      = fill(coords[1], length(gens)),
    bus_lon      = fill(coords[2], length(gens)),
)
# VA # from summarize_solar("VA", va_ei_eia_result)
non_solar_bus_numbers = [315608, 316283, 270214, 316246, 316257, 316192, 316375, 316381, 
                          314331, 316382, 316164, 316185, 316237, 316305, 316169, 316236, 
                          316316, 316223, 316280, 313527, 316337, 316322, 316217, 270173, 
                          316314, 316131, 316369, 313506, 316132, 316159, 270197, 316197, 
                          316313, 316181, 316248, 316075, 316312, 316123, 316152, 316294, 
                          316087, 316077, 316258, 316118, 316222]
#MD

result = DataFrame()
for bn in non_solar_bus_numbers
    bus  = get_bus(sys, bn)
    gens = filter(g -> get_bus(g) == bus, collect(get_components(Generator, sys)))
    geo    = get_supplemental_attributes(GeographicInfo, bus)
    coords = isempty(geo) ? (missing, missing) : (geo[1].geo_json["coordinates"][2], geo[1].geo_json["coordinates"][1])
    
    n_gens = length(gens)
    println("Bus $bn ($(get_name(bus))): $n_gens generator(s) attached")
    
    for g in gens
        push!(result, (
            bus_number      = bn,
            bus_name        = get_name(bus),
            n_gens_at_bus   = n_gens,
            name            = get_name(g),
            type            = string(typeof(g)),
            rating          = get_rating(g),
            prime_mover     = get_prime_mover_type(g),
            bus_lat         = coords[1],
            bus_lon         = coords[2],
        ), promote=true)
    end
end
show(result, allrows=true)



#TODO remove generator-316283-6731062071 from list # EIA suggests Solar PV but 30km coordinate gap and full CC thermal parameterization indicate a likely hybrid plant bus mismatch — skipping update.

# MD #from summarize_solar("MD", md_ei_eia_result)
md_non_solar_bus_numbers = [237006, 233944, 237778, 237779, 233946, 233923, 237025, 227287]

md_result = DataFrame()
for bn in md_non_solar_bus_numbers
    bus  = get_bus(sys, bn)
    gens = filter(g -> get_bus(g) == bus, collect(get_components(Generator, sys)))
    geo    = get_supplemental_attributes(GeographicInfo, bus)
    coords = isempty(geo) ? (missing, missing) : (geo[1].geo_json["coordinates"][2], geo[1].geo_json["coordinates"][1])
    
    n_gens = length(gens)
    println("Bus $bn ($(get_name(bus))): $n_gens generator(s) attached")
    
    for g in gens
        push!(md_result, (
            bus_number      = bn,
            bus_name        = get_name(bus),
            n_gens_at_bus   = n_gens,
            name            = get_name(g),
            type            = string(typeof(g)),
            rating          = get_rating(g),
            prime_mover     = get_prime_mover_type(g),
            bus_lat         = coords[1],
            bus_lon         = coords[2],
        ), promote=true)
    end
end
show(md_result, allrows=true)

# WV — from summarize_wind("WV", wv_ei_wind_result)
wv_non_wind_bus_numbers = [235054]

wv_wind_result = DataFrame()
for bn in wv_non_wind_bus_numbers
    bus  = get_bus(sys, bn)
    gens = filter(g -> get_bus(g) == bus, collect(get_components(Generator, sys)))
    geo    = get_supplemental_attributes(GeographicInfo, bus)
    coords = isempty(geo) ? (missing, missing) : (geo[1].geo_json["coordinates"][2], geo[1].geo_json["coordinates"][1])
    
    n_gens = length(gens)
    println("Bus $bn ($(get_name(bus))): $n_gens generator(s) attached")
    
    for g in gens
        push!(wv_wind_result, (
            bus_number      = bn,
            bus_name        = get_name(bus),
            n_gens_at_bus   = n_gens,
            name            = get_name(g),
            type            = string(typeof(g)),
            rating          = get_rating(g),
            prime_mover     = get_prime_mover_type(g),
            bus_lat         = coords[1],
            bus_lon         = coords[2],
        ), promote=true)
    end
end
show(wv_wind_result, allrows=true)

# ── Minnesota generators ───────────────────────────────────────────────────────
# ── Build the dict lookup first ───────────────────────────────────────────────
# bus_coords_dict = Dict{String, Tuple{Float64, Float64}}(
#     row.bus_name => (row.lat, row.lon)
#     for row in eachrow(bus_coords)
#     if !ismissing(row.lat) && !ismissing(row.lon)
# )
# all_generators = collect(get_components(Generator, sys))
# gen_data = DataFrame(
#     name     = [get_name(g) for g in all_generators],
#     bus_name = [get_name(g.bus) for g in all_generators],
#     lat      = [get(bus_coords_dict, get_name(g.bus), (missing, missing))[1] for g in all_generators],
#     lon      = [get(bus_coords_dict, get_name(g.bus), (missing, missing))[2] for g in all_generators],
# )


# mn_gen_data = filter(row ->
#     !ismissing(row.lat) && !ismissing(row.lon) &&
#     point_in_polygon(row.lat, row.lon, mn_poly),
#     gen_data
# )

# mn_generators = [get_component(Generator, sys, name) for name in mn_gen_data.name]
# filter!(!isnothing, mn_generators)
# println("MN generators: $(length(mn_generators))")

# # ── Build output DataFrame (same schema as your WECC export) ──────────────────
# mn_resources = DataFrame(
#     "generator_type"          => [get_gen_type(g)              for g in mn_generators],
#     "name"                    => [get_name(g)                   for g in mn_generators],
#     "available"               => [get_available(g)              for g in mn_generators],
#     "bus"                     => [get_name(g.bus)               for g in mn_generators],
#     "bus_number"              => [get_number(g.bus)             for g in mn_generators],
#     "rating"                  => [get_rating(g)                 for g in mn_generators],
#     "active_power_limits_min" => [get_min_power(g)              for g in mn_generators],
#     "active_power_limits_max" => [get_max_active_power(g)       for g in mn_generators],
#     "prime_mover_type"        => [get_prime_mover_str(g)        for g in mn_generators],
#     "fuel"                    => [get_fuel(g)                   for g in mn_generators],
#     "ts_column_name"          => [get_ts_name(g)                for g in mn_generators],
#     "lat"                     => [get(bus_coords_dict, get_name(g.bus), (missing, missing))[1] for g in mn_generators],
#     "lon"                     => [get(bus_coords_dict, get_name(g.bus), (missing, missing))[2] for g in mn_generators],
#     "plant_name"              => [get(g.ext, "plant_name", missing) for g in mn_generators],
# )

# # ── Write CSVs ────────────────────────────────────────────────────────────────
# CSV.write("minnesota_resources.csv", mn_resources; transform=(col,val)->something(val,missing))
# CSV.write("minnesota_buses.csv",     mn_buses)
