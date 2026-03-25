using Pkg
Pkg.activate("EasternInterconnection.jl/")


using PowerSystems
using Logging
using DataFrames
using CSV



const PSY = PowerSystems

_load_year=2023
_weather_year=2012 #TODO Ask Jarrad if this is the year that he is using


DATA_DIR = "/Users/sabrilg/Documents/GitHub/va_updates"

sys = System("system.json")


set_units_base_system!(sys, "NATURAL_UNITS")


# Changing status of Solar generators that are on EIA but unavailable on the current system
gens_solar_EIA = CSV.read(joinpath(DATA_DIR, "solar_va_unavailable_good_matches.csv"), DataFrame)

# Add extra generator not in the CSV
extra_gens = ["generator-314722-1964101667"]

all_gen_names = vcat(
    [strip.(split(s, ";")) for s in gens_solar_EIA.GenNames]...,
    extra_gens
)

# Track generators that need time series added
needs_ts = String[]

for gen_name in all_gen_names #TODO: generator, Prime mover, lat and lon for TS extraction
    gen = get_component(RenewableDispatch, sys, gen_name)
    if isnothing(gen)
        @warn "Generator not found: $gen_name"
        continue
    end

    ts_list = get_time_series_multiple(gen)
    has_ts = !isempty(collect(ts_list))

    if has_ts
        set_available!(gen, true)
        @info "Set available=true for $gen_name"
    else
        set_available!(gen, true)
        @warn "No time series found for $gen_name — Set available but TS pendding"  #TODO: Add time series for this generator
        push!(needs_ts, gen_name)
    end
end

@info "Generators that need time series added ($(length(needs_ts))):"
for g in needs_ts
    println("  - $g")
end

# Wind

gens_va_wind_NOT_EIA = CSV.read(joinpath(DATA_DIR, "wind_va_to_unavailable.csv"), DataFrame)
gens_va_wind_NOT_EIA = filter(row -> row.GenNames != "generator-314295-1876580362", gens_va_wind_EIA) # remove the 6BIRDNECK_314295 for coincidence with the EIA 

# Set wind generators as unavailable
for gen_name in gens_va_wind_NOT_EIA.GenNames
    gen = get_component(RenewableDispatch, sys, gen_name)
    if !isnothing(gen)
        set_available!(gen, false)
        @info "Generator $gen_name set as unavailable"
    else
        @warn "Generator $gen_name not found in system"
    end
end

# Hydro #TODO: Make capacity by gens coincide. The macro capacity coincides, there are not generators set as unavailable that are cointained on EIA, but the capacities are differents
# /projects/ntps/sabrilg/EasternInterconnection.jl/data/godeeep_hydro
# Find the plant and then

# Pumped Storage: #TODO Make capacities coincide. The macro capacities coincide but slights differences on gens.

# Natural Gas Thermal #TODO Check the ones that are >25% of differnce capacity but match on name or lat/lon.
# Changing status of Thermal generators that are on EIA but unavailable on the current system
gens_ng_EIA = CSV.read(joinpath(DATA_DIR, "ng_va_unavailable_good_matches.csv"), DataFrame)

# Add extra generator not in the CSV
extra_gens = ["generator-315178-4533270255", "generator-315177-8574402224", "generator-315179-5929992923", "generator-315180-6192914777"]

ng_gen_names = vcat(
    [strip.(split(s, ";")) for s in gens_ng_EIA.GenNames]...,
    extra_gens
)

for gen_name in ng_gen_names 
    gen = get_component(ThermalStandard, sys, gen_name)
    if isnothing(gen)
        @warn "Generator not found: $gen_name"
        continue
    end
    set_available!(gen, true)
    @info "Set available=true for $gen_name"
end

gens_va_ng_NOT_EIA = CSV.read(joinpath(DATA_DIR, "ng_va_only_EI.csv"), DataFrame)
ng_gen_names = []
ng_gen_names = vcat(
    [strip.(split(s, ";")) for s in gens_va_ng_NOT_EIA.GenNames]...
)

# Set NG generators as unavailable
for gen_name in ng_gen_names
    gen = get_component(ThermalStandard, sys, gen_name)
    if !isnothing(gen)
        set_available!(gen, false)
        @info "Generator $gen_name set as unavailable"
    else
        @warn "Generator $gen_name not found in system"
    end
end

# Coal Thermal
# Set coal generators that are in EIA as available
gens_coal_EIA = CSV.read(joinpath(DATA_DIR, "coal_va_unavailable_good_matches.csv"), DataFrame)

coal_gen_names = vcat(
    [strip.(split(s, ";")) for s in gens_coal_EIA.GenNames]...
)

for gen_name in coal_gen_names 
    gen = get_component(ThermalStandard, sys, gen_name)
    if isnothing(gen)
        @warn "Generator not found: $gen_name"
        continue
    end
    set_available!(gen, true)
    @info "Set available=true for $gen_name"
end

# Set coal generators not in EIA as unavailable
gens_va_coal_NOT_EIA = CSV.read(joinpath(DATA_DIR, "coal_va_only_EI.csv"), DataFrame)

coal_gen_names = vcat(
    [strip.(split(s, ";")) for s in gens_va_coal_NOT_EIA.GenNames]...
)

for gen_name in coal_gen_names
    gen = get_component(ThermalStandard, sys, gen_name)
    if !isnothing(gen)
        set_available!(gen, false)
        @info "Generator $gen_name set as unavailable"
    else
        @warn "Generator $gen_name not found in system"
    end
end

# Oil Thermal
# Set oil generators that are in EIA as available
gens_oil_EIA = CSV.read(joinpath(DATA_DIR, "oil_va_unavailable_good_matches.csv"), DataFrame)

oil_gen_names = vcat(
    [strip.(split(s, ";")) for s in gens_oil_EIA.GenNames]...
)

for gen_name in oil_gen_names 
    gen = get_component(ThermalStandard, sys, gen_name)
    if isnothing(gen)
        @warn "Generator not found: $gen_name"
        continue
    end
    set_available!(gen, true)
    @info "Set available=true for $gen_name"
end

# Set oil generators not in EIA as unavailable
gens_va_oil_NOT_EIA = CSV.read(joinpath(DATA_DIR, "oil_va_only_EI.csv"), DataFrame)

oil_gen_names = vcat(
    [strip.(split(s, ";")) for s in gens_va_oil_NOT_EIA.GenNames]...
)

for gen_name in oil_gen_names
    gen = get_component(ThermalStandard, sys, gen_name)
    if !isnothing(gen)
        set_available!(gen, false)
        @info "Generator $gen_name set as unavailable"
    else
        @warn "Generator $gen_name not found in system"
    end
end

#MD

# Changing status of Solar generators that are on EIA but unavailable on the current system
gens_MD_solar_EIA = CSV.read(joinpath(DATA_DIR, "solar_md_unavailable_good_matches.csv"), DataFrame)


solarMD_gen_names = vcat(
    [strip.(split(s, ";")) for s in gens_MD_solar_EIA.GenNames]...  
)

# Track generators that need time series added
needs_ts = String[]

for gen_name in solarMD_gen_names #TODO: generator, Prime mover, lat and lon for TS extraction
    gen = get_component(RenewableDispatch, sys, gen_name)
    if isnothing(gen)
        @warn "Generator not found: $gen_name"
        continue
    end

    ts_list = get_time_series_multiple(gen)
    has_ts = !isempty(collect(ts_list))

    if has_ts
        set_available!(gen, true)
        @info "Set available=true for $gen_name"
    else
        set_available!(gen, true)
        @warn "No time series found for $gen_name — Set available but TS pendding"  #TODO: Add time series for this generator
        push!(needs_ts, gen_name)
    end
end

@info "Generators that need time series added ($(length(needs_ts))):"
for g in needs_ts
    println("  - $g")
end
