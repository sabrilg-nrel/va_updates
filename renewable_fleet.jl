using CSV
using DataFrames
using Statistics

VA_ei_gens = CSV.read(joinpath(@__DIR__, "VA_original_EI.csv"), DataFrame)

# Renewable dispatch generators from EI and EIA
ei_rd_gens = filter(row -> row.Type == "RenewableDispatch", VA_ei_gens)
eia_rd = CSV.read(joinpath(@__DIR__, "renewable_VA_EIA.csv"), DataFrame)

# Total capacity
eia_capacity = sum(eia_rd[:, :nameplate_capacity])
ei_capacity = sum(ei_rd_gens[:, :Rating])

# Define a tolerance for matching coordinates (in degrees, ~0.01 ≈ 1km)
coord_tolerance = 0.02

# Function to find matches based on lat/lon
function find_closest_match(lat, lon, df, tolerance)
    distances = sqrt.((df.lat .- lat).^2 .+ (df.lon .- lon).^2)
    min_dist, idx = findmin(distances)
    return min_dist <= tolerance ? idx : nothing
end

# Create matching results
ei_rd_gens.eia_match_idx = [find_closest_match(row.Lat, row.Lon, eia_rd, coord_tolerance) 
                             for row in eachrow(ei_rd_gens)]

# Generators in both datasets
in_both = filter(row -> !isnothing(row.eia_match_idx), ei_rd_gens)
println("Generators in both EI and EIA: ", nrow(in_both))

# Add EIA lat/lon to the matched generators
in_both.eia_lat = [eia_rd[row.eia_match_idx, :lat] for row in eachrow(in_both)]
in_both.eia_lon = [eia_rd[row.eia_match_idx, :lon] for row in eachrow(in_both)]

# Rename EI lat/lon for clarity
rename!(in_both, :Lat => :ei_lat, :Lon => :ei_lon)

# Generators in EI but not in EIA (likely unavailable)
in_ei_only = filter(row -> isnothing(row.eia_match_idx), ei_rd_gens)
println("Generators in EI only: ", nrow(in_ei_only))

# Generators in EIA but not in EI
matched_eia_indices = filter(!isnothing, ei_rd_gens.eia_match_idx)
in_eia_only = eia_rd[setdiff(1:nrow(eia_rd), matched_eia_indices), :]
println("Generators in EIA only: ", nrow(in_eia_only))

# Compare capacities for matched generators
in_both.eia_capacity = [eia_rd[row.eia_match_idx, :nameplate_capacity] for row in eachrow(in_both)]
in_both.capacity_diff = in_both.Rating .- in_both.eia_capacity

# Summary statistics
println("\nCapacity comparison for matched generators:")
println("Total EI capacity (matched): ", sum(in_both.Rating))
println("Total EIA capacity (matched): ", sum(in_both.eia_capacity))
println("Mean capacity difference: ", mean(in_both.capacity_diff))
                            