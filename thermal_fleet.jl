# ADVISORY
# I will find high discrepancies between the EI and EIA datasets for thermal generators
# The plants are aggregated different.
# Bus name indicates which plant it is.

using CSV
using DataFrames

thermal_map = CSV.read(joinpath(@__DIR__, "VA_thermal_correction_data.csv"), DataFrame)
mapped_ei_gens = filter(row -> !ismissing(row."EI gen"), thermal_map)[:, :"EI gen"]
unique_eia_plants = unique(skipmissing(thermal_map.plant))
ei_thermal_gens = filter(row -> row.Type == "ThermalStandard", CSV.read(joinpath(@__DIR__, "VA_original_EI.csv"), DataFrame))
eia_plant_ei_gen_dict = Dict{String, Vector{Any}}()
for unique_plant in unique_eia_plants
        matching_ei_gens =
            collect(skipmissing(filter(row -> !ismissing(row.plant) && row.plant == unique_plant,
            thermal_map)[:, :"EI gen"]))
        if !isempty(matching_ei_gens)
            eia_plant_ei_gen_dict[unique_plant] = matching_ei_gens
        end
end

# eia_w_no_ei = filter(row ->  !haskey(eia_plant_ei_gen_dict, p),
#                      unique_eia_plants)

eia_w_no_ei =
    thermal_map[
        .!ismissing.(thermal_map.plant) .&
        .!haskey.(Ref(eia_plant_ei_gen_dict), thermal_map.plant),
        :
    ]
CSV.write(joinpath(@__DIR__, "eia_w_no_ei.csv"), eia_w_no_ei)

eia_w_no_ei_capacity = sum(eia_w_no_ei[:, :nameplate_capacity])

mapped_set = Set(skipmissing(mapped_ei_gens))

unmapped_ei = filter(
    row -> !ismissing(row.GenName) && !(row.GenName in mapped_set),
    ei_thermal_gens
)

available_unmapped_ei = filter(row -> row.Available == true, unmapped_ei)




