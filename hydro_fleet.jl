using CSV
using DataFrames

VA_ei_gens = CSV.read(joinpath(@__DIR__, "VA_original_EI.csv"), DataFrame)
ei_hydro_gens = filter(row -> row.Type == "HydroDispatch" || row.Type == "HydroPumpStorage", VA_ei_gens)
eia_hydro = CSV.read(joinpath(@__DIR__, "va_hydro_EIA.csv"), DataFrame)
eia_capacity = sum(eia_hydro[:, :nameplate_capacity])
ei_capacity = sum(ei_hydro_gens[:, :Rating])

