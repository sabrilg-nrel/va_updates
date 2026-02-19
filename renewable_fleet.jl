using CSV
using DataFrames

VA_ei_gens = CSV.read(joinpath(@__DIR__, "VA_original_EI.csv"), DataFrame)
ei_rd_gens = filter(row -> row.Type == "RenewableDispatch", VA_ei_gens)
eia_rd = CSV.read(joinpath(@__DIR__, "renewable_VA_EIA.csv"), DataFrame)
eia_capacity = sum(eia_rd[:, :nameplate_capacity])
ei_capacity = sum(ei_rd_gens[:, :Rating])
