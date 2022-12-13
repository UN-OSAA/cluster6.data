library(pacman)
pacman::p_load(
  tidyverse,
  data.table,
  lubridate,
  here,
  labelled,
  janitor,
  WDI,
  rsdmx
  # readxl
)

load("data/reference.rda")

########## LOAD RAW DATA (E)

# indicators_list <- readxl::read_xlsx(here("utils/world-bank-indicators.xlsx"), sheet = "energy")

ind_list_energy <- c(
  'electricity_access'='EG.ELC.ACCS.ZS',
  'alternative_energy'='EG.USE.COMM.CL.ZS',
  'electricity_consumption'='EG.USE.ELEC.KH.PC',
  'energy_imports'='EG.IMP.CONS.ZS',
  'energy_intensity'='EG.EGY.PRIM.PP.KD',
  'energy_use'='EG.USE.PCAP.KG.OE',
  'fossil_fuel_consumption'='EG.USE.COMM.FO.ZS',
  'fuel_exports'='TX.VAL.FUEL.ZS.UN',
  'energy_unit_use_gdp'='EG.GDP.PUSE.KO.PP.KD',
  'energy_investment_private'='IE.PPI.ENGY.CD',
  'ores_metal_exports'='TX.VAL.MMTL.ZS.UN',
  'renewable_electricity_output'='EG.ELC.RNEW.ZS',
  'renewable_energy_consumption'='EG.FEC.RNEW.ZS',
  'time_electricity'='IC.ELC.TIME',
  'resources_rents'='NY.GDP.TOTL.RT.ZS',
  'electricity_access_ru'='EG.ELC.ACCS.RU.ZS',
  'electricity_access_ur'='EG.ELC.ACCS.UR.ZS'
)

raw_query_func <- function(source = "saved"){
  if(source == "saved"){
    read_csv(here("data-raw/query_wdi_2022-12-07.csv"))
  } else{
    WDI( indicator=ind_list_energy, country='all', start=2000, end=2020)|> select(-country)
  }
}

raw_query <- raw_query_func()

########## PROCESS RAW DATA (T)

# process WDI data
wdi <- raw_query |>
  left_join(reference, by = c("iso2c","iso3c")) |>
  filter(region == "Africa") |>
  select(-c(region_id, region, region_sub_id,region_int_id)) |>
  mutate(
    year = structure(year, label = "year"),
    m49c = structure(m49c, label = "M49 Code"),
    iso2c = structure(iso2c, label = "iso2c Code"),
    iso3c = structure(iso3c, label = "iso3c Code"),
    # Reference columns
    region_sub = structure(region_sub, label = "Sub-region Name"),
    region_int = structure(region_int, label = "Intermediate Region Name"),
    ldc = structure(ldc, label = "Least Developed Countries (LDC)"),
    lldc = structure(lldc, label = "Land Locked Developing Countries (LLDC)"),
    sids = structure(sids, label = "Small Island Developing States (SIDS)"),
    income = structure(income, label = "World Bank Income-level Classification"),
    lending = structure(lending, label = "World Bank Lending Group"),
    # WDI Columns
    electricity_access = structure(electricity_access, label = 'Access to electricity (% of population)'),
    alternative_energy = structure(alternative_energy, label = 'Alternative and nuclear energy (% of total energy use)'),
    electricity_consumption = structure(electricity_consumption, label = 'Electric power consumption (kWh per capita)'),
    energy_imports = structure(energy_imports, label = 'Energy imports, net (% of energy use)'),
    energy_intensity = structure(energy_intensity, label = 'Energy intensity level of primary energy (MJ/$2017 PPP GDP)'),
    energy_use = structure(energy_use, label = 'Energy use (kg of oil equivalent per capita)'),
    fossil_fuel_consumption = structure(fossil_fuel_consumption, label = 'Fossil fuel energy consumption (% of total)'),
    fuel_exports = structure(fuel_exports, label = 'Fuel exports (% of merchandise exports)'),
    energy_unit_use_gdp = structure(energy_unit_use_gdp, label = 'GDP per unit of energy use (constant 2017 PPP $ per kg of oil equivalent)'),
    energy_investment_private = structure(energy_investment_private, label = 'Investment in energy with private participation (current US$)'),
    ores_metal_exports = structure(ores_metal_exports, label = 'Ores and metals exports (% of merchandise exports)'),
    renewable_electricity_output = structure(renewable_electricity_output, label = 'Renewable electricity output (% of total electricity output)'),
    renewable_energy_consumption = structure(renewable_energy_consumption, label = 'Renewable energy consumption (% of total final energy consumption)'),
    time_electricity = structure(time_electricity, label = 'Time required to get electricity (days)'),
    resources_rents = structure(resources_rents, label = 'Total natural resources rents (% of GDP)'),
    electricity_access_ru = structure(electricity_access_ru, label = 'Access to electricity, rural (% of rural population)'),
    electricity_access_ur = structure(electricity_access_ur, label = 'Access to electricity, urban (% of urban population)')
  ) |>
  relocate(
    ldc,
    lldc,
    sids,
    income,
    lending,
    iso2c,
    iso3c,
    m49c,
    region_sub,
    region_int,
    country,
    year,
    electricity_access,
    electricity_access_ru,
    electricity_access_ur
  )

# catalog
meta_wdi <- wdi |>
  filter(country == "Algeria", year == 2000) |>
  rowid_to_column("index") |>
  mutate(across(everything(), ~as.character(.x))) |>
  pivot_longer(cols = -index, names_to = "var", values_drop_na = TRUE) |>
  transmute(var, description = var_label(wdi[var])) |>
  as.data.frame()

######### SAVE PROCESSED DATA (L)
fwrite(wdi, here("data/wdi.csv"))
save(wdi, file = "data/wdi.rda")
fwrite(meta_wdi, here("data-raw/meta_wdi.csv"))

raw_query_save <- function(source = "saved"){
  if(source == "saved"){
    fwrite(raw_query, here("data-raw/query_wdi_2022-12-07.csv"))
  }else{
    fwrite(raw_query, here(paste0("data-raw/query_wdi_",Sys.Date(),".csv")))
  }
}

raw_query_save()
