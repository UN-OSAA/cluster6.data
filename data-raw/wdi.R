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

raw_query <- WDI( # wdi_energy_raw
  indicator=ind_list_energy,
  country='all',
  start=2000,
  end=2020
)|> select(-country)

########## PROCESS RAW DATA (T)

# process WDI data
wdi <- raw_query |>
  left_join(reference, by = c("iso2c","iso3c")) |>
  mutate(
    year = structure(year, label = "year"),
    m49c = structure(m49c, label = "M49 Code"),
    iso2c = structure(iso2c, label = "iso2c Code"),
    iso3c = structure(iso3c, label = "iso3c Code")
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
saveRDS(wdi, here("data/wdi.rda"))
fwrite(meta_wdi, here("data-raw/meta_wdi.csv"))
fwrite(raw_query, here(paste0("data-raw/query_wdi_",Sys.Date(),".csv")))
