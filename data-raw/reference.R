library(pacman)
pacman::p_load(
  dplyr,
  data.table,
  readr,
  here,
  labelled,
  WDI
)

########## LOAD RAW DATA (E)

# UNSD Methodology data from: https://unstats.un.org/unsd/methodology/m49/overview/
raw_unsd <- read_csv2(here("data-raw/UNSD_Methodology.csv"))

# World Bank WDI data
raw_wdi <- WDI("NY.GDP.MKTP.CD", country='all', start=2021, end=2021, extra = TRUE) |>
  select(-c(NY.GDP.MKTP.CD, lastupdated, status, region, country, year)) |>
  filter(income != "Aggregates")

########## PROCESS RAW DATA (T)

# Construct reference dataset
reference <- raw_unsd |>
  mutate(
    across(`Least Developed Countries (LDC)`:`Small Island Developing States (SIDS)`, ~ifelse(is.na(.x), "No", "Yes")),
    across(c(`Region Name`,`Sub-region Name`), ~ifelse((is.na(.x) & `Country or Area`=="Antarctica"), "Antarctica", .x)),
    across(c(`Region Code`,`Sub-region Code`), ~ifelse((is.na(.x) & `Country or Area`=="Antarctica"), "999", .x)),
    `Intermediate Region Name` = ifelse(!is.na(`Intermediate Region Name`),`Intermediate Region Name`, `Sub-region Name`),
    `Intermediate Region Code` = ifelse(!is.na(`Intermediate Region Code`), `Intermediate Region Code`, `Sub-region Code`),
    across(c(`Region Code`:`Intermediate Region Name`,`Least Developed Countries (LDC)`:`Small Island Developing States (SIDS)`),~as_factor(.x)),
    `M49 Code` = as.numeric(`M49 Code`)
  ) |>
  rename(
    "iso2c" = `ISO-alpha2 Code`,
    "iso3c" = `ISO-alpha3 Code`
  ) |>
  left_join(raw_wdi, by = c("iso2c", "iso3c")) |>
  mutate(across(c(income,lending),~as_factor(.x))) |>
  transmute(
    region_id = structure(`Region Code`, label = "Region/Continent Code"),
    region = structure(`Region Name`, label = "Region/Continent Name"),
    region_sub_id = structure(`Sub-region Code`, label = "Sub-region Code"),
    region_sub = structure(`Sub-region Name`, label = "Sub-region Name"),
    region_int_id = structure(`Intermediate Region Code`, label = "Intermediate Region Code"),
    region_int = structure(`Intermediate Region Name`, label = "Intermediate Region Name"),
    ldc = structure(`Least Developed Countries (LDC)`, label = "Least Developed Countries (LDC)"),
    lldc = structure(`Land Locked Developing Countries (LLDC)`, label = "Land Locked Developing Countries (LLDC)"),
    sids = structure(`Small Island Developing States (SIDS)`, label = "Small Island Developing States (SIDS)"),
    country = structure(`Country or Area`, label = "Country or Area"),
    m49c = structure(`M49 Code`, label = "M49 Code"),
    iso2c = structure(iso2c, label = "ISO-alpha2 Code"),
    iso3c = structure(iso3c, label = "ISO-alpha3 Code"),
    income = structure(income, label = "World Bank Income-level Classification"),
    lending = structure(lending, label = "World Bank Lending Group")
  )

# catalog
meta_reference <- reference |>
  filter(country == "Algeria") |>
  rowid_to_column("index") |>
  mutate(across(everything(), ~as.character(.x))) |>
  pivot_longer(cols = -index, names_to = "var", values_drop_na = TRUE) |>
  transmute(var, description = var_label(reference[var]))


######### SAVE PROCESSED DATA (L)
fwrite(reference, here("data/reference.csv"))
save(reference, file = "data/reference.rda")
fwrite(meta_reference, here("data-raw/meta_reference.csv"))
